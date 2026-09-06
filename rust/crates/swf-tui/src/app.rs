//! Owning the terminal, and giving it back.
//!
//! The boundary this module defends is non-negotiable 8: raw mode and the alternate screen are
//! released on a normal exit, on an `Err`, on a panic and on `SIGINT`/`SIGTERM`. A tool that
//! leaves a shell echoing nothing because it died halfway through a frame has done more damage
//! than the bug it died of, so restoration is a `Drop` guard plus a panic hook that restores
//! *before* it re-raises — the backtrace is worth nothing if it prints into a terminal that can no
//! longer render it.
//!
//! And non-negotiable 9: closing this window stops nothing. Airflow is still scheduling, the
//! agents are still working, the gates are still waiting. Quitting cancels the in-flight *reads*
//! this process was making and nothing else.

use std::io::{self, Stdout, Write};
use std::sync::{Arc, Once};
use std::time::Duration;

use anyhow::Context as _;
use chrono::Utc;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::tty::IsTty;
use crossterm::{execute, ExecutableCommand};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use swf_app::ops::Ops;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::effect::{Runtime, CHANNEL_CAPACITY};
use crate::event::spawn_reader;
use crate::model::{update, Env, Model, Msg, DEFAULT_REFRESH};
use crate::theme::Theme;
use crate::view;

/// How often the clock advances on screen. Ages and staleness are worth a second's precision; the
/// collection interval is a separate, much longer thing.
pub const TICK: Duration = Duration::from_secs(1);

static PANIC_HOOK: Once = Once::new();

/// How the session should behave, for a caller that has flags of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TuiOptions {
    /// How often to re-read every source.
    pub refresh: Duration,
    /// False under `--no-color` or `NO_COLOR`.
    pub color: bool,
}

impl Default for TuiOptions {
    fn default() -> Self {
        Self {
            refresh: DEFAULT_REFRESH,
            color: Theme::env_allows_color(),
        }
    }
}

/// Run the interactive factory against one environment.
///
/// Takes the `Ops` behind an `Arc` because every effect is a task that outlives the call that
/// spawned it, and because the caller usually already has one — a CLI that has just run a command
/// against the same environment should not have to rebuild it to open a window on it.
pub async fn run(ops: Arc<Ops>) -> anyhow::Result<()> {
    run_with(ops, TuiOptions::default()).await
}

/// The same, with the caller's own refresh interval and colour decision.
pub async fn run_with(ops: Arc<Ops>, opts: TuiOptions) -> anyhow::Result<()> {
    if !io::stdout().is_tty() {
        anyhow::bail!(
            "swf tui needs a terminal; use `swf attention` or `swf jobs list --json` when piping"
        );
    }
    install_panic_hook();

    let mut terminal = enter().context("could not take over the terminal")?;
    // From here to the end of the function the terminal belongs to this process, and `_guard`
    // gives it back however the function leaves — return, `?`, or unwinding panic.
    let _guard = Guard;

    let size = terminal.size().unwrap_or_default();
    let theme = Theme::new(opts.color);
    let mut model = Model::new(Env::from_context(ops.context()), opts.refresh, Utc::now());
    model.size = (size.width, size.height);
    model.note(format!(
        "watching {} at {}",
        model.env.context, model.env.airflow_url
    ));

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let runtime = Runtime::new(ops, tx.clone());
    let reader = spawn_reader(tx.clone());
    spawn_clock(tx.clone());
    spawn_signals(tx.clone());
    drop(tx);

    let outcome = drive(&mut terminal, &mut model, theme, &runtime, rx).await;

    // Cancel the reads this process had outstanding. Nothing on the far side is affected: the
    // gates still wait, the runs still run, and the next `swf` sees exactly what this one saw.
    runtime.shutdown();
    drop(runtime);
    let _ = reader.join();
    outcome
}

/// The update loop: draw, take a message, apply it, dispatch whatever it asked for.
///
/// Everything already queued is applied before the next draw. A burst — a held arrow key, a
/// snapshot landing on the same frame as a resize — becomes one repaint instead of five, which is
/// the difference between a cursor that keeps up and one that lags behind the operator's hands.
async fn drive(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    model: &mut Model,
    theme: Theme,
    runtime: &Runtime,
    mut rx: Receiver<Msg>,
) -> anyhow::Result<()> {
    loop {
        terminal.draw(|frame| view::render(frame, model, theme))?;
        let Some(msg) = rx.recv().await else {
            return Ok(());
        };
        for effect in update(model, msg) {
            runtime.dispatch(effect);
        }
        while let Ok(msg) = rx.try_recv() {
            for effect in update(model, msg) {
                runtime.dispatch(effect);
            }
        }
        if model.quit {
            return Ok(());
        }
    }
}

/// Advance the clock once a second, and stop as soon as nobody is listening.
fn spawn_clock(tx: Sender<Msg>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if tx.send(Msg::Tick(Utc::now())).await.is_err() {
                return;
            }
        }
    });
}

/// Turn a signal into the same message `q` produces, so there is one way out and one restoration.
fn spawn_signals(tx: Sender<Msg>) {
    tokio::spawn(async move {
        wait_for_signal().await;
        let _ = tx.send(Msg::Interrupt).await;
    });
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        // Without a SIGTERM handler the default disposition kills the process and the `Drop` guard
        // never runs; Ctrl-C is still handled as a keystroke, so this degrades rather than fails.
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Take over the terminal.
fn enter() -> io::Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    terminal.hide_cursor()?;
    terminal.clear()?;
    Ok(terminal)
}

/// Give the terminal back. Safe to call twice, and safe to call when it was never taken.
pub fn restore() -> io::Result<()> {
    let mut stdout = io::stdout();
    let leave = stdout.execute(LeaveAlternateScreen).map(|_| ());
    let show = stdout.execute(crossterm::cursor::Show).map(|_| ());
    let raw = disable_raw_mode();
    let _ = stdout.flush();
    leave.and(show).and(raw)
}

/// Restores the terminal however this function is left.
struct Guard;

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = restore();
    }
}

/// Restore the terminal *before* the panic message is printed, then let the normal hook run.
///
/// Order is the whole point: a backtrace written into a raw-mode alternate screen is a backtrace
/// nobody reads, and the shell it is written over is left unusable. Installed once — a second
/// session in the same process must not stack a second hook.
pub fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = restore();
            previous(info);
        }));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restoring_a_terminal_that_was_never_taken_is_not_an_error_worth_propagating() {
        // Under `cargo test` stdout is a pipe, so every escape sequence goes nowhere. What matters
        // is that the call is total: the `Drop` guard cannot afford to care whether it worked.
        let _ = restore();
        let _ = restore();
    }

    #[test]
    fn the_panic_hook_is_installed_once_however_many_sessions_run() {
        install_panic_hook();
        install_panic_hook();
        assert!(PANIC_HOOK.is_completed());
    }

    #[test]
    fn colour_follows_the_no_color_convention() {
        let opts = TuiOptions::default();
        assert_eq!(opts.color, Theme::env_allows_color());
        assert_eq!(opts.refresh, DEFAULT_REFRESH);
    }
}
