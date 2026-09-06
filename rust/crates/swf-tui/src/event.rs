//! Keystrokes in, messages out — and the key map itself, written down once.
//!
//! The boundary this module defends is muscle memory. An operator who knows `swfactory herd`
//! should not have to relearn anything: `r`/`F5` refresh, `q` quits, `?` helps, `t` triggers, `s`
//! stops, `o` opens, `a` approves. The one deliberate departure is `herd`'s Gates tab, where `r`
//! meant *reject* and shadowed the global refresh — an operator who pressed `r` to refresh was
//! asked to reject a gate (`02-herd-tui.md` §10.16). Here `r` is always refresh and the
//! destructive answer is `x`, which is what `00-architecture.md` §7 binds.
//!
//! The map is a pure function of mode, view and key so that "what does this key do here" is a test
//! and not a walk through the widget tree. Reading the terminal is the only thing in this module
//! that touches the outside world, and it does nothing but forward.

use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use tokio::sync::mpsc::Sender;

use crate::model::{Mode, Msg, View};

/// How long the reader waits for a key before looking at whether it should stop.
pub const POLL: Duration = Duration::from_millis(100);

/// One thing a key can ask for. The command palette runs exactly these, by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Leave. Remote work keeps running (non-negotiable 9).
    Quit,
    /// Re-read every source now.
    Refresh,
    /// Show the key map.
    Help,
    /// Show or hide the activity pane.
    ToggleLog,
    /// Open the command palette.
    Palette,
    /// Open the table filter.
    Search,
    /// Go to a named view.
    GoView(View),
    /// Go to the next view.
    NextView,
    /// Go to the previous view.
    PrevView,
    /// Move the cursor up one row.
    Up,
    /// Move the cursor down one row.
    Down,
    /// Move the cursor up a screenful.
    PageUp,
    /// Move the cursor down a screenful.
    PageDown,
    /// First row.
    Top,
    /// Last row.
    Bottom,
    /// `enter`: open, submit, confirm — whatever the mode's positive answer is.
    Accept,
    /// `esc`: back out of whatever is open.
    Cancel,
    /// Answer the selected gate yes.
    Approve,
    /// Answer the selected gate no.
    Reject,
    /// Submit issues to a blueprint.
    Trigger,
    /// Mark the selected Airflow run failed.
    Stop,
    /// Open the selected run or delivery in a browser.
    Open,
    /// Verify the selected delivery.
    Verify,
    /// Remove the selected sandbox.
    Remove,
    /// Fetch the selected job's current task log.
    Logs,
    /// A character typed into a text field.
    Char(char),
    /// Rub out the last character of a text field.
    Backspace,
}

/// One row of the help overlay: the key, and what it does here.
pub const HELP: &[(&str, &str)] = &[
    ("r / F5", "refresh every source now"),
    ("Tab / S-Tab", "next / previous view"),
    ("1 - 7", "go straight to a view"),
    ("j k / arrows", "move the cursor"),
    ("g / G", "first / last row"),
    ("enter", "open the selected job or gate"),
    ("esc", "back, or clear the filter"),
    ("a", "approve the selected gate"),
    (
        "x",
        "reject the gate — remove the sandbox on infrastructure",
    ),
    ("t", "trigger a blueprint with issue refs"),
    ("s", "mark the selected Airflow run failed"),
    ("o", "open the run or delivery in a browser"),
    ("v", "verify the selected delivery"),
    ("L", "fetch the selected task's log"),
    ("l", "show or hide the activity pane"),
    ("/", "filter the table"),
    (":", "command palette"),
    ("?", "this"),
    ("q / ctrl-c", "quit — remote work keeps running"),
];

/// True for the key that means "stop, now", whatever mode is in force.
///
/// Raw mode delivers Ctrl-C as a keystroke rather than a signal, so the interrupt has to be
/// recognised here as well as in the signal handler, or the one escape hatch every terminal user
/// relies on would do nothing (non-negotiable 8).
pub fn is_interrupt(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c' | 'C'))
}

/// Turn one terminal event into a message, or discard it.
pub fn to_msg(event: Event) -> Option<Msg> {
    match event {
        // Windows reports press *and* release; acting on both doubles every keystroke.
        Event::Key(key) if key.kind != KeyEventKind::Press => None,
        Event::Key(key) if is_interrupt(&key) => Some(Msg::Interrupt),
        Event::Key(key) => Some(Msg::Key(key)),
        Event::Resize(cols, rows) => Some(Msg::Resize(cols, rows)),
        _ => None,
    }
}

/// What a key does, given where the operator is.
pub fn map_key(mode: Mode, view: View, key: &KeyEvent) -> Option<Action> {
    match mode {
        Mode::Normal => normal(view, key),
        Mode::Confirm => confirm(key),
        Mode::Help => Some(Action::Cancel),
        Mode::Search | Mode::Palette | Mode::Prompt => text(mode, key),
    }
}

fn confirm(key: &KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('y' | 'Y') | KeyCode::Enter => Some(Action::Accept),
        KeyCode::Char('n' | 'N') | KeyCode::Esc => Some(Action::Cancel),
        _ => None,
    }
}

fn text(mode: Mode, key: &KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Enter => Some(Action::Accept),
        KeyCode::Esc => Some(Action::Cancel),
        KeyCode::Backspace => Some(Action::Backspace),
        KeyCode::Up if mode == Mode::Palette => Some(Action::Up),
        KeyCode::Down if mode == Mode::Palette => Some(Action::Down),
        // A typed character is text, never a command: the whole point of a filter box is that `q`
        // does not quit while it is open.
        KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => Some(Action::Char(c)),
        _ => None,
    }
}

fn normal(view: View, key: &KeyEvent) -> Option<Action> {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char('q') => Some(Action::Quit),
        KeyCode::Char('r') | KeyCode::F(5) => Some(Action::Refresh),
        KeyCode::Char('?') => Some(Action::Help),
        KeyCode::Char(':') => Some(Action::Palette),
        KeyCode::Char('/') => Some(Action::Search),
        KeyCode::Char('l') => Some(Action::ToggleLog),
        KeyCode::Tab => Some(Action::NextView),
        KeyCode::BackTab => Some(Action::PrevView),
        KeyCode::Char(c @ '1'..='7') => {
            View::from_number(c.to_digit(10)? as usize).map(Action::GoView)
        }
        KeyCode::Char('j') | KeyCode::Down => Some(Action::Down),
        KeyCode::Char('k') | KeyCode::Up => Some(Action::Up),
        KeyCode::Char('g') | KeyCode::Home => Some(Action::Top),
        KeyCode::Char('G') | KeyCode::End => Some(Action::Bottom),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Char('f') if ctrl => Some(Action::PageDown),
        KeyCode::Char('b') if ctrl => Some(Action::PageUp),
        KeyCode::Enter => Some(Action::Accept),
        KeyCode::Esc => Some(Action::Cancel),
        KeyCode::Char('a') => Some(Action::Approve),
        // The destructive key, and what it destroys depends on what is on screen. Both are
        // confirmed, and both name what they are about to do.
        KeyCode::Char('x') => Some(match view {
            View::Infrastructure => Action::Remove,
            _ => Action::Reject,
        }),
        KeyCode::Char('t') => Some(Action::Trigger),
        KeyCode::Char('s') => Some(Action::Stop),
        KeyCode::Char('o') => Some(Action::Open),
        KeyCode::Char('v') => Some(Action::Verify),
        KeyCode::Char('L') => Some(Action::Logs),
        _ => None,
    }
}

/// Read the terminal until the channel closes, forwarding every event that means something.
///
/// A plain OS thread rather than a task: `crossterm`'s reader blocks, and a blocking read inside
/// the runtime would stall every effect that is waiting on a socket. It ends when the receiver is
/// dropped, which is what [`crate::app::run`] does on its way out.
pub fn spawn_reader(tx: Sender<Msg>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || loop {
        match event::poll(POLL) {
            Ok(true) => match event::read() {
                Ok(event) => {
                    if let Some(msg) = to_msg(event) {
                        if tx.blocking_send(msg).is_err() {
                            return;
                        }
                    }
                }
                // A terminal that cannot be read is a terminal that will not be read again.
                Err(_) => return,
            },
            Ok(false) => {
                if tx.is_closed() {
                    return;
                }
            }
            Err(_) => return,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    #[test]
    fn the_herd_keys_still_do_what_they_did() {
        let view = View::Jobs;
        assert_eq!(
            map_key(Mode::Normal, view, &key(KeyCode::Char('r'))),
            Some(Action::Refresh)
        );
        assert_eq!(
            map_key(Mode::Normal, view, &key(KeyCode::F(5))),
            Some(Action::Refresh)
        );
        assert_eq!(
            map_key(Mode::Normal, view, &key(KeyCode::Char('q'))),
            Some(Action::Quit)
        );
        assert_eq!(
            map_key(Mode::Normal, view, &key(KeyCode::Char('?'))),
            Some(Action::Help)
        );
        for (c, action) in [
            ('t', Action::Trigger),
            ('s', Action::Stop),
            ('o', Action::Open),
            ('a', Action::Approve),
        ] {
            assert_eq!(
                map_key(Mode::Normal, view, &key(KeyCode::Char(c))),
                Some(action)
            );
        }
    }

    #[test]
    fn r_is_refresh_on_every_view_including_the_one_where_herd_made_it_reject() {
        for view in View::ALL {
            assert_eq!(
                map_key(Mode::Normal, view, &key(KeyCode::Char('r'))),
                Some(Action::Refresh),
                "{view:?} must not turn refresh into a mutation"
            );
        }
        assert_eq!(
            map_key(Mode::Normal, View::Attention, &key(KeyCode::Char('x'))),
            Some(Action::Reject)
        );
        assert_eq!(
            map_key(Mode::Normal, View::Infrastructure, &key(KeyCode::Char('x'))),
            Some(Action::Remove)
        );
    }

    #[test]
    fn the_digits_reach_all_seven_views() {
        for (i, view) in View::ALL.iter().enumerate() {
            let digit = char::from_digit(i as u32 + 1, 10).unwrap_or('0');
            assert_eq!(
                map_key(Mode::Normal, View::Attention, &key(KeyCode::Char(digit))),
                Some(Action::GoView(*view))
            );
        }
        assert_eq!(
            map_key(Mode::Normal, View::Attention, &key(KeyCode::Char('8'))),
            None
        );
    }

    #[test]
    fn a_filter_box_swallows_the_keys_that_would_otherwise_be_commands() {
        for mode in [Mode::Search, Mode::Palette, Mode::Prompt] {
            assert_eq!(
                map_key(mode, View::Jobs, &key(KeyCode::Char('q'))),
                Some(Action::Char('q'))
            );
            assert_eq!(
                map_key(mode, View::Jobs, &key(KeyCode::Enter)),
                Some(Action::Accept)
            );
            assert_eq!(
                map_key(mode, View::Jobs, &key(KeyCode::Esc)),
                Some(Action::Cancel)
            );
        }
    }

    #[test]
    fn a_confirmation_takes_only_yes_and_no() {
        assert_eq!(
            map_key(Mode::Confirm, View::Jobs, &key(KeyCode::Char('y'))),
            Some(Action::Accept)
        );
        assert_eq!(
            map_key(Mode::Confirm, View::Jobs, &key(KeyCode::Char('n'))),
            Some(Action::Cancel)
        );
        // `a` must not answer a gate while a confirmation is on screen.
        assert_eq!(
            map_key(Mode::Confirm, View::Jobs, &key(KeyCode::Char('a'))),
            None
        );
    }

    #[test]
    fn ctrl_c_is_an_interrupt_no_matter_what_is_open() {
        assert!(is_interrupt(&ctrl('c')));
        assert!(is_interrupt(&ctrl('C')));
        assert!(!is_interrupt(&key(KeyCode::Char('c'))));
        assert!(matches!(
            to_msg(Event::Key(ctrl('c'))),
            Some(Msg::Interrupt)
        ));
    }

    #[test]
    fn a_resize_is_a_message_and_a_key_release_is_not() {
        assert!(matches!(
            to_msg(Event::Resize(80, 24)),
            Some(Msg::Resize(80, 24))
        ));
        let release = KeyEvent::new_with_kind(
            KeyCode::Char('q'),
            KeyModifiers::NONE,
            KeyEventKind::Release,
        );
        assert!(to_msg(Event::Key(release)).is_none());
    }

    #[test]
    fn help_closes_on_anything() {
        assert_eq!(
            map_key(Mode::Help, View::Jobs, &key(KeyCode::Char('z'))),
            Some(Action::Cancel)
        );
    }
}
