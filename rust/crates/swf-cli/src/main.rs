//! `swf` — one command that operates the whole factory.
//!
//! The binary's own job is small and worth stating: parse, connect, dispatch, print, exit. Every
//! decision it renders was made in `swf-app`, where `swf tui` makes the same ones by the same
//! route, so the two faces of the product cannot drift (`00-architecture.md` §0).
//!
//! The boundary this file defends is the process contract a script depends on. There is exactly
//! one exit from the program, so the exit-code table is enforced in one place; stdout carries the
//! answer and nothing else, so `swf … --json | jq` is safe even when the command failed; and the
//! first Ctrl-C cancels in-flight work through a token the adapters already honour, rather than
//! tearing the process down in the middle of a write.

mod cell_exec;
mod cli;
mod exec;
mod exit;
mod json;
mod operator_exec;
mod render;
mod term;

use std::io::Write;

use clap::{CommandFactory, Parser};
use swf_app::ops::OpsError;
use tokio_util::sync::CancellationToken;

use crate::cli::{Cli, Command};
use crate::exec::Ctx;
use crate::term::Term;

fn main() {
    std::process::exit(start());
}

/// Parse, run, print. The only place that decides what the shell is told.
fn start() -> i32 {
    let argv: Vec<String> = std::env::args().collect();
    let wants_json = argv.iter().any(|arg| arg == "--json");

    // Queue/repair/fleet/compatibility were added during the liquid-development stabilization.
    // Their parser is temporarily isolated so the huge legacy clap enum does not become a merge
    // hotspot. The commands immediately delegate to the shared swf-app/FactoryApi path; no service
    // or business logic is duplicated. Issue #197 tracks folding this spelling shim into cli.rs.
    if operator_command(&argv) {
        return operator_exec::start(&argv);
    }

    let cli = match Cli::try_parse_from(&argv) {
        Ok(cli) => cli,
        Err(err) => {
            let _ = err.print();
            if !err.use_stderr() {
                return 0;
            }
            if wants_json {
                let usage = OpsError::usage(first_line(&err.to_string())).with_hint("swf --help");
                let _ = writeln!(
                    std::io::stdout(),
                    "{}",
                    serde_json::to_string_pretty(&usage.envelope())
                        .unwrap_or_else(|_| "null".to_string())
                );
            }
            return 2;
        }
    };

    if let Command::Completions(args) = &cli.command {
        let mut command = Cli::command();
        let name = command.get_name().to_string();
        clap_complete::generate(args.shell, &mut command, name, &mut std::io::stdout());
        return 0;
    }

    let term = Term::detect(cli.no_color, cli.yes, cli.json);
    let json = cli.json;
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            let failure = OpsError::operational(format!("cannot start the async runtime: {err}"));
            return exit::report(
                &failure,
                json,
                &mut std::io::stdout(),
                &mut std::io::stderr(),
            );
        }
    };

    let cancel = CancellationToken::new();
    let ctx = Ctx { cli, term, cancel };
    runtime.block_on(async move {
        watch_for_interrupt(ctx.cancel.clone());
        match exec::run(&ctx).await {
            Ok(outcome) => outcome
                .emit(json, &mut std::io::stdout())
                .unwrap_or_else(|err| {
                    let _ = writeln!(std::io::stderr(), "error: cannot write the answer: {err}");
                    1
                }),
            Err(failure) => exit::report(
                &failure,
                json,
                &mut std::io::stdout(),
                &mut std::io::stderr(),
            ),
        }
    })
}

fn operator_command(argv: &[String]) -> bool {
    if !operator_exec::recognizes(argv) {
        return false;
    }
    let mut index = 1;
    while index < argv.len() {
        let arg = argv[index].as_str();
        if matches!(arg, "--context" | "--timeout") {
            index += 2;
            continue;
        }
        if arg.starts_with("--context=") || arg.starts_with("--timeout=") {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            index += 1;
            continue;
        }
        return matches!(arg, "queue" | "operations" | "fleet" | "compatibility");
    }
    false
}

/// Cancel in-flight work on the first Ctrl-C, and let the second one end the process.
fn watch_for_interrupt(cancel: CancellationToken) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = writeln!(std::io::stderr(), "interrupted; stopping");
            cancel.cancel();
            if tokio::signal::ctrl_c().await.is_ok() {
                std::process::exit(130);
            }
        }
    });
}

fn first_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("invalid arguments")
        .trim_start_matches("error: ")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clap_failure_becomes_one_sentence() {
        let text = "error: unexpected argument '--nope' found\n\nUsage: swf doctor\n";
        assert_eq!(first_line(text), "unexpected argument '--nope' found");
        assert_eq!(first_line("   \n\n"), "invalid arguments");
    }

    #[test]
    fn the_parser_still_builds_after_every_command_was_added() {
        Cli::command().debug_assert();
    }

    #[test]
    fn operator_dispatch_ignores_global_option_values() {
        let argv = vec!["swf", "--context", "fleet", "doctor"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert!(!operator_command(&argv));
        let argv = vec!["swf", "--json", "queue", "list"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert!(operator_command(&argv));
    }
}