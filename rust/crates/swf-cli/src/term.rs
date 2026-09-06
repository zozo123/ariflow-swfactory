//! What the terminal is, and what may therefore be shown.
//!
//! Two decisions live here and nowhere else. Colour is a rendering hint, so it is off whenever
//! stdout is not a terminal — a pipeline that captures `swf jobs list` must get text, not escape
//! codes. Confirmation is a safety rule, so it is refused rather than skipped when stdin is not a
//! terminal: a prompt nobody can answer is a hang, and a mutation that proceeds because nobody was
//! there to object is worse. The boundary this module defends is that neither behaviour is ever
//! decided by a command; they ask, and they get the same answer.

use std::io::{BufRead, IsTerminal, Write};

use swf_app::ops::{OpsError, Result};

/// How the process is being used: what is a terminal, and what the operator asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Term {
    /// stdout is a terminal, so colour and progress lines are meaningful.
    pub stdout_tty: bool,
    /// stdin is a terminal, so a question can be answered.
    pub stdin_tty: bool,
    /// Colour is allowed at all.
    pub color: bool,
    /// `--yes`: every confirmation is already answered.
    pub assume_yes: bool,
    /// `--json`: stdout carries exactly one document, so nothing else may be written to it.
    pub json: bool,
}

impl Term {
    /// Read the real process. `NO_COLOR` and `TERM=dumb` are honoured because they are the
    /// conventions every other tool on the operator's machine already follows.
    pub fn detect(no_color: bool, assume_yes: bool, json: bool) -> Self {
        let stdout_tty = std::io::stdout().is_terminal();
        let suppressed = no_color
            || json
            || std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
            || std::env::var("TERM").map(|t| t == "dumb").unwrap_or(false);
        Self {
            stdout_tty,
            stdin_tty: std::io::stdin().is_terminal(),
            color: stdout_tty && !suppressed,
            assume_yes,
            json,
        }
    }

    /// A terminal-free shape, for a renderer test that must see the uncoloured text.
    #[cfg(test)]
    pub fn plain() -> Self {
        Self {
            stdout_tty: false,
            stdin_tty: false,
            color: false,
            assume_yes: false,
            json: false,
        }
    }

    /// Wrap `text` in an SGR sequence, or hand it back untouched when colour is off.
    pub fn paint(&self, code: &str, text: &str) -> String {
        if self.color {
            format!("\u{1b}[{code}m{text}\u{1b}[0m")
        } else {
            text.to_string()
        }
    }

    /// Green: something is how it should be.
    pub fn good(&self, text: &str) -> String {
        self.paint("32", text)
    }

    /// Red: something is not.
    pub fn bad(&self, text: &str) -> String {
        self.paint("31", text)
    }

    /// Yellow: something needs a person.
    pub fn warn(&self, text: &str) -> String {
        self.paint("33", text)
    }

    /// Dim: context, not content.
    pub fn dim(&self, text: &str) -> String {
        self.paint("2", text)
    }

    /// Bold: a heading.
    pub fn head(&self, text: &str) -> String {
        self.paint("1", text)
    }
}

/// Ask before a mutation, or explain why the answer cannot be assumed.
///
/// The non-interactive case is an error and not a silent yes on purpose (`00-architecture.md` §6):
/// a cron job that answers gates because nobody was watching is the failure this rule exists to
/// prevent, and `--yes` is one word for an operator who really means it.
pub fn confirm(term: &Term, question: &str) -> Result<()> {
    if term.assume_yes {
        return Ok(());
    }
    if !term.stdin_tty || term.json {
        return Err(OpsError::usage(format!(
            "{question} — refusing to assume the answer with no terminal to ask"
        ))
        .with_hint("pass --yes if you mean it"));
    }
    let mut err = std::io::stderr();
    let _ = write!(err, "{question} [y/N] ");
    let _ = err.flush();
    let mut answer = String::new();
    if std::io::stdin().lock().read_line(&mut answer).is_err() {
        return Err(OpsError::usage("could not read an answer from stdin")
            .with_hint("pass --yes if you mean it"));
    }
    let answer = answer.trim().to_ascii_lowercase();
    if answer == "y" || answer == "yes" {
        Ok(())
    } else {
        Err(OpsError::operational("cancelled at the confirmation"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(stdin_tty: bool, yes: bool, json: bool) -> Term {
        Term {
            stdout_tty: false,
            stdin_tty,
            color: false,
            assume_yes: yes,
            json,
        }
    }

    #[test]
    fn a_pipe_never_gets_a_prompt_and_never_gets_a_free_yes() {
        let err = confirm(&term(false, false, false), "approve it?").expect_err("must refuse");
        assert_eq!(err.exit_code(), 2);
        assert!(err.message.contains("no terminal"), "{}", err.message);
        assert_eq!(err.hint.as_deref(), Some("pass --yes if you mean it"));
    }

    #[test]
    fn yes_answers_the_question_without_asking_it() {
        assert!(confirm(&term(false, true, false), "approve it?").is_ok());
        assert!(confirm(&term(true, true, true), "approve it?").is_ok());
    }

    #[test]
    fn json_mode_refuses_to_prompt_even_on_a_terminal() {
        // stdout carries exactly one document; a question written into it would corrupt the
        // document, and a question the operator cannot see is a hang.
        let err = confirm(&term(true, false, true), "approve it?").expect_err("must refuse");
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn colour_is_off_whenever_stdout_is_not_a_terminal() {
        let plain = Term::plain();
        assert_eq!(plain.good("ok"), "ok");
        let coloured = Term {
            color: true,
            ..Term::plain()
        };
        assert_eq!(coloured.good("ok"), "\u{1b}[32mok\u{1b}[0m");
    }
}
