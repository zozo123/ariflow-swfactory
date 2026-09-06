//! One place where an answer becomes bytes on a stream and a number for the shell.
//!
//! The exit-code table is documented public surface: a script that sees `4` must know it needs a
//! credential and not a retry, and one that sees `6` must know somebody else got there first
//! (`00-architecture.md` §6, §D-E). Classification already happened in `swf-app`; what this module
//! defends is that nothing re-derives it, that `--json` puts exactly one document on stdout and
//! every diagnostic on stderr, and that a non-zero exit under `--json` still leaves a consumer
//! with a parseable answer instead of an empty pipe.

use std::io::Write;

use serde_json::Value;
use swf_app::ops::OpsError;

/// The structured half of an answer.
///
/// `Raw` exists for the snapshot, which is a byte-diff target against the Python control room
/// (`02-herd-tui.md` §3): re-serialising it through `serde_json` here would move key order and
/// non-ASCII escaping, and the CI job that compares the two documents would fail on formatting
/// rather than on facts.
#[derive(Debug, Clone, PartialEq)]
pub enum Doc {
    /// The command has no structured form; `--json` prints `null`.
    None,
    /// A document to render with the standard pretty printer.
    Value(Value),
    /// A document that is already exactly the bytes to print.
    Raw(String),
}

/// What a command produced: the human rendering, the machine rendering, and the shell's answer.
///
/// A command can succeed at *reporting* and still exit non-zero — `swf doctor` with a red row is
/// the whole reason this is a struct and not a `Result`. That case prints the report, not an error
/// envelope, because the report is the answer the operator ran the command to get.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    /// What a person reads. No trailing newline; the writer adds one.
    pub text: String,
    /// What a script reads.
    pub doc: Doc,
    /// What the shell reads.
    pub code: i32,
}

impl Outcome {
    /// A successful answer with both renderings.
    pub fn new(text: impl Into<String>, doc: Value) -> Self {
        Self {
            text: into_text(text),
            doc: Doc::Value(doc),
            code: 0,
        }
    }

    /// A successful answer whose JSON form is already rendered, byte for byte.
    pub fn raw(text: impl Into<String>, doc: impl Into<String>) -> Self {
        Self {
            text: into_text(text),
            doc: Doc::Raw(doc.into()),
            code: 0,
        }
    }

    /// A successful answer with nothing structured to say.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: into_text(text),
            doc: Doc::None,
            code: 0,
        }
    }

    /// The same report, but the shell is told something is wrong.
    pub fn with_code(mut self, code: i32) -> Self {
        self.code = code;
        self
    }

    /// Write the answer and return the process's exit code.
    pub fn emit(&self, json: bool, out: &mut dyn Write) -> std::io::Result<i32> {
        if json {
            match &self.doc {
                Doc::Raw(text) => writeln!(out, "{text}")?,
                Doc::Value(value) => writeln!(out, "{}", render(value))?,
                Doc::None => writeln!(out, "null")?,
            }
        } else if !self.text.is_empty() {
            writeln!(out, "{}", self.text)?;
        }
        out.flush()?;
        Ok(self.code)
    }
}

/// Trim only the trailing newline a renderer left behind, so `emit` decides line endings.
fn into_text(text: impl Into<String>) -> String {
    let text = text.into();
    text.strip_suffix('\n').map(str::to_string).unwrap_or(text)
}

/// `indent = 2`, matching `json.dumps(..., indent=2)` — the shape every other document in this
/// product is compared against.
fn render(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".to_string())
}

/// Print a failure the way the operator asked to be told about it, and answer the exit code.
///
/// Under `--json` the envelope goes to **stdout**, because it is the one document the command
/// promised; the same words also go to stderr so a human watching a `| jq` still sees what
/// happened. Without `--json` there is nothing on stdout at all: a shell substitution that
/// captured a failure must capture emptiness, never an error message.
pub fn report(err: &OpsError, json: bool, out: &mut dyn Write, log: &mut dyn Write) -> i32 {
    let _ = writeln!(log, "error: {}", err.message);
    if let Some(hint) = &err.hint {
        let _ = writeln!(log, "fix: {hint}");
    }
    if json {
        let _ = writeln!(out, "{}", render(&err.envelope()));
        let _ = out.flush();
    }
    let _ = log.flush();
    err.exit_code()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_exit_table_is_what_every_error_carries() {
        let cases: [(OpsError, i32, &str); 6] = [
            (OpsError::operational("red"), 1, "operational"),
            (OpsError::usage("what"), 2, "usage"),
            (OpsError::not_found("gone"), 3, "not_found"),
            (OpsError::auth("who"), 4, "auth"),
            (OpsError::unreachable("silence"), 5, "unreachable"),
            (OpsError::conflict("first"), 6, "conflict"),
        ];
        for (err, code, kind) in cases {
            let mut out = Vec::new();
            let mut log = Vec::new();
            assert_eq!(report(&err, true, &mut out, &mut log), code);
            let doc: Value = serde_json::from_slice(&out).expect("one json document");
            assert_eq!(doc["error"]["kind"], kind);
            assert_eq!(doc["error"]["exit_code"], code);
        }
    }

    #[test]
    fn a_failure_without_json_leaves_stdout_empty() {
        let mut out = Vec::new();
        let mut log = Vec::new();
        let err = OpsError::not_found("no such run").with_hint("swf runs list");
        assert_eq!(report(&err, false, &mut out, &mut log), 3);
        assert!(out.is_empty(), "stdout must stay clean for a pipeline");
        let text = String::from_utf8(log).expect("utf8");
        assert!(text.contains("error: no such run"), "{text}");
        assert!(text.contains("fix: swf runs list"), "{text}");
    }

    #[test]
    fn json_prints_exactly_one_document_and_text_prints_none() {
        let outcome = Outcome::new("two rows", json!([{"id": "a"}, {"id": "b"}]));
        let mut out = Vec::new();
        assert_eq!(outcome.emit(true, &mut out).expect("write"), 0);
        let text = String::from_utf8(out).expect("utf8");
        assert!(!text.contains("two rows"));
        let doc: Value = serde_json::from_str(&text).expect("one document");
        assert_eq!(doc.as_array().map(Vec::len), Some(2));

        let mut out = Vec::new();
        assert_eq!(outcome.emit(false, &mut out).expect("write"), 0);
        assert_eq!(String::from_utf8(out).expect("utf8"), "two rows\n");
    }

    #[test]
    fn a_report_that_is_red_still_prints_its_report() {
        // `swf doctor --json` with a failing check answers the checks, not an error envelope: the
        // e2e reads the rows out of that file to find out which row is red.
        let outcome =
            Outcome::new("1 failed", json!([{"name": "airflow api", "ok": false}])).with_code(1);
        let mut out = Vec::new();
        assert_eq!(outcome.emit(true, &mut out).expect("write"), 1);
        let doc: Value = serde_json::from_slice(&out).expect("one document");
        assert!(doc.get("error").is_none());
        assert_eq!(doc[0]["name"], "airflow api");
    }

    #[test]
    fn a_prerendered_document_is_printed_unchanged() {
        let outcome = Outcome::raw("text", "{\n  \"b\": 1,\n  \"a\": 2\n}");
        let mut out = Vec::new();
        assert_eq!(outcome.emit(true, &mut out).expect("write"), 0);
        assert_eq!(
            String::from_utf8(out).expect("utf8"),
            "{\n  \"b\": 1,\n  \"a\": 2\n}\n"
        );
    }
}
