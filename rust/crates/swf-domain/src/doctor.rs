//! One readiness finding, and the two renderings an operator ever sees.
//!
//! `swf doctor` is the command a person runs on a machine that is not working, so its boundary is
//! *actionability*: every failing check carries a `fix:` line with the literal command to run, and
//! nothing is reported as broken unless the product actually needs it. That is what `required`
//! separates — an informational check may be red all day without changing the exit code, because
//! failing a build over an optional integration teaches people to ignore the whole report.
//!
//! The table layout is byte-compatible with `swfactory doctor` on purpose: operators have the
//! shape memorised, and scripts grep the summary line.

use serde::{Deserialize, Serialize};

/// How wide the status column is. All three status words fit, so nothing is ever truncated.
const STATUS_WIDTH: usize = 5;

/// The name column's width when there are no checks at all — the only case it is not computed.
const EMPTY_NAME_WIDTH: usize = 4;

/// What a check turned out to be.
///
/// Three values, not two, and the third is the point: `warn` is a real finding that must not
/// change the exit code. `FAIL` is shouted in capitals because it is the only one that stops
/// anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    /// The check passed.
    Ok,
    /// A required check failed. This is what `exit_code` counts.
    Fail,
    /// An informational check failed. Reported, never fatal.
    Warn,
}

impl CheckStatus {
    /// The word the table prints, exactly as the Python spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Fail => "FAIL",
            Self::Warn => "warn",
        }
    }
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One thing the doctor looked at.
///
/// `fix` is not optional decoration. A check that can fail and cannot say what to do about it is
/// a check that sends someone to a search engine, so the renderer only prints the continuation
/// line when there is one — and the absence is visible in review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    /// The short name in the left column, e.g. `islo cli`, `integration github`.
    pub name: String,
    /// Whether it passed.
    pub ok: bool,
    /// What was observed — a version, a tenant, or the reason it failed.
    #[serde(default)]
    pub detail: String,
    /// The literal command or action that would fix it. Empty for a passing check.
    #[serde(default)]
    pub fix: String,
    /// False for an informational check, which is reported but never fatal.
    #[serde(default = "yes")]
    pub required: bool,
}

fn yes() -> bool {
    true
}

impl Check {
    /// A check that passed, with what was observed.
    pub fn pass(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok: true,
            detail: detail.into(),
            fix: String::new(),
            required: true,
        }
    }

    /// A required check that failed, with the reason and the fix.
    pub fn fail(
        name: impl Into<String>,
        detail: impl Into<String>,
        fix: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            ok: false,
            detail: detail.into(),
            fix: fix.into(),
            required: true,
        }
    }

    /// Mark this check informational: it will be reported but will never change the exit code.
    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }

    /// `ok` / `FAIL` / `warn`.
    pub fn status(&self) -> CheckStatus {
        if self.ok {
            CheckStatus::Ok
        } else if self.required {
            CheckStatus::Fail
        } else {
            CheckStatus::Warn
        }
    }
}

/// The checks that actually stop the product working.
pub fn failed(checks: &[Check]) -> Vec<&Check> {
    checks.iter().filter(|c| c.required && !c.ok).collect()
}

/// `0` when every required check passed, `1` otherwise.
///
/// Only these two values. A machine-readable "how broken" scale would invite scripts to branch on
/// it, and the answer to a red doctor is always the same: read the `fix:` line.
pub fn exit_code(checks: &[Check]) -> i32 {
    i32::from(!failed(checks).is_empty())
}

/// The human report: one line per check, a `fix:` continuation under each failure, and a summary.
///
/// The name column is sized to the longest name *this run produced*, not a constant, so a context
/// with fewer integrations does not print a field of blank space.
pub fn table(checks: &[Check]) -> String {
    let width = checks
        .iter()
        .map(|c| c.name.chars().count())
        .max()
        .unwrap_or(EMPTY_NAME_WIDTH);
    let mut lines: Vec<String> = Vec::with_capacity(checks.len() + 1);
    for check in checks {
        let status = check.status().as_str();
        let line = format!(
            "{status:<STATUS_WIDTH$}{:<width$}  {}",
            check.name, check.detail
        );
        lines.push(line.trim_end().to_string());
        if !check.ok && !check.fix.is_empty() {
            // Deliberately not trimmed: the fix text starts in the detail column, so a reader
            // scanning down the report sees it as a continuation and not a new finding.
            lines.push(format!(
                "{:<STATUS_WIDTH$}{:<width$}  fix: {}",
                "", "", check.fix
            ));
        }
    }
    let bad = failed(checks).len();
    let warn = checks.iter().filter(|c| !c.ok && !c.required).count();
    let mut summary = format!("{} checks, {bad} failed", checks.len());
    if warn > 0 {
        summary.push_str(&format!(", {warn} warnings"));
    }
    lines.push(summary);
    lines.join("\n")
}

/// The `--json` report: an array, never an object, so a consumer can stream it.
///
/// Six keys per element in a fixed order, `status` last because it is derived from the two before
/// it and a reader should meet the raw facts first.
pub fn to_json(checks: &[Check]) -> String {
    let report: Vec<CheckReport<'_>> = checks.iter().map(CheckReport::from).collect();
    serde_json::to_string_pretty(&report).unwrap_or_else(|_| "[]".to_string())
}

/// The same report as a value, for a caller embedding it in a larger document.
pub fn to_json_value(checks: &[Check]) -> serde_json::Value {
    let report: Vec<CheckReport<'_>> = checks.iter().map(CheckReport::from).collect();
    serde_json::to_value(report).unwrap_or(serde_json::Value::Null)
}

/// The JSON projection. A separate type because `status` is derived and must still serialise in
/// the position the Python's `asdict` + explicit key puts it.
#[derive(Serialize)]
struct CheckReport<'a> {
    name: &'a str,
    ok: bool,
    detail: &'a str,
    fix: &'a str,
    required: bool,
    status: &'static str,
}

impl<'a> From<&'a Check> for CheckReport<'a> {
    fn from(check: &'a Check) -> Self {
        Self {
            name: &check.name,
            ok: check.ok,
            detail: &check.detail,
            fix: &check.fix,
            required: check.required,
            status: check.status().as_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<Check> {
        vec![
            Check::pass("islo cli", "islo 0.48.1"),
            Check::pass("integration github", "connected (json)"),
            Check::fail(
                "gateway profile",
                "'swfactory' not found; have: default",
                "islo gateway create --name swfactory",
            ),
        ]
    }

    #[test]
    fn status_has_exactly_three_spellings() {
        assert_eq!(Check::pass("a", "").status().as_str(), "ok");
        assert_eq!(Check::fail("a", "", "").status().as_str(), "FAIL");
        assert_eq!(
            Check::fail("a", "", "").optional().status().as_str(),
            "warn"
        );
    }

    #[test]
    fn an_informational_failure_never_changes_the_exit_code() {
        let checks = vec![Check::fail("optional thing", "missing", "install it").optional()];
        assert_eq!(exit_code(&checks), 0);
        assert!(failed(&checks).is_empty());
        assert!(table(&checks).ends_with("1 checks, 0 failed, 1 warnings"));
    }

    #[test]
    fn the_table_columns_are_status_five_then_the_longest_name_then_two_spaces() {
        let text = table(&sample());
        let lines: Vec<&str> = text.lines().collect();
        // "integration github" is 18 characters, so every name column is 18 wide.
        assert_eq!(lines[0], "ok   islo cli            islo 0.48.1");
        assert_eq!(lines[1], "ok   integration github  connected (json)");
        assert_eq!(
            lines[2],
            "FAIL gateway profile     'swfactory' not found; have: default"
        );
        assert_eq!(
            lines[3],
            "                         fix: islo gateway create --name swfactory"
        );
        assert_eq!(lines[4], "3 checks, 1 failed");
        assert!(!text.ends_with('\n'));
    }

    #[test]
    fn the_fix_line_starts_in_the_detail_column() {
        let text = table(&sample());
        let lines: Vec<&str> = text.lines().collect();
        let detail_col = lines[2].find('\'').unwrap_or_default();
        let fix_col = lines[3].find("fix:").unwrap_or_default();
        assert_eq!(detail_col, fix_col);
    }

    #[test]
    fn an_empty_detail_leaves_no_trailing_whitespace() {
        let text = table(&[Check::pass("a", "")]);
        assert_eq!(text.lines().next(), Some("ok   a"));
    }

    #[test]
    fn a_failure_with_no_fix_emits_no_continuation_line() {
        let text = table(&[Check::fail("a", "broken", "")]);
        assert_eq!(text.lines().count(), 2, "{text}");
        assert!(!text.contains("fix:"));
    }

    #[test]
    fn an_empty_check_list_still_reports_a_summary() {
        assert_eq!(table(&[]), "0 checks, 0 failed");
        assert_eq!(exit_code(&[]), 0);
        assert_eq!(to_json(&[]), "[]");
    }

    #[test]
    fn the_json_report_is_an_array_of_six_ordered_keys() {
        let text = to_json(&[Check::pass("islo cli", "islo 0.48.1")]);
        assert!(text.starts_with('['), "{text}");
        let keys: Vec<&str> = text
            .lines()
            .filter(|l| l.trim_start().starts_with('"'))
            .filter_map(|l| l.trim().split('"').nth(1))
            .collect();
        assert_eq!(
            keys,
            vec!["name", "ok", "detail", "fix", "required", "status"]
        );
        assert!(text.contains("\"status\": \"ok\""), "{text}");
        assert!(!text.ends_with('\n'));
    }

    #[test]
    fn a_wide_name_widens_every_row_together() {
        let checks = vec![
            Check::pass("a", "x"),
            Check::pass("a much longer name", "y"),
        ];
        let text = table(&checks);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0].find('x'), lines[1].find('y'));
    }

    #[test]
    fn a_check_round_trips_through_json() {
        let check = Check::fail("a", "b", "c").optional();
        let text = match serde_json::to_string(&check) {
            Ok(text) => text,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(serde_json::from_str::<Check>(&text).ok(), Some(check));
    }
}
