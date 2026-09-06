//! The committed metrics history: what one run recorded about itself, and the aggregate.
//!
//! `docs/factory/<issue>/metrics.json` is written by the run that produced it, which makes every
//! field here **self-reported** — the boundary this module defends is that it never pretends
//! otherwise. Nothing in `summarize` verifies a claim; it counts them. `swf deliveries verify`
//! exists because a reported `tests_passed` is not a verified one (see [`crate::evidence`]).
//!
//! The arithmetic is Python's, deliberately. `median` averages the two middle values on an even
//! count, `mean_iterations` averages only the runs that actually reported iterations, and the cost
//! is rounded half-to-even at six places — these are `statistics.median`, `statistics.fmean` and
//! `round()`, and a fixture pins each of them.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// The severity vocabulary, in the order every histogram and every rendered row uses.
pub const SEVERITIES: &[&str] = &["blocker", "major", "minor", "nit"];

/// The nine keys [`summarize`] emits, in order. The snapshot renderer restates this order because
/// an untyped `Value` cannot carry it.
pub const SUMMARY_KEYS: &[&str] = &[
    "runs",
    "scripted_runs",
    "first_pass_rate",
    "mean_iterations",
    "p50_cycle_s",
    "tests_pass_rate",
    "findings_by_severity",
    "blockers",
    "total_cost_usd",
];

/// The label column width in [`table`] — `"mean build iterations"`, the longest row label.
const LABEL_WIDTH: usize = 21;

/// One recorded approval, as `metrics.json` carries it.
///
/// `artifact_sha256` is the only cryptographic binding in the whole artifact chain: it says the
/// operator approved *these bytes*. It is `None` for a gate answered before the hash existed, and
/// a verifier must treat that as "unverifiable", never as "matches".
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Approval {
    /// `intent` or `plan`.
    #[serde(default)]
    pub gate: String,
    /// `approve` or `reject`.
    #[serde(default)]
    pub decision: String,
    /// Who answered. `auto` when the blueprint's gate self-approved.
    #[serde(default)]
    pub actor: String,
    /// When, as the ISO string the file carries — kept verbatim so a re-render is lossless.
    #[serde(default)]
    pub at: String,
    /// 64 lowercase hex of the artifact the operator was shown, when one was recorded.
    #[serde(default)]
    pub artifact_sha256: Option<String>,
}

/// The four-way histogram, always with all four keys present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FindingsBySeverity {
    /// Findings that block the delivery outright.
    #[serde(default, deserialize_with = "de_i64")]
    pub blocker: i64,
    /// Findings worth fixing before merge.
    #[serde(default, deserialize_with = "de_i64")]
    pub major: i64,
    /// Findings worth noting.
    #[serde(default, deserialize_with = "de_i64")]
    pub minor: i64,
    /// Findings capped by `review.nit_cap`.
    #[serde(default, deserialize_with = "de_i64")]
    pub nit: i64,
}

impl FindingsBySeverity {
    /// The count for one severity name, or 0 for a name outside [`SEVERITIES`].
    pub fn get(&self, severity: &str) -> i64 {
        match severity {
            "blocker" => self.blocker,
            "major" => self.major,
            "minor" => self.minor,
            "nit" => self.nit,
            _ => 0,
        }
    }

    /// Accumulate another run's histogram.
    fn add(&mut self, other: &Self) {
        self.blocker += other.blocker;
        self.major += other.major;
        self.minor += other.minor;
        self.nit += other.nit;
    }
}

/// One run's `metrics.json`, field for field.
///
/// Every field defaults, and the numeric ones read leniently, because this file is written by a
/// past version of the factory and read by a future one. A metrics file that gained a key must
/// not make `swf metrics` fail, and a key that lost its type must not either — the honest
/// degradation is a zero, not a crash.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RunMetrics {
    /// The factory run id; a file without one is not a run record at all.
    #[serde(default)]
    pub run_id: String,
    /// The issue this run answered — also the directory name.
    #[serde(default)]
    pub issue_id: String,
    /// The blueprint name, i.e. the DAG id.
    #[serde(default)]
    pub blueprint: String,
    /// The agent kind. `scripted` marks a demo replay, which the aggregate counts separately.
    #[serde(default)]
    pub agent: String,
    /// The sandbox kind: local/islo/srt/docker/toolset.
    #[serde(default)]
    pub sandbox: String,
    /// The provider's sandbox name.
    #[serde(default)]
    pub sandbox_name: String,
    /// The SCM kind.
    #[serde(default)]
    pub scm: String,
    /// When the run started, as the ISO string the file carries.
    #[serde(default)]
    pub started: String,
    /// When the run finished, as the ISO string the file carries.
    #[serde(default)]
    pub finished: String,
    /// Seconds per stage. Repeated stages keep the last record's duration.
    #[serde(default)]
    pub stage_durations_s: BTreeMap<String, f64>,
    /// `ok` / `skipped` / `blocked` / `failed` per stage.
    #[serde(default)]
    pub stage_status: BTreeMap<String, String>,
    /// The whole cycle in seconds, summed over every stage record including repeats.
    #[serde(default, deserialize_with = "de_f64")]
    pub cycle_s: f64,
    /// Build/test iterations. Falsy means "not reported", which is why the mean skips it.
    #[serde(default, deserialize_with = "de_i64")]
    pub iterations: i64,
    /// Whether CI was green on the first attempt.
    #[serde(default, deserialize_with = "de_truthy")]
    pub first_pass_ci: bool,
    /// Whether the last stage that ran tests reported them green.
    #[serde(default, deserialize_with = "de_truthy")]
    pub tests_passed: bool,
    /// The review histogram.
    #[serde(default, deserialize_with = "de_findings")]
    pub findings_by_severity: FindingsBySeverity,
    /// Duplicated out of the histogram for convenience, as the Python writes it.
    #[serde(default, deserialize_with = "de_i64")]
    pub blockers: i64,
    /// How many fixes the review round produced.
    #[serde(default, deserialize_with = "de_i64")]
    pub review_fixes: i64,
    /// Tool calls the sandbox hook denied. Zero when the hook never ran.
    #[serde(default, deserialize_with = "de_i64")]
    pub denied_tool_calls: i64,
    /// Agent spend in USD.
    #[serde(default, deserialize_with = "de_f64")]
    pub total_cost_usd: f64,
    /// Who approved, in gate order.
    #[serde(default)]
    pub approvers: Vec<String>,
    /// The approval records themselves.
    #[serde(default)]
    pub approvals: Vec<Approval>,
}

impl RunMetrics {
    /// True when this record is a demo replay rather than a real agent run.
    pub fn scripted(&self) -> bool {
        self.agent == "scripted"
    }
}

/// The aggregate over a set of runs. Nine fields, in the order the JSON must print them.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MetricsSummary {
    /// How many runs went into the aggregate.
    #[serde(default)]
    pub runs: usize,
    /// How many of those were scripted replays rather than real agent runs.
    #[serde(default)]
    pub scripted_runs: usize,
    /// Fraction of runs whose CI was green first time. `0.0` for no runs.
    #[serde(default, deserialize_with = "de_f64")]
    pub first_pass_rate: f64,
    /// Mean build iterations over the runs that reported any.
    #[serde(default, deserialize_with = "de_f64")]
    pub mean_iterations: f64,
    /// Median cycle time over all runs, missing values counted as zero.
    #[serde(default, deserialize_with = "de_f64")]
    pub p50_cycle_s: f64,
    /// Fraction of runs reporting green tests.
    #[serde(default, deserialize_with = "de_f64")]
    pub tests_pass_rate: f64,
    /// Findings summed across runs.
    #[serde(default, deserialize_with = "de_findings")]
    pub findings_by_severity: FindingsBySeverity,
    /// The blocker count, lifted out of the histogram.
    #[serde(default, deserialize_with = "de_i64")]
    pub blockers: i64,
    /// Total agent spend, rounded to six places.
    #[serde(default, deserialize_with = "de_f64")]
    pub total_cost_usd: f64,
}

/// Aggregate a set of run records.
///
/// The two rates count *truthy* values rather than `true`, and `mean_iterations` averages only
/// runs that reported a non-zero iteration count. That last one looks like a bug and is not: a
/// run that never reached `build_and_test` has no iteration count to average, and folding a zero
/// in would make the fleet look faster the more often it failed early.
pub fn summarize(runs: &[RunMetrics]) -> MetricsSummary {
    let n = runs.len();
    let mut findings = FindingsBySeverity::default();
    for run in runs {
        findings.add(&run.findings_by_severity);
    }
    let cycles: Vec<f64> = runs.iter().map(|r| r.cycle_s).collect();
    let iterations: Vec<f64> = runs
        .iter()
        .filter(|r| r.iterations != 0)
        .map(|r| r.iterations as f64)
        .collect();
    let rate = |count: usize| if n > 0 { count as f64 / n as f64 } else { 0.0 };

    MetricsSummary {
        runs: n,
        scripted_runs: runs.iter().filter(|r| r.scripted()).count(),
        first_pass_rate: rate(runs.iter().filter(|r| r.first_pass_ci).count()),
        mean_iterations: fmean(&iterations),
        p50_cycle_s: median(&cycles),
        tests_pass_rate: rate(runs.iter().filter(|r| r.tests_passed).count()),
        findings_by_severity: findings,
        blockers: findings.blocker,
        total_cost_usd: round_half_even(runs.iter().map(|r| r.total_cost_usd).sum(), 6),
    }
}

/// `statistics.median`: the middle value, or the **mean of the two middle values** on an even
/// count. Taking the lower middle would be a different number and a failing fixture.
pub fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    }
}

/// `statistics.fmean`: the arithmetic mean, and `0.0` rather than an error for no input.
pub fn fmean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Python's `round(x, n)` — nearest, ties to even.
///
/// Rust's `f64::round` breaks ties away from zero, which differs from Python at exactly the
/// values a hand-written test picks (`round(0.5)` is `0` there and `1` here). Money is summed
/// here, so the tie rule is not academic.
pub fn round_half_even(value: f64, digits: i32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let factor = 10f64.powi(digits);
    let scaled = value * factor;
    let truncated = scaled.trunc();
    let fraction = scaled - truncated;
    let rounded = if (fraction.abs() - 0.5).abs() < f64::EPSILON {
        // An exact tie: go to the even neighbour, as Python does.
        if (truncated / 2.0).fract() == 0.0 {
            truncated
        } else {
            truncated + fraction.signum()
        }
    } else {
        scaled.round()
    };
    rounded / factor
}

/// Render the summary as the seven-row text table `swfactory metrics` prints.
///
/// Labels are left-justified to 21 characters, then exactly two spaces, then the value; no
/// trailing newline. It is a fixed layout rather than a computed one because the numbers change
/// every run and a column that moves is a column nobody can scan.
pub fn table(summary: &MetricsSummary) -> String {
    let findings = SEVERITIES
        .iter()
        .map(|s| format!("{s}={}", summary.findings_by_severity.get(s)))
        .collect::<Vec<_>>()
        .join(", ");
    let rows: [(&str, String); 7] = [
        (
            "runs",
            format!("{} ({} scripted)", summary.runs, summary.scripted_runs),
        ),
        (
            "first-pass rate",
            format!("{:.0}%", summary.first_pass_rate * 100.0),
        ),
        (
            "mean build iterations",
            format!("{:.2}", summary.mean_iterations),
        ),
        ("p50 cycle time", format!("{:.1}s", summary.p50_cycle_s)),
        (
            "tests pass rate",
            format!("{:.0}%", summary.tests_pass_rate * 100.0),
        ),
        ("findings", findings),
        ("total cost", format!("${:.4}", summary.total_cost_usd)),
    ];
    rows.iter()
        .map(|(label, value)| format!("{label:<LABEL_WIDTH$}  {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Render whatever a metrics source produced, including a partial or absent summary.
///
/// Mirrors `herd._metrics_text`: an empty or unreadable summary says so in words rather than
/// rendering a table of zeros that looks like a measurement.
pub fn table_value(metrics: &Value) -> String {
    match metrics {
        Value::Object(fields) if !fields.is_empty() => {
            match serde_json::from_value::<MetricsSummary>(metrics.clone()) {
                Ok(summary) => table(&summary),
                Err(_) => metrics.to_string(),
            }
        }
        Value::Object(_) | Value::Null => "(no metrics yet)".to_string(),
        other => {
            if crate::rollup::py_truthy(other) {
                crate::rollup::py_str(other)
            } else {
                "(no metrics yet)".to_string()
            }
        }
    }
}

/// Read a number from whatever the file actually held, as Python's `float()` would.
fn de_f64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    Ok(as_f64(&Value::deserialize(deserializer)?))
}

fn as_f64(value: &Value) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::Bool(b) => f64::from(u8::from(*b)),
        Value::String(s) => s.trim().parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Read an integer, truncating a float the way Python's `int()` does.
fn de_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let value = Value::deserialize(deserializer)?;
    Ok(match &value {
        Value::Number(n) => n.as_i64().unwrap_or_else(|| as_f64(&value).trunc() as i64),
        other => as_f64(other).trunc() as i64,
    })
}

/// Read a flag by Python truthiness, so a `1` written by an older factory still reads as true.
fn de_truthy<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Ok(crate::rollup::py_truthy(&Value::deserialize(deserializer)?))
}

/// Read the histogram, tolerating `null` and a missing severity.
fn de_findings<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<FindingsBySeverity, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let Some(fields) = value.as_object() else {
        return Ok(FindingsBySeverity::default());
    };
    let count = |key: &str| {
        fields
            .get(key)
            .map(|v| as_f64(v).trunc() as i64)
            .unwrap_or(0)
    };
    Ok(FindingsBySeverity {
        blocker: count("blocker"),
        major: count("major"),
        minor: count("minor"),
        nit: count("nit"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(agent: &str, cycle: f64, iterations: i64, first_pass: bool, tests: bool) -> RunMetrics {
        RunMetrics {
            run_id: "r1".into(),
            agent: agent.into(),
            cycle_s: cycle,
            iterations,
            first_pass_ci: first_pass,
            tests_passed: tests,
            ..RunMetrics::default()
        }
    }

    #[test]
    fn an_empty_aggregate_is_all_zeros_not_nan() {
        let summary = summarize(&[]);
        assert_eq!(summary.runs, 0);
        assert_eq!(summary.first_pass_rate, 0.0);
        assert_eq!(summary.mean_iterations, 0.0);
        assert_eq!(summary.p50_cycle_s, 0.0);
        assert_eq!(summary.tests_pass_rate, 0.0);
        assert_eq!(summary.total_cost_usd, 0.0);
        assert_eq!(summary.blockers, 0);
    }

    #[test]
    fn the_summary_keys_are_exactly_the_nine_python_emits_in_order() {
        let text = serde_json::to_string_pretty(&summarize(&[])).unwrap_or_default();
        let keys: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("  \""))
            .filter_map(|l| l.trim().split('"').nth(1))
            .collect();
        assert_eq!(keys, SUMMARY_KEYS.to_vec());
    }

    #[test]
    fn rates_count_truthy_values_over_every_run() {
        let runs = vec![
            run("claude", 10.0, 2, true, true),
            run("scripted", 20.0, 0, false, false),
        ];
        let summary = summarize(&runs);
        assert_eq!(summary.runs, 2);
        assert_eq!(summary.scripted_runs, 1);
        assert_eq!(summary.first_pass_rate, 0.5);
        assert_eq!(summary.tests_pass_rate, 0.5);
        // Only the run that reported iterations is averaged.
        assert_eq!(summary.mean_iterations, 2.0);
        assert_eq!(summary.p50_cycle_s, 15.0);
    }

    #[test]
    fn an_even_median_averages_the_two_middle_values() {
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        assert_eq!(median(&[3.0, 1.0]), 2.0);
        assert_eq!(median(&[5.0]), 5.0);
        assert_eq!(median(&[]), 0.0);
        // Unsorted input must be sorted first, not taken positionally.
        assert_eq!(median(&[9.0, 1.0, 5.0]), 5.0);
    }

    #[test]
    fn fmean_is_zero_for_no_input() {
        assert_eq!(fmean(&[]), 0.0);
        assert_eq!(fmean(&[1.0, 2.0]), 1.5);
    }

    #[test]
    fn cost_rounds_half_to_even_like_python() {
        assert_eq!(round_half_even(0.5, 0), 0.0);
        assert_eq!(round_half_even(1.5, 0), 2.0);
        assert_eq!(round_half_even(2.5, 0), 2.0);
        assert_eq!(round_half_even(-0.5, 0), 0.0);
        assert_eq!(round_half_even(-1.5, 0), -2.0);
        assert!((round_half_even(1.234_567_8, 6) - 1.234_568).abs() < 1e-12);
    }

    #[test]
    fn findings_sum_across_runs_and_blockers_is_lifted_out() {
        let mut a = RunMetrics::default();
        a.findings_by_severity = FindingsBySeverity {
            blocker: 1,
            major: 2,
            minor: 0,
            nit: 3,
        };
        let mut b = RunMetrics::default();
        b.findings_by_severity = FindingsBySeverity {
            blocker: 2,
            major: 0,
            minor: 1,
            nit: 0,
        };
        let summary = summarize(&[a, b]);
        assert_eq!(summary.findings_by_severity.blocker, 3);
        assert_eq!(summary.findings_by_severity.major, 2);
        assert_eq!(summary.findings_by_severity.minor, 1);
        assert_eq!(summary.findings_by_severity.nit, 3);
        assert_eq!(summary.blockers, 3);
    }

    #[test]
    fn the_table_is_seven_rows_padded_to_twenty_one_columns() {
        let runs = vec![
            run("claude", 0.0, 0, true, false),
            run("claude", 0.0, 0, false, false),
        ];
        let mut summary = summarize(&runs);
        summary.findings_by_severity.blocker = 1;
        let text = table(&summary);
        assert_eq!(
            text,
            "runs                   2 (0 scripted)\n\
             first-pass rate        50%\n\
             mean build iterations  0.00\n\
             p50 cycle time         0.0s\n\
             tests pass rate        0%\n\
             findings               blocker=1, major=0, minor=0, nit=0\n\
             total cost             $0.0000"
        );
        assert!(!text.ends_with('\n'));
        assert!(text.lines().all(|l| l.len() >= LABEL_WIDTH));
    }

    #[test]
    fn a_partial_or_absent_summary_renders_without_pretending_to_measure() {
        assert_eq!(table_value(&json!({})), "(no metrics yet)");
        assert_eq!(table_value(&Value::Null), "(no metrics yet)");
        assert_eq!(table_value(&json!("")), "(no metrics yet)");
        assert_eq!(table_value(&json!("boom")), "boom");
        let partial = table_value(&json!({"runs": 3}));
        assert!(partial.starts_with("runs                   3 (0 scripted)"), "{partial}");
        assert!(partial.ends_with("$0.0000"), "{partial}");
    }

    #[test]
    fn a_metrics_file_with_loose_types_still_reads() {
        let value = json!({
            "run_id": "r1",
            "agent": "scripted",
            "cycle_s": "12.5",
            "iterations": 2.9,
            "first_pass_ci": 1,
            "tests_passed": 0,
            "findings_by_severity": null,
            "total_cost_usd": null,
        });
        let parsed: RunMetrics = match serde_json::from_value(value) {
            Ok(m) => m,
            Err(e) => panic!("lenient parse failed: {e}"),
        };
        assert_eq!(parsed.cycle_s, 12.5);
        assert_eq!(parsed.iterations, 2); // Python's int() truncates.
        assert!(parsed.first_pass_ci);
        assert!(!parsed.tests_passed);
        assert_eq!(parsed.findings_by_severity, FindingsBySeverity::default());
        assert_eq!(parsed.total_cost_usd, 0.0);
        assert!(parsed.scripted());
    }

    #[test]
    fn unknown_keys_in_a_metrics_file_do_not_break_the_read() {
        let value = json!({"run_id": "r1", "invented_next_quarter": {"a": 1}});
        assert!(serde_json::from_value::<RunMetrics>(value).is_ok());
    }

    #[test]
    fn approvals_round_trip_including_a_missing_hash() {
        let value = json!([
            {"gate": "intent", "decision": "approve", "actor": "auto",
             "at": "2026-09-03T12:00:00+00:00", "artifact_sha256": null},
            {"gate": "plan", "decision": "reject", "actor": "dev",
             "at": "2026-09-03T13:00:00+00:00", "artifact_sha256": "ab"},
        ]);
        let parsed: Vec<Approval> = match serde_json::from_value(value) {
            Ok(a) => a,
            Err(e) => panic!("approval parse failed: {e}"),
        };
        assert_eq!(parsed[0].artifact_sha256, None);
        assert_eq!(parsed[1].artifact_sha256.as_deref(), Some("ab"));
        assert_eq!(parsed[1].decision, "reject");
    }
}
