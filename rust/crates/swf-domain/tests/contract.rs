//! The Rust half of the contract-equivalence harness.
//!
//! `tests/fixtures/contract/*.json` holds `{"function", "cases": [{"name", "input", "expected"}]}`
//! generated from the Python implementation these functions were ported from. A pytest module
//! asserts the Python still produces `expected`; this asserts the Rust does. Neither side can
//! drift without a red build, which is the only mechanism that makes "byte-compatible with
//! `swfactory herd --once --json`" a checkable claim rather than an intention.
//!
//! The boundary this file defends is that it never *invents* a contract. It reads whatever the
//! generator wrote, is tolerant about how an argument is spelled (a bare array, or named under
//! the Python keyword), and is completely intolerant about the answer: a mismatch fails, loudly,
//! naming the fixture, the case and both values. When the directory does not exist yet — another
//! agent generates it — the test skips with a message instead of failing, because a missing
//! safety net is a different problem from a broken one and should not look the same.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use swf_domain::model::{Run, Snapshot, TaskState};
use swf_domain::{blueprint, doctor, metrics, rollup, snapshot};

/// One fixture file: every case for one ported function.
#[derive(Debug, Deserialize)]
struct Fixture {
    /// The ported function's name, as the Python spells it.
    function: String,
    /// The cases, in generation order.
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    expected: Value,
}

#[test]
fn rust_reproduces_every_python_contract_fixture() {
    let Some(dir) = fixture_dir() else {
        println!(
            "SKIP: no contract fixtures at {}. They are generated from the Python \
             implementation; run the generator (or wait for the agent that owns it) and re-run \
             this test. Set SWF_CONTRACT_FIXTURES to point somewhere else.",
            expected_dir().display()
        );
        return;
    };

    let mut files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
            .collect(),
        Err(e) => panic!("cannot read {}: {e}", dir.display()),
    };
    files.sort();

    if files.is_empty() {
        println!(
            "SKIP: {} exists but holds no *.json fixtures yet.",
            dir.display()
        );
        return;
    }

    let mut failures: Vec<String> = Vec::new();
    let mut unhandled: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for path in &files {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) => {
                failures.push(format!("{}: unreadable: {e}", path.display()));
                continue;
            }
        };
        let fixture: Fixture = match serde_json::from_str(&text) {
            Ok(fixture) => fixture,
            Err(e) => {
                failures.push(format!("{}: not a fixture document: {e}", path.display()));
                continue;
            }
        };
        for case in &fixture.cases {
            match apply(&fixture.function, &case.input) {
                Outcome::Unhandled => {
                    unhandled.push(format!("{} ({})", fixture.function, path.display()));
                    break;
                }
                Outcome::Error(why) => failures.push(format!(
                    "{}::{} [{}]: could not build the input: {why}\n  input: {}",
                    fixture.function,
                    case.name,
                    path.display(),
                    pretty(&case.input),
                )),
                Outcome::Value(actual) => {
                    checked += 1;
                    if !json_eq(&actual, &case.expected) {
                        failures.push(format!(
                            "{}::{} [{}] diverged from the Python\n  input:    {}\n  \
                             expected: {}\n  actual:   {}",
                            fixture.function,
                            case.name,
                            path.display(),
                            pretty(&case.input),
                            pretty(&case.expected),
                            pretty(&actual),
                        ));
                    }
                }
            }
        }
    }

    unhandled.sort();
    unhandled.dedup();
    if !unhandled.is_empty() {
        // Not a failure: a newer generator may cover functions this harness has not learned yet.
        // It is printed so nobody believes those fixtures were checked.
        println!("NOT CHECKED (no Rust binding in this harness): {unhandled:?}");
    }
    println!(
        "contract: {checked} cases checked from {} files",
        files.len()
    );

    assert!(
        failures.is_empty(),
        "{} contract case(s) diverged from the Python:\n\n{}",
        failures.len(),
        failures.join("\n\n"),
    );
}

/// Compare two JSON documents the way JSON itself defines equality, with numbers compared by
/// value rather than by spelling.
///
/// This is *not* a loosening of the contract. JSON has exactly one number type: `0` and `0.0` are
/// two spellings of the same value, and no conforming reader can tell them apart. The divergence
/// it removes is an artefact of the two languages, not of the port — Python's `sum([])` is the
/// `int` `0`, so `round(0, 6)` is `0` and `json.dumps` writes `0`, while a Rust `f64` total writes
/// `0.0`. Requiring textual identity there would pin a CPython implementation detail (the `int`/
/// `float` split) that the wire format does not have, and the honest fix is not to make the Rust
/// emit integers for money — a cost that happens to land on a whole dollar is still a cost.
///
/// Everything else stays exactly as strict as it was: types must match, strings are compared byte
/// for byte, arrays must be the same length *in order*, objects must have the same key set, and a
/// number that differs by any amount at all still fails. Key *order* was never compared here —
/// `serde_json::Value` sorts object keys without the `preserve_order` feature — and the snapshot
/// module's own tests pin the emitted order on the string form instead.
///
/// The one spelling this forgives is `int` vs `float`. It is *not* a licence to compare numbers
/// approximately: see [`number_eq`] for the two ways a naive `as_f64` comparison would have gone
/// further than that and silently stopped biting.
fn json_eq(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => number_eq(a, b),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| json_eq(x, y))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).is_some_and(|other| json_eq(v, other)))
        }
        _ => actual == expected,
    }
}

/// Two JSON numbers, compared by value — and *only* by value.
///
/// Routing both sides through `as_f64` looks like the obvious way to forgive the `int`/`float`
/// spelling, and it quietly forgives two things that are not spellings at all:
///
/// 1. **Integers wider than 53 bits.** `Number::as_f64` answers `Some` for *every* JSON number,
///    widening `u64`/`i64` lossily on the way, so it never reaches a fallback arm — it would call
///    `9007199254740993` and `9007199254740992` equal. Integers are therefore compared as
///    integers, and only a comparison with an actual float falls through to `f64`.
/// 2. **The sign of zero.** `-0.0 == 0.0` is true in IEEE-754 and false on the wire: `json.dumps`
///    prints `-0.0`, and `math.copysign` reads it back, so the two are different documents that a
///    Python consumer can tell apart. This is exactly the divergence `metrics::round_half_even`
///    normalises away (Rust's `Sum for f64` folds from `-0.0`; Python's `sum()` folds from the
///    `int` `0`), and comparing through bare `f64` equality would have hidden that bug rather
///    than caught it — verified by deleting the normalisation and watching this harness stay
///    green. The sign bit stays part of the contract.
///
/// What remains forgiven is only the case the doc on [`json_eq`] argues for: an integer against a
/// float of the same value, `0` against `0.0`.
fn number_eq(a: &Number, b: &Number) -> bool {
    if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
        return a == b;
    }
    match (a.as_f64(), b.as_f64()) {
        (Some(a), Some(b)) => a == b && a.is_sign_negative() == b.is_sign_negative(),
        _ => a == b,
    }
}

/// Whether the harness could evaluate a case at all.
enum Outcome {
    /// The function ran; here is its answer as JSON.
    Value(Value),
    /// This harness has no binding for that function name.
    Unhandled,
    /// The binding exists but the fixture's input could not be read into it.
    Error(String),
}

/// Run one ported function against one fixture input.
///
/// The name aliases are generosity about spelling only. Every arm below runs the real function
/// from `swf-domain`; none of them reimplements anything.
fn apply(function: &str, input: &Value) -> Outcome {
    match function {
        "job_state" => match tasks_of(input) {
            Ok(tasks) => Outcome::Value(json!(rollup::job_state(&tasks))),
            Err(e) => Outcome::Error(e),
        },
        "group_jobs" => group_jobs(input),
        "collapsed_job" => match run_of(input) {
            Ok(run) => to_value(&rollup::collapsed_job(&run)),
            Err(e) => Outcome::Error(e),
        },
        "stage_progress" => match tasks_of(input) {
            Ok(tasks) => Outcome::Value(json!(rollup::stage_progress(&tasks))),
            Err(e) => Outcome::Error(e),
        },
        "summarize_checks" => {
            let rollup_arg = pick(input, &["rollup", "checks", "value"]);
            Outcome::Value(json!(rollup::summarize_checks(Some(rollup_arg))))
        }
        "parse_issues" => match pick(input, &["text", "value", "issues"]).as_str() {
            Some(text) => Outcome::Value(json!(rollup::parse_issues(text))),
            None => Outcome::Error("expected a string under `text`".into()),
        },
        "job_index" => Outcome::Value(json!(job_index(pick(
            input,
            &["map_index", "value", "idx"]
        )))),
        "age" => age(input),
        "summarize" | "metrics_summarize" => summarize(input),
        "table" | "metrics_table" => metrics_table(input),
        "snapshot_data" | "snapshot_json" => snapshot_json(input),
        "snapshot_text" => snapshot_text(input),
        "doctor_table" => doctor_table(input),
        "doctor_to_json" => doctor_to_json(input),
        "blueprint_loads" | "blueprint_from_toml" => blueprint_from_toml(input),
        _ => Outcome::Unhandled,
    }
}

fn group_jobs(input: &Value) -> Outcome {
    let dag_id = pick(input, &["dag_id"]).as_str().unwrap_or_default();
    let run_id = pick(input, &["run_id"]).as_str().unwrap_or_default();
    let tasks = match tasks_of(pick(input, &["tasks"])) {
        Ok(tasks) => tasks,
        Err(e) => return Outcome::Error(e),
    };
    let fan_out = pick(input, &["fan_out"])
        .as_array()
        .cloned()
        .unwrap_or_default();
    let fallback: Vec<String> = pick(input, &["fallback_issues", "fallback"])
        .as_array()
        .map(|items| items.iter().map(stringify).collect())
        .unwrap_or_default();
    to_value(&rollup::group_jobs(
        dag_id, run_id, &tasks, &fan_out, &fallback,
    ))
}

fn age(input: &Value) -> Outcome {
    let then = pick(input, &["value", "then", "at"]);
    let now = match parse_dt(pick(input, &["now"])) {
        Some(now) => now,
        None => return Outcome::Error("`now` must be a parseable timestamp".into()),
    };
    Outcome::Value(json!(rollup::age(parse_dt(then), now)))
}

fn summarize(input: &Value) -> Outcome {
    let runs = pick(input, &["runs", "value"]).clone();
    match serde_json::from_value::<Vec<metrics::RunMetrics>>(runs) {
        Ok(runs) => to_value(&metrics::summarize(&runs)),
        Err(e) => Outcome::Error(format!("`runs` is not a list of metrics records: {e}")),
    }
}

fn metrics_table(input: &Value) -> Outcome {
    let summary = pick(input, &["summary", "value", "metrics"]).clone();
    match serde_json::from_value::<metrics::MetricsSummary>(summary) {
        Ok(summary) => Outcome::Value(json!(metrics::table(&summary))),
        Err(e) => Outcome::Error(format!("`summary` is not a metrics summary: {e}")),
    }
}

fn snapshot_json(input: &Value) -> Outcome {
    match snapshot_of(input) {
        Ok(snap) => Outcome::Value(snapshot::snapshot_json(&snap, Utc::now())),
        Err(e) => Outcome::Error(e),
    }
}

fn snapshot_text(input: &Value) -> Outcome {
    match snapshot_of(input) {
        Ok(snap) => Outcome::Value(json!(snapshot::snapshot_text(&snap))),
        Err(e) => Outcome::Error(e),
    }
}

fn doctor_table(input: &Value) -> Outcome {
    match checks_of(input) {
        Ok(checks) => Outcome::Value(json!(doctor::table(&checks))),
        Err(e) => Outcome::Error(e),
    }
}

fn doctor_to_json(input: &Value) -> Outcome {
    match checks_of(input) {
        Ok(checks) => Outcome::Value(doctor::to_json_value(&checks)),
        Err(e) => Outcome::Error(e),
    }
}

/// A blueprint case's `expected` is either the parsed blueprint or `{"error": "<message>"}`, so
/// the message text stays under test alongside the happy path.
fn blueprint_from_toml(input: &Value) -> Outcome {
    let Some(text) = pick(input, &["text", "toml", "value"]).as_str() else {
        return Outcome::Error("expected the TOML source under `text`".into());
    };
    match blueprint::Blueprint::from_toml(text) {
        Ok(bp) => to_value(&bp),
        Err(e) => Outcome::Value(json!({"error": e.to_string()})),
    }
}

/// `job_index` in Rust takes an `i32`, while Python took anything and answered `"-"` for what it
/// could not read. The coercion lives here so the domain signature stays honest.
fn job_index(value: &Value) -> String {
    let index = match value {
        // Python's `bool` *is* an `int`, so `int(True)` is `1` and `int(False)` is `0`, and
        // `herd.job_index(True)` answers `"1"`. Airflow never sends a boolean `map_index`, but the
        // coercion belongs at this JSON boundary — exactly where Python's `int()` sits — rather
        // than in the typed `job_index(i32)` signature, which would have to grow a parameter it
        // has no honest use for.
        Value::Bool(b) => Some(f64::from(u8::from(*b))),
        Value::Number(n) => n.as_f64().map(|f| f.trunc()),
        Value::String(s) => s.trim().parse::<f64>().ok().map(f64::trunc),
        _ => None,
    };
    match index {
        Some(index)
            if index >= i64::from(i32::MIN) as f64 && index <= i64::from(i32::MAX) as f64 =>
        {
            rollup::job_index(index as i32)
        }
        Some(index) if index < 0.0 => rollup::job_index(-1),
        _ => "-".to_string(),
    }
}

/// The argument named by the first key the input actually carries, or the whole input when it is
/// not a keyword mapping at all.
fn pick<'a>(input: &'a Value, keys: &[&str]) -> &'a Value {
    if let Value::Object(fields) = input {
        for key in keys {
            if let Some(value) = fields.get(*key) {
                return value;
            }
        }
        // A single-key mapping whose key we do not know is still unambiguous.
        if fields.len() == 1 {
            if let Some((_, only)) = fields.iter().next() {
                return only;
            }
        }
    }
    input
}

/// Read a task list, accepting both a list of task objects and a bare list of state strings.
fn tasks_of(input: &Value) -> Result<Vec<TaskState>, String> {
    let value = pick(input, &["tasks", "states", "value"]);
    let Some(items) = value.as_array() else {
        return Err(format!("expected a list of tasks, got {}", kind(value)));
    };
    items
        .iter()
        .enumerate()
        .map(|(i, item)| match item {
            Value::Object(fields) => Ok(TaskState::new(
                fields
                    .get("task_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                fields
                    .get("map_index")
                    .and_then(Value::as_i64)
                    .map(|v| v as i32)
                    .unwrap_or(-1),
                fields.get("state").filter(|v| !v.is_null()).map(stringify),
            )),
            // A bare state string is the shorthand the Python's own table of examples uses.
            Value::String(state) => Ok(TaskState::new(format!("job.t{i}"), 0, Some(state.clone()))),
            Value::Null => Ok(TaskState::new(format!("job.t{i}"), 0, None)),
            other => Err(format!("task {i} is {}", kind(other))),
        })
        .collect()
}

fn run_of(input: &Value) -> Result<Run, String> {
    serde_json::from_value(pick(input, &["run", "value"]).clone())
        .map_err(|e| format!("not a run record: {e}"))
}

fn snapshot_of(input: &Value) -> Result<Snapshot, String> {
    serde_json::from_value(pick(input, &["snapshot", "value"]).clone())
        .map_err(|e| format!("not a snapshot: {e}"))
}

fn checks_of(input: &Value) -> Result<Vec<doctor::Check>, String> {
    serde_json::from_value(pick(input, &["checks", "value"]).clone())
        .map_err(|e| format!("not a list of checks: {e}"))
}

fn to_value<T: serde::Serialize>(value: &T) -> Outcome {
    match serde_json::to_value(value) {
        Ok(value) => Outcome::Value(value),
        Err(e) => Outcome::Error(format!("result did not serialise: {e}")),
    }
}

/// Python's `str()` for the scalars a fixture puts where a string belongs.
fn stringify(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Null => "None".to_string(),
        other => other.to_string(),
    }
}

/// Accept the ISO-8601 spellings Python's `fromisoformat` does, plus a `Z` suffix. A naive
/// timestamp is *stamped* UTC, not converted — that is what `herd._as_datetime` does.
fn parse_dt(value: &Value) -> Option<DateTime<Utc>> {
    let text = value.as_str()?.trim();
    if text.is_empty() {
        return None;
    }
    let text = text.replace('Z', "+00:00").replace(' ', "T");
    if let Ok(dt) = DateTime::parse_from_rfc3339(&text) {
        return Some(dt.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(&text, format) {
            return Some(Utc.from_utc_datetime(&naive));
        }
    }
    NaiveDate::parse_from_str(&text, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|naive| Utc.from_utc_datetime(&naive))
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "a list",
        Value::Object(_) => "an object",
    }
}

fn pretty(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserialisable>".to_string())
}

/// Where the fixtures live, unless `SWF_CONTRACT_FIXTURES` says otherwise.
fn expected_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("SWF_CONTRACT_FIXTURES") {
        return PathBuf::from(dir);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tests/fixtures/contract")
}

fn fixture_dir() -> Option<PathBuf> {
    let dir = expected_dir();
    dir.is_dir().then_some(dir)
}
