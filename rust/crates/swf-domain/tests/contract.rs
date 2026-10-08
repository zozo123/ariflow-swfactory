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
//! naming the fixture, the case and both values. A missing fixture directory and a fixture this
//! harness cannot dispatch fail too, exactly as `tests/test_contract_fixtures.py` does: a silent
//! skip is how the two halves would drift apart while the suite stayed green.

use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Number, Value};

use swf_domain::model::{Snapshot, TaskState};
use swf_domain::{metrics, policy, rollup, snapshot};

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
    let dir = fixture_dir();
    let mut files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
            .collect(),
        Err(e) => panic!(
            "no contract fixtures at {}: {e}. Set SWF_CONTRACT_FIXTURES to point somewhere else.",
            dir.display()
        ),
    };
    files.sort();
    assert!(
        !files.is_empty(),
        "no *.json contract fixtures under {}",
        dir.display()
    );

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
    assert!(
        unhandled.is_empty(),
        "fixtures for functions this harness cannot call: {unhandled:?}"
    );
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

/// The comparison is the one place a contract harness can quietly stop being a contract, so its
/// exact tolerance is pinned rather than left to the reader of `number_eq`.
#[test]
fn the_number_comparison_forgives_only_the_int_float_spelling() {
    let eq = |a: &str, b: &str| {
        json_eq(
            &serde_json::from_str(a).expect(a),
            &serde_json::from_str(b).expect(b),
        )
    };

    // The one sanctioned tolerance: `round(sum([]), 6)` is the `int` `0` in Python and an `f64`
    // in Rust, and JSON cannot tell the two apart.
    assert!(eq("0", "0.0"));
    assert!(eq("0.0", "0"));
    assert!(eq("1", "1.0"));

    // Not tolerances. Float error, the sign of zero, and integers past 2^53 all still fail.
    assert!(!eq("0.30000000000000004", "0.3"));
    assert!(!eq("-0.0", "0.0"));
    assert!(!eq("-0.0", "0"));
    assert!(!eq("9007199254740993", "9007199254740992"));
    assert!(!eq("18446744073709551615", "18446744073709551614"));
    assert!(!eq("-1", "18446744073709551615"));

    // And nothing about the structural strictness moved.
    assert!(!eq(r#"{"a":1}"#, r#"{"a":1,"b":2}"#));
    assert!(!eq("[1,2]", "[2,1]"));
    assert!(!eq(r#""0""#, "0"));
    assert!(!eq("null", "0"));
    assert!(!eq("false", "0"));
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
/// Every arm below runs the real function from `swf-domain`; none of them reimplements anything.
fn apply(function: &str, input: &Value) -> Outcome {
    match function {
        "job_state" => match tasks_of(input) {
            Ok(tasks) => Outcome::Value(json!(rollup::job_state(&tasks))),
            Err(e) => Outcome::Error(e),
        },
        "group_jobs" => group_jobs(input),
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
        "summarize" => summarize(input),
        "snapshot_json" => snapshot_json(input),
        "policy_digest" => match policy::policy_digest(input) {
            Ok(digest) => Outcome::Value(json!(digest)),
            Err(error) => Outcome::Error(format!("policy did not canonicalize: {error}")),
        },
        "is_cell_id" => match input.get("value").and_then(Value::as_str) {
            Some(value) => Outcome::Value(json!(swf_domain::cell::is_cell_id(value))),
            None => Outcome::Error("is_cell_id case has no string `value`".to_string()),
        },
        "credential_lease_binding_digest" => {
            match serde_json::from_value::<swf_domain::CredentialLeaseBinding>(input.clone()) {
                Ok(binding) => match binding.digest() {
                    Ok(value) => Outcome::Value(json!(value)),
                    Err(error) => {
                        Outcome::Error(format!("credential lease binding invalid: {error}"))
                    }
                },
                Err(error) => Outcome::Error(format!("credential lease fixture invalid: {error}")),
            }
        }
        "phase_control" => phase_control(input),
        _ => Outcome::Unhandled,
    }
}

fn phase_control(input: &Value) -> Outcome {
    use swf_domain::phase_control::{assess, FactoryPhase, PhaseObservation};

    let observation =
        match serde_json::from_value::<PhaseObservation>(pick(input, &["observation"]).clone()) {
            Ok(observation) => observation,
            Err(error) => return Outcome::Error(format!("invalid phase observation: {error}")),
        };
    let previous_phase = match input.get("previous_phase") {
        None | Some(Value::Null) => None,
        Some(value) => match serde_json::from_value::<FactoryPhase>(value.clone()) {
            Ok(phase) => Some(phase),
            Err(error) => return Outcome::Error(format!("invalid previous phase: {error}")),
        },
    };
    match assess(&observation, previous_phase) {
        Ok(assessment) => {
            let recommendation = &assessment.recommendation;
            Outcome::Value(json!({
                "raw_phase": assessment.raw_phase,
                "phase": assessment.phase,
                "mode": recommendation.mode,
                "spawn": recommendation.spawn,
                "trajectory": recommendation.trajectory,
                "context": recommendation.context,
                "candidates": recommendation.candidates,
                "queue": recommendation.queue,
                "verification": recommendation.verification,
                "attention": recommendation.attention,
                "allow_new_implementation_lanes": recommendation.allow_new_implementation_lanes,
            }))
        }
        Err(error) => Outcome::Error(format!("phase assessment failed: {error}")),
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

fn summarize(input: &Value) -> Outcome {
    let runs = pick(input, &["runs", "value"]).clone();
    match serde_json::from_value::<Vec<metrics::RunMetrics>>(runs) {
        Ok(runs) => to_value(&metrics::summarize(&runs)),
        Err(e) => Outcome::Error(format!("`runs` is not a list of metrics records: {e}")),
    }
}

fn snapshot_json(input: &Value) -> Outcome {
    match snapshot_of(input) {
        Ok(snap) => Outcome::Value(snapshot::snapshot_json(&snap, Utc::now())),
        Err(e) => Outcome::Error(e),
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

fn snapshot_of(input: &Value) -> Result<Snapshot, String> {
    serde_json::from_value(pick(input, &["snapshot", "value"]).clone())
        .map_err(|e| format!("not a snapshot: {e}"))
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
fn fixture_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("SWF_CONTRACT_FIXTURES") {
        return PathBuf::from(dir);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tests/fixtures/contract")
}
