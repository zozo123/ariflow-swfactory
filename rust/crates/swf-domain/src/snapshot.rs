//! The snapshot renderers, which are the migration's equivalence target.
//!
//! `swf snapshot --json` has to be diffable against `swfactory herd --once --json`, so this module
//! is not "serialize the struct" — it is a deliberate re-statement of Python's `snapshot_data`,
//! key by key, in the order Python's dict literals emit them. The boundary it defends is that
//! diff: anything the Rust side adds, renames or reorders costs the migration its safety net, so
//! `Snapshot::health` (which Python has no concept of) is pointedly absent from the output.
//!
//! Timestamps use Python's `datetime.isoformat()` spelling — `+00:00`, never `Z`, and six
//! fractional digits or none at all. Chrono's RFC-3339 helpers emit `Z` and 0/3/6/9 digits, which
//! is a different string for the same instant and would show up as a diff on every line.

use chrono::{DateTime, Utc};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

use crate::metrics;
use crate::model::{Gate, PullRequest, Run, SandboxRef, Snapshot, SourceError};
use crate::rollup::{job_index, stage_progress};

// `iso`/`iso_opt` are the `Timestamp` type's own Python spelling, so they live beside it in
// `model`. They stay re-exported here because this is the module the spelling is *for*.
pub use crate::model::{iso, iso_opt};

/// The exact document `swfactory herd --once --json` prints, as a value.
///
/// `now` is part of the signature because every other renderer in the product takes a clock, but
/// this one deliberately does not consult it: a snapshot is a statement about the moment it was
/// collected, and mixing in the render time would make two prints of the same snapshot differ.
/// Freshness lives in `SourceHealth`, where it can be attributed to a source.
///
/// Note the returned `Value` does not itself carry key order — `serde_json`'s `Map` is sorted
/// unless the `preserve_order` feature is on. Use [`snapshot_json_string`] when the bytes matter.
pub fn snapshot_json(snap: &Snapshot, now: DateTime<Utc>) -> Value {
    let _ = now;
    serde_json::to_value(SnapshotDoc::from(snap)).unwrap_or(Value::Null)
}

/// The same document, serialised with Python's `json.dumps(..., indent=2)` layout and key order.
///
/// This is what the CLI prints and what a byte-diff against the Python compares.
pub fn snapshot_json_string(snap: &Snapshot, now: DateTime<Utc>) -> String {
    let _ = now;
    serde_json::to_string_pretty(&SnapshotDoc::from(snap)).unwrap_or_default()
}

/// The plain-text form `swfactory herd --once` prints: one screen, no colour, no trailing newline.
///
/// Note the job lines use [`job_index`], so an unmapped row prints `-` here while the JSON keeps
/// the raw `-1`. That asymmetry is intentional and pinned: humans read "no index yet", machines
/// read the integer they can compare.
pub fn snapshot_text(snap: &Snapshot) -> String {
    let jobs: usize = snap.job_count();
    let mut lines = vec![format!(
        "collected {}  runs {}  jobs {}  gates {}  prs {}  sandboxes {}",
        // Python interpolates `data["collected_at"]` straight into the f-string, so a snapshot
        // with no collection time prints the word `None` rather than a blank.
        iso_opt(snap.collected_at).unwrap_or_else(|| "None".to_string()),
        snap.runs.len(),
        jobs,
        snap.gates.len(),
        snap.prs.len(),
        snap.sandboxes.len(),
    )];
    for run in &snap.runs {
        lines.push(format!("run  {}/{} {}", run.dag_id, run.run_id, run.state));
        for job in &run.jobs {
            lines.push(format!(
                "  job {} {} {} {}",
                job_index(job.map_index),
                job.issue,
                stage_progress(&job.tasks),
                job.state,
            ));
        }
    }
    for gate in &snap.gates {
        lines.push(format!(
            "gate {}/{}[{}] {} {}",
            gate.dag_id,
            gate.run_id,
            gate.map_index,
            gate.short_name(),
            gate.subject,
        ));
    }
    for error in &snap.errors {
        lines.push(format!("error {}: {}", error.source, error.message));
    }
    lines.join("\n")
}

/// The top-level document. Field order is the contract; serde emits struct fields in declaration
/// order, which is why this is a struct and not a `serde_json::Map`.
#[derive(Serialize)]
struct SnapshotDoc<'a> {
    collected_at: Option<String>,
    runs: Vec<RunDoc<'a>>,
    gates: Vec<GateDoc<'a>>,
    prs: Vec<PrDoc<'a>>,
    sandboxes: Vec<SandboxDoc<'a>>,
    metrics: OrderedMetrics<'a>,
    errors: ErrorsDoc<'a>,
}

impl<'a> From<&'a Snapshot> for SnapshotDoc<'a> {
    fn from(snap: &'a Snapshot) -> Self {
        Self {
            collected_at: iso_opt(snap.collected_at),
            runs: snap.runs.iter().map(RunDoc::from).collect(),
            gates: snap.gates.iter().map(GateDoc::from).collect(),
            prs: snap.prs.iter().map(PrDoc::from).collect(),
            sandboxes: snap.sandboxes.iter().map(SandboxDoc::from).collect(),
            metrics: OrderedMetrics(&snap.metrics),
            errors: ErrorsDoc(&snap.errors),
        }
    }
}

#[derive(Serialize)]
struct RunDoc<'a> {
    dag_id: &'a str,
    run_id: &'a str,
    state: &'a str,
    start: Option<String>,
    end: Option<String>,
    issues: Vec<String>,
    jobs: Vec<JobDoc<'a>>,
}

impl<'a> From<&'a Run> for RunDoc<'a> {
    fn from(run: &'a Run) -> Self {
        Self {
            dag_id: &run.dag_id,
            run_id: &run.run_id,
            state: &run.state,
            start: iso_opt(run.start),
            end: iso_opt(run.end),
            issues: run.issues(),
            jobs: run.jobs.iter().map(JobDoc::from).collect(),
        }
    }
}

/// A job row as the snapshot reports it: the raw index, and the frontier computed here rather
/// than stored, because `stage_progress` is the thing under equivalence test.
#[derive(Serialize)]
struct JobDoc<'a> {
    map_index: i32,
    issue: &'a str,
    stage: String,
    state: &'a str,
}

impl<'a> From<&'a crate::model::JobRow> for JobDoc<'a> {
    fn from(job: &'a crate::model::JobRow) -> Self {
        Self {
            map_index: job.map_index,
            issue: &job.issue,
            stage: stage_progress(&job.tasks),
            state: &job.state,
        }
    }
}

#[derive(Serialize)]
struct GateDoc<'a> {
    dag_id: &'a str,
    run_id: &'a str,
    task_id: &'a str,
    gate: &'a str,
    map_index: i32,
    subject: &'a str,
    options: &'a [String],
    created_at: Option<String>,
}

impl<'a> From<&'a Gate> for GateDoc<'a> {
    fn from(gate: &'a Gate) -> Self {
        Self {
            dag_id: &gate.dag_id,
            run_id: &gate.run_id,
            task_id: &gate.task_id,
            gate: gate.short_name(),
            map_index: gate.map_index,
            subject: &gate.subject,
            options: &gate.options,
            created_at: iso_opt(gate.created_at),
        }
    }
}

#[derive(Serialize)]
struct PrDoc<'a> {
    number: i64,
    title: &'a str,
    url: &'a str,
    labels: &'a [String],
    state: &'a str,
    checks: &'a str,
    head: &'a str,
}

impl<'a> From<&'a PullRequest> for PrDoc<'a> {
    fn from(pr: &'a PullRequest) -> Self {
        Self {
            number: pr.number,
            title: &pr.title,
            url: &pr.url,
            labels: &pr.labels,
            state: &pr.state,
            checks: &pr.checks,
            head: &pr.head,
        }
    }
}

#[derive(Serialize)]
struct SandboxDoc<'a> {
    name: &'a str,
    status: &'a str,
    created_by: &'a str,
    created_at: Option<String>,
}

impl<'a> From<&'a SandboxRef> for SandboxDoc<'a> {
    fn from(sandbox: &'a SandboxRef) -> Self {
        Self {
            name: &sandbox.name,
            status: &sandbox.status,
            created_by: &sandbox.created_by,
            created_at: iso_opt(sandbox.created_at),
        }
    }
}

/// `errors` as a JSON object in collection order.
///
/// `Snapshot::errors` is a `Vec` precisely so this order survives; a sorted map would silently
/// reshuffle the keys as soon as two runs failed in one pass, and the diff would blame the wrong
/// thing.
struct ErrorsDoc<'a>(&'a [SourceError]);

impl Serialize for ErrorsDoc<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for error in self.0 {
            map.serialize_entry(&error.source, &error.message)?;
        }
        map.end()
    }
}

/// The metrics summary, re-emitted in `metrics::summarize`'s key order.
///
/// `Snapshot::metrics` is an untyped `Value` and this crate builds `serde_json` without
/// `preserve_order`, so the key order the summary was created with is already gone by the time it
/// gets here. Restating the canonical order is what keeps the byte diff clean; keys the summary
/// does not declare follow, sorted, so the output stays deterministic whatever a future source
/// puts there.
struct OrderedMetrics<'a>(&'a Value);

impl Serialize for OrderedMetrics<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Python emits `dict(snapshot.metrics or {})`, so this key is *always* an object: a pass
        // that read no metrics prints `{}`, never `null`. `Snapshot::metrics` is an untyped
        // `Value` and a source could hand back anything, so anything that is not an object is
        // rendered as the empty summary rather than leaking a non-dict into the contract.
        let empty = Value::Object(Map::new());
        let value = if self.0.is_object() { self.0 } else { &empty };
        serialize_ordered(value, metrics::SUMMARY_KEYS, serializer)
    }
}

/// Emit an object with `known` keys first, in that order, then the rest sorted.
fn serialize_ordered<S: Serializer>(
    value: &Value,
    known: &[&str],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Value::Object(fields) => {
            let mut map = serializer.serialize_map(Some(fields.len()))?;
            for key in known {
                if let Some(entry) = fields.get(*key) {
                    map.serialize_entry(key, &Nested(entry, nested_order(key)))?;
                }
            }
            for (key, entry) in fields {
                if !known.contains(&key.as_str()) {
                    map.serialize_entry(key, &Nested(entry, nested_order(key)))?;
                }
            }
            map.end()
        }
        Value::Array(items) => {
            let mut seq = serializer.serialize_seq(Some(items.len()))?;
            for item in items {
                seq.serialize_element(&Nested(item, &[]))?;
            }
            seq.end()
        }
        other => other.serialize(serializer),
    }
}

/// The one nested object whose key order Python pins: the severity histogram.
fn nested_order(key: &str) -> &'static [&'static str] {
    if key == "findings_by_severity" {
        metrics::SEVERITIES
    } else {
        &[]
    }
}

struct Nested<'a>(&'a Value, &'static [&'static str]);

impl Serialize for Nested<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_ordered(self.0, self.1, serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{JobRow, TaskState};
    use chrono::TimeZone;
    use serde_json::json;

    fn at(text: &str) -> DateTime<Utc> {
        match DateTime::parse_from_rfc3339(text) {
            Ok(dt) => dt.with_timezone(&Utc),
            Err(_) => Utc::now(),
        }
    }

    fn sample() -> Snapshot {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        let mut run = Run::new("factory", "manual__1", "running");
        run.start = Some(at("2026-09-03T11:00:00Z").fixed_offset());
        run.conf = match json!({"issues": ["42", "43"]}) {
            Value::Object(map) => map,
            _ => Default::default(),
        };
        let mut job = JobRow::new("factory", "manual__1", 1);
        job.issue = "43".to_string();
        job.state = "running".to_string();
        job.tasks = vec![
            TaskState::new("job.plan", 1, Some("success".into())),
            TaskState::new("job.approve_plan", 1, Some("awaiting_input".into())),
        ];
        run.jobs = vec![job];
        snap.runs = vec![run];
        snap.gates = vec![Gate::new(
            "factory",
            "manual__1",
            "job.approve_plan",
            1,
            "Approve plan for 43",
            "body",
            Some(at("2026-09-03T11:30:00Z")),
            vec!["Approve".into(), "Reject".into()],
        )];
        snap
    }

    #[test]
    fn iso_uses_pythons_offset_spelling_not_z() {
        assert_eq!(
            iso(at("2026-09-03T12:00:00Z").fixed_offset()),
            "2026-09-03T12:00:00+00:00"
        );
        let micro = match Utc.timestamp_opt(1_756_900_800, 123_456_000) {
            chrono::LocalResult::Single(dt) => dt,
            _ => return,
        };
        let micro = micro.fixed_offset();
        assert!(iso(micro).ends_with(".123456+00:00"), "{}", iso(micro));
        // Sub-microsecond precision cannot survive Python's datetime, so it is truncated.
        let nano = match Utc.timestamp_opt(1_756_900_800, 999) {
            chrono::LocalResult::Single(dt) => dt,
            _ => return,
        };
        let nano = nano.fixed_offset();
        assert!(iso(nano).ends_with(":00+00:00"), "{}", iso(nano));
    }

    #[test]
    fn the_top_level_keys_are_exactly_pythons_seven_in_order() {
        let text = snapshot_json_string(&sample(), Utc::now());
        let keys: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("  \""))
            .filter_map(|l| l.trim().split('"').nth(1))
            .collect();
        assert_eq!(
            keys,
            vec![
                "collected_at",
                "runs",
                "gates",
                "prs",
                "sandboxes",
                "metrics",
                "errors"
            ]
        );
    }

    #[test]
    fn health_is_never_emitted_because_python_has_no_such_key() {
        let mut snap = sample();
        snap.health.insert(
            "airflow".to_string(),
            crate::model::SourceHealth::fresh(Utc::now()),
        );
        let text = snapshot_json_string(&snap, Utc::now());
        assert!(!text.contains("health"), "{text}");
    }

    #[test]
    fn a_job_carries_the_computed_stage_and_the_raw_index() {
        let value = snapshot_json(&sample(), Utc::now());
        let job = &value["runs"][0]["jobs"][0];
        assert_eq!(job["map_index"], json!(1));
        assert_eq!(job["stage"], json!("approve_plan"));
        assert_eq!(job["issue"], json!("43"));
        assert_eq!(job["state"], json!("running"));
    }

    #[test]
    fn a_gate_carries_both_the_task_id_and_the_short_name() {
        let value = snapshot_json(&sample(), Utc::now());
        assert_eq!(value["gates"][0]["task_id"], json!("job.approve_plan"));
        assert_eq!(value["gates"][0]["gate"], json!("approve_plan"));
        assert_eq!(
            value["gates"][0]["created_at"],
            json!("2026-09-03T11:30:00+00:00")
        );
    }

    #[test]
    fn missing_timestamps_are_null_not_absent() {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        snap.runs = vec![Run::new("factory", "r1", "queued")];
        let value = snapshot_json(&snap, Utc::now());
        assert_eq!(value["runs"][0]["start"], Value::Null);
        assert_eq!(value["runs"][0]["end"], Value::Null);
    }

    #[test]
    fn an_empty_snapshot_still_carries_every_key() {
        let snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        let text = snapshot_json_string(&snap, Utc::now());
        assert!(text.contains("\"runs\": []"), "{text}");
        assert!(text.contains("\"errors\": {}"), "{text}");
        assert!(text.contains("\"metrics\": {}"), "{text}");
        assert!(!text.ends_with('\n'));
    }

    #[test]
    fn metrics_is_always_an_object_even_when_the_pass_read_none() {
        // `dict(snapshot.metrics or {})`: the key is present and object-shaped whatever the
        // source did, so a snapshot that never reached the metrics tree still diffs cleanly.
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        snap.metrics = Value::Null;
        assert_eq!(snapshot_json(&snap, Utc::now())["metrics"], json!({}));
        snap.metrics = json!("not a summary");
        assert_eq!(snapshot_json(&snap, Utc::now())["metrics"], json!({}));
    }

    #[test]
    fn a_snapshot_with_no_collection_time_prints_null_and_the_word_none() {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        snap.collected_at = None;
        assert_eq!(
            snapshot_json(&snap, Utc::now())["collected_at"],
            Value::Null
        );
        assert!(snapshot_text(&snap).starts_with("collected None  "));
    }

    #[test]
    fn a_reported_offset_reaches_the_document_unchanged() {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        snap.collected_at = crate::model::parse_timestamp(&json!("2026-09-03T14:00:00+02:00"));
        assert_eq!(
            snapshot_json(&snap, Utc::now())["collected_at"],
            json!("2026-09-03T14:00:00+02:00")
        );
    }

    #[test]
    fn errors_keep_collection_order_rather_than_sorting() {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        snap.set_error("islo", "islo not on PATH");
        snap.set_error("airflow:factory/r-b", "boom");
        snap.set_error("airflow:factory/r-a", "boom");
        let text = snapshot_json_string(&snap, Utc::now());
        let islo = text.find("\"islo\"");
        let b = text.find("\"airflow:factory/r-b\"");
        let a = text.find("\"airflow:factory/r-a\"");
        assert!(islo < b && b < a, "{text}");
    }

    #[test]
    fn metrics_are_re_emitted_in_the_summary_key_order() {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        snap.metrics = serde_json::to_value(metrics::summarize(&[])).unwrap_or(Value::Null);
        let text = snapshot_json_string(&snap, Utc::now());
        let positions: Vec<usize> = metrics::SUMMARY_KEYS
            .iter()
            .filter_map(|k| text.find(&format!("\"{k}\"")))
            .collect();
        assert_eq!(positions.len(), metrics::SUMMARY_KEYS.len(), "{text}");
        assert!(positions.windows(2).all(|w| w[0] < w[1]), "{text}");
        let blocker = text.find("\"blocker\"");
        let nit = text.find("\"nit\"");
        assert!(blocker < nit, "{text}");
    }

    #[test]
    fn snapshot_text_matches_the_python_line_shapes() {
        let text = snapshot_text(&sample());
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines[0],
            "collected 2026-09-03T12:00:00+00:00  runs 1  jobs 1  gates 1  prs 0  sandboxes 0"
        );
        assert_eq!(lines[1], "run  factory/manual__1 running");
        assert_eq!(lines[2], "  job 1 43 approve_plan running");
        assert_eq!(
            lines[3],
            "gate factory/manual__1[1] approve_plan Approve plan for 43"
        );
        assert!(!text.ends_with('\n'));
    }

    #[test]
    fn snapshot_text_prints_a_dash_where_the_json_prints_minus_one() {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        let mut run = Run::new("factory", "r1", "queued");
        run.jobs = vec![JobRow::new("factory", "r1", -1)];
        snap.runs = vec![run];
        snap.set_error("islo", "islo not on PATH");
        let text = snapshot_text(&snap);
        assert!(text.contains("\n  job - - - queued"), "{text}");
        assert!(text.ends_with("error islo: islo not on PATH"), "{text}");
        assert_eq!(
            snapshot_json(&snap, Utc::now())["runs"][0]["jobs"][0]["map_index"],
            json!(-1)
        );
    }

    #[test]
    fn a_run_with_no_jobs_contributes_only_its_own_line() {
        let mut snap = Snapshot::new(at("2026-09-03T12:00:00Z"));
        snap.runs = vec![Run::new("factory", "r1", "success")];
        assert_eq!(
            snapshot_text(&snap).lines().count(),
            2,
            "{}",
            snapshot_text(&snap)
        );
    }
}
