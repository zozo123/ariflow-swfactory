//! The shapes the whole product agrees on, and the small amount of truth derivable from them.
//!
//! These mirror the dataclasses in `src/swfactory/control.py` field for field, because the JSON
//! they serialise to is a compatibility surface: `swf snapshot --json` has to be diffable against
//! `swfactory herd --once --json` or the migration has no safety net. Where Python computed
//! something as a `@property`, it is a method here with the same name and the same edge cases —
//! including the ones that look like bugs, because a fixture pins them.
//!
//! The boundary this module defends is honesty about what is known. `JobRow::map_index == -1`
//! means "there is no job index yet", never "job number -1"; `SourceHealth` carries each source's
//! own freshness so one dead service cannot blank another's pane; and `Gate::ready` exists because
//! a gate that merely *has a HITL detail* is not yet a gate that can be answered.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Duration, Utc};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::ids::{GateId, JobId, RunRef, UNMAPPED};
use crate::states;

/// The issue a job row shows when the issue genuinely cannot be known yet. Printing `-` is the
/// honest answer; guessing an issue is not.
pub const NO_ISSUE: &str = "-";

/// The roll-up word a job carries before any task has a state.
pub const DEFAULT_JOB_STATE: &str = "queued";

/// The source key `collect()` files a whole-Airflow failure under.
pub const SOURCE_AIRFLOW: &str = "airflow";
/// The source key for the HITL gate listing, which is read even when run listing failed.
pub const SOURCE_GATES: &str = "gates";
/// The source key for anything read through `gh`.
pub const SOURCE_GITHUB: &str = "github";
/// The source key for anything read through `islo`.
pub const SOURCE_ISLO: &str = "islo";
/// The source key for the committed metrics tree.
pub const SOURCE_METRICS: &str = "metrics";

/// One Airflow task instance, reduced to the three fields any roll-up needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskState {
    /// e.g. `fan_out`, `job.build_and_test`, `job.approve_intent`.
    pub task_id: String,
    /// `-1` for an unmapped task, else the job index `fan_out` assigned.
    pub map_index: i32,
    /// Airflow's state, or `None` for a task instance it has created but not scheduled.
    pub state: Option<String>,
}

impl TaskState {
    /// Build a task state; `state` accepts anything string-like or `None`.
    pub fn new(task_id: impl Into<String>, map_index: i32, state: Option<String>) -> Self {
        Self {
            task_id: task_id.into(),
            map_index,
            state,
        }
    }

    /// The state as the roll-up sees it: `None` and `""` both read as `"none"`.
    pub fn state_or_none(&self) -> &str {
        states::or_none(self.state.as_deref())
    }
}

/// One row of the jobs table: a mapped job of a run, with the tasks it rolled up from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRow {
    /// The DAG this job's run belongs to.
    pub dag_id: String,
    /// Airflow's `dag_run_id`.
    pub run_id: String,
    /// The index `fan_out` assigned, or `-1`.
    pub map_index: i32,
    /// The issue this job answers, or [`NO_ISSUE`] when it cannot yet be known.
    #[serde(default = "no_issue")]
    pub issue: String,
    /// The rolled-up word: `queued`/`running`/`failed`/`skipped`/`success`, or — for a collapsed
    /// row — the DAG run's own state verbatim.
    #[serde(default = "default_job_state")]
    pub state: String,
    /// The task instances this row rolled up, in the order the API returned them.
    #[serde(default)]
    pub tasks: Vec<TaskState>,
}

fn no_issue() -> String {
    NO_ISSUE.to_string()
}

fn default_job_state() -> String {
    DEFAULT_JOB_STATE.to_string()
}

impl JobRow {
    /// Build a row with the Python defaults for `issue`, `state` and `tasks`.
    pub fn new(dag_id: impl Into<String>, run_id: impl Into<String>, map_index: i32) -> Self {
        Self {
            dag_id: dag_id.into(),
            run_id: run_id.into(),
            map_index,
            issue: no_issue(),
            state: default_job_state(),
            tasks: Vec::new(),
        }
    }

    /// True once `fan_out` has given this row a real job index.
    ///
    /// `map_index == -1` happens only before `fan_out` produced the job list, or for a run whose
    /// task instances were not fetched. There is no job index yet — there is no job numbered -1.
    pub fn mapped(&self) -> bool {
        self.map_index > UNMAPPED
    }

    /// The identity this row addresses, which is what a selection or a command argument keys on.
    pub fn id(&self) -> JobId {
        JobId::new(self.dag_id.clone(), self.run_id.clone(), self.map_index)
    }

    /// The run this row belongs to.
    pub fn run(&self) -> RunRef {
        RunRef::new(self.dag_id.clone(), self.run_id.clone())
    }
}

/// One DAG run, plus the job rows the collector attached to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    /// The DAG this run belongs to.
    pub dag_id: String,
    /// Airflow's `dag_run_id`.
    pub run_id: String,
    /// The DAG run state verbatim — `unknown` when Airflow reported nothing.
    pub state: String,
    /// When the run started, if it has.
    pub start: Option<DateTime<Utc>>,
    /// When the run finished, if it has.
    pub end: Option<DateTime<Utc>>,
    /// The `conf` the run was triggered with. This is where the issue list lives.
    #[serde(default)]
    pub conf: Map<String, Value>,
    /// Job rows, filled by the collector — one per mapped job, or a single collapsed row.
    #[serde(default)]
    pub jobs: Vec<JobRow>,
}

impl Run {
    /// Build a run with an empty `conf` and no jobs.
    pub fn new(
        dag_id: impl Into<String>,
        run_id: impl Into<String>,
        state: impl Into<String>,
    ) -> Self {
        Self {
            dag_id: dag_id.into(),
            run_id: run_id.into(),
            state: state.into(),
            start: None,
            end: None,
            conf: Map::new(),
            jobs: Vec::new(),
        }
    }

    /// True while Airflow is still working on this run. Drives whether the collector spends two
    /// API calls on real job rows or collapses the run to one line.
    pub fn active(&self) -> bool {
        states::run_is_active(&self.state)
    }

    /// The issue refs this run was triggered for, `issues` first and the legacy singular `issue`
    /// appended only if it adds something.
    ///
    /// The Python is `conf.get("issues") or []` followed by an `isinstance(list)` test, so a
    /// non-list `issues` yields nothing at all rather than a guess; and the singular is skipped
    /// only for `None` and `""`, which means a literal `0` or `false` *is* appended. Both of those
    /// are pinned by fixtures, so they are reproduced deliberately.
    pub fn issues(&self) -> Vec<String> {
        let mut refs: Vec<String> = match self.conf.get("issues") {
            Some(Value::Array(items)) => items.iter().map(py_str).collect(),
            _ => Vec::new(),
        };
        match self.conf.get("issue") {
            None | Some(Value::Null) => {}
            Some(Value::String(s)) if s.is_empty() => {}
            Some(one) => {
                let text = py_str(one);
                if !refs.contains(&text) {
                    refs.push(text);
                }
            }
        }
        refs
    }

    /// The run this is, addressable without a job index.
    pub fn id(&self) -> RunRef {
        RunRef::new(self.dag_id.clone(), self.run_id.clone())
    }
}

/// Render a JSON value the way Python's `str()` would, because `Run.issues` stringifies whatever
/// was in `conf` and the fixtures pin `42 -> "42"`, `true -> "True"`, `null -> "None"`.
///
/// Containers are the one divergence: Python would print a repr with single quotes, this prints
/// JSON. No real `conf` nests a list inside `issues`, and JSON is the more useful thing to see.
fn py_str(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Null => "None".to_string(),
        other => other.to_string(),
    }
}

/// One approval gate: a HITL detail Airflow is holding open for a human.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gate {
    /// The DAG this gate's run belongs to.
    pub dag_id: String,
    /// Airflow's `dag_run_id`.
    pub run_id: String,
    /// The full task id, always `job.approve_<stage>` for the shipped blueprints.
    pub task_id: String,
    /// The job index this gate belongs to.
    pub map_index: i32,
    /// The one-line question. Untrusted: render through [`crate::sanitize`].
    pub subject: String,
    /// The evidence the operator is being asked to judge. Untrusted.
    pub body: String,
    /// When the HITL detail was created.
    pub created_at: Option<DateTime<Utc>>,
    /// The answers Airflow will accept, e.g. `["Approve", "Reject"]`.
    #[serde(default)]
    pub options: Vec<String>,
    /// True only when the gate's task instance is genuinely parked in `awaiting_input` (or
    /// `deferred` on older builds).
    ///
    /// A HITL detail exists from the moment the operator task creates it — a beat *before* the
    /// task defers. Answering inside that window makes the scheduler fail the gate, which is a
    /// stress-test lesson that cost real debugging. So a gate that is not ready is shown but not
    /// answerable, and `gate_answer` refuses it unless forced.
    #[serde(default)]
    pub ready: bool,
    /// A short digest of the evidence the operator was shown.
    ///
    /// Approval re-reads the gate immediately before the PATCH and compares this. If the subject
    /// or body moved while the operator was reading, they are approving something they did not
    /// see, and that is a conflict (exit 6), not a write.
    #[serde(default)]
    pub revision: String,
}

impl Gate {
    /// Build a gate and stamp its revision from the evidence it carries.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dag_id: impl Into<String>,
        run_id: impl Into<String>,
        task_id: impl Into<String>,
        map_index: i32,
        subject: impl Into<String>,
        body: impl Into<String>,
        created_at: Option<DateTime<Utc>>,
        options: Vec<String>,
    ) -> Self {
        let subject = subject.into();
        let body = body.into();
        let revision = Self::revision_of(&subject, &body);
        Self {
            dag_id: dag_id.into(),
            run_id: run_id.into(),
            task_id: task_id.into(),
            map_index,
            subject,
            body,
            created_at,
            options,
            ready: false,
            revision,
        }
    }

    /// The digest [`Gate::revision`] carries.
    ///
    /// FNV-1a/64 rendered as 16 hex characters: deterministic across builds and platforms (which
    /// `DefaultHasher` explicitly is not), short enough to print in a confirmation prompt, and
    /// answering the only question asked of it — *is this the same text I was shown?* It is not a
    /// security primitive and nothing here treats it as one.
    pub fn revision_of(subject: &str, body: &str) -> String {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = OFFSET;
        // A separator, so ("ab", "c") and ("a", "bc") cannot collide by concatenation.
        for byte in subject
            .as_bytes()
            .iter()
            .chain(&[0x1f])
            .chain(body.as_bytes())
        {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        format!("{hash:016x}")
    }

    /// Recompute the revision from the current subject and body — used after a re-read.
    pub fn current_revision(&self) -> String {
        Self::revision_of(&self.subject, &self.body)
    }

    /// The bare stage name an operator recognises: `job.approve_plan` -> `approve_plan`.
    /// This is the `gate` key of the snapshot JSON.
    pub fn short_name(&self) -> &str {
        self.task_id.rsplit('.').next().unwrap_or(&self.task_id)
    }

    /// The job this gate belongs to.
    pub fn job(&self) -> JobId {
        JobId::new(self.dag_id.clone(), self.run_id.clone(), self.map_index)
    }

    /// The identity a command argument or a TUI selection keys on.
    pub fn id(&self) -> GateId {
        GateId::new(self.job(), self.task_id.clone())
    }

    /// True when Airflow would accept this answer at all. An empty `options` list means the gate
    /// did not declare any, and Python skips the check entirely.
    pub fn accepts(&self, choice: &str) -> bool {
        self.options.is_empty() || self.options.iter().any(|o| o == choice)
    }
}

/// A pull request the factory opened, as `gh` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    /// The PR number.
    pub number: i64,
    /// Untrusted: render through [`crate::sanitize`].
    pub title: String,
    /// The web URL.
    pub url: String,
    /// Label names, flattened from `gh`'s objects.
    #[serde(default)]
    pub labels: Vec<String>,
    /// `OPEN` / `MERGED` / `CLOSED`.
    pub state: String,
    /// Pre-rendered check summary, e.g. `3 pass / 0 fail / 1 pending`.
    pub checks: String,
    /// The head branch name — how a delivery is matched back to its run.
    pub head: String,
}

/// An issue the factory is working from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueRef {
    /// The issue number.
    pub number: i64,
    /// Untrusted: render through [`crate::sanitize`].
    pub title: String,
    /// The web URL.
    pub url: String,
    /// Label names.
    #[serde(default)]
    pub labels: Vec<String>,
}

/// A sandbox the factory may have created. Named `SandboxRef` because it is a reference to
/// something the factory does not own outright — removal is guarded twice over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxRef {
    /// The provider's sandbox name.
    pub name: String,
    /// The provider's status string; `deleted` means it is already gone.
    pub status: String,
    /// Who the provider says created it.
    pub created_by: String,
    /// When the provider says it was created.
    pub created_at: Option<DateTime<Utc>>,
}

impl SandboxRef {
    /// Build a sandbox reference.
    pub fn new(
        name: impl Into<String>,
        status: impl Into<String>,
        created_by: impl Into<String>,
        created_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            name: name.into(),
            status: status.into(),
            created_by: created_by.into(),
            created_at,
        }
    }

    /// True when the name matches `^swf-[a-z0-9][a-z0-9_-]*-[0-9a-f]{8}$`.
    ///
    /// This is half of the removal guard: `swf` refuses to delete anything that is not both
    /// factory-named *and* created by the configured owner. Someone's `prod-db` sandbox must
    /// survive a fat-fingered `swf sandboxes rm`, so the check is spelled out here rather than
    /// left to a regex crate the domain crate would otherwise not need.
    pub fn factory_named(&self) -> bool {
        is_factory_name(&self.name)
    }
}

/// The name policy behind [`SandboxRef::factory_named`], as a free function so the adapter's
/// removal guard can use it without building a value first.
pub fn is_factory_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("swf-") else {
        return false;
    };
    let bytes = rest.as_bytes();
    // slug (>= 1) + '-' + 8 hex. The trailing `$` anchor makes the split deterministic.
    if bytes.len() < 10 {
        return false;
    }
    let (slug, tail) = bytes.split_at(bytes.len() - 9);
    if tail[0] != b'-' || !tail[1..].iter().all(u8::is_ascii_hexdigit) {
        return false;
    }
    if tail[1..].iter().any(|b| b.is_ascii_uppercase()) {
        return false;
    }
    let Some((first, more)) = slug.split_first() else {
        return false;
    };
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return false;
    }
    more.iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-')
}

/// How fresh one source's data is, and why it might not be.
///
/// Rule 4 of the architecture: one dead service never blanks another's data. That only works if
/// freshness is per source, so every pane can say *when* it last succeeded and *what* went wrong,
/// instead of the whole screen going empty because `gh` is not installed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealth {
    /// When this source last answered successfully.
    pub fetched_at: Option<DateTime<Utc>>,
    /// Why the last read failed, if it did. Untrusted text may appear here.
    pub error: Option<String>,
    /// True while a read is outstanding — the difference between "stale" and "loading".
    pub in_flight: bool,
    /// True when a collection read stopped at its page bound instead of exhausting
    /// `total_entries`. Hiding jobs silently is the failure mode this replaces (rule 3), so this
    /// flag must reach the operator.
    pub truncated: bool,
}

impl SourceHealth {
    /// A source that has just answered cleanly.
    pub fn fresh(at: DateTime<Utc>) -> Self {
        Self {
            fetched_at: Some(at),
            error: None,
            in_flight: false,
            truncated: false,
        }
    }

    /// A source that failed. Any previous `fetched_at` is kept by the caller if it wants to show
    /// the last good time alongside the error.
    pub fn failed(at: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            fetched_at: Some(at),
            error: Some(error.into()),
            in_flight: false,
            truncated: false,
        }
    }

    /// True when the last read succeeded.
    pub fn ok(&self) -> bool {
        self.error.is_none()
    }

    /// How old this source's data is, or `None` if it has never answered.
    pub fn age(&self, now: DateTime<Utc>) -> Option<Duration> {
        self.fetched_at.map(|at| now - at)
    }

    /// True once the data is older than `threshold` — the TUI uses 3x the refresh interval. A
    /// source that has never answered is stale by definition.
    pub fn is_stale(&self, now: DateTime<Utc>, threshold: Duration) -> bool {
        match self.age(now) {
            Some(age) => age > threshold,
            None => true,
        }
    }
}

/// One source's failure, kept as an ordered entry rather than a map key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceError {
    /// The source key, e.g. `airflow`, `gates`, or `airflow:factory/r-running` for one run.
    pub source: String,
    /// `str(exception)` from the Python, and the adapter's error text here.
    pub message: String,
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.source, self.message)
    }
}

/// Everything one collection pass learned, and everything it failed to learn.
///
/// `errors` is a list and not a map on purpose: the snapshot JSON is a byte-compatibility target
/// and Python emits these keys in *insertion* order, which a sorted map would silently reorder as
/// soon as two runs fail in the same pass. The accessors below make it read like a map anyway.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// When the pass started.
    pub collected_at: DateTime<Utc>,
    /// Runs, newest first per DAG, in the order the API returned them.
    #[serde(default)]
    pub runs: Vec<Run>,
    /// Gates awaiting an answer.
    #[serde(default)]
    pub gates: Vec<Gate>,
    /// Pull requests the factory opened.
    #[serde(default)]
    pub prs: Vec<PullRequest>,
    /// Sandboxes the configured owner created.
    #[serde(default)]
    pub sandboxes: Vec<SandboxRef>,
    /// The committed metrics summary, verbatim. Left as a `Value` because its key order is
    /// `metrics::summarize`'s and belongs to that module, not this one.
    #[serde(default)]
    pub metrics: Value,
    /// Per-source failures, in the order the collector hit them.
    #[serde(
        default,
        serialize_with = "serialize_errors",
        deserialize_with = "deserialize_errors"
    )]
    pub errors: Vec<SourceError>,
    /// Per-source freshness. Not part of the Python JSON; the snapshot emitter must not print it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub health: BTreeMap<String, SourceHealth>,
}

impl Snapshot {
    /// An empty pass stamped with the collection time.
    pub fn new(collected_at: DateTime<Utc>) -> Self {
        Self {
            collected_at,
            runs: Vec::new(),
            gates: Vec::new(),
            prs: Vec::new(),
            sandboxes: Vec::new(),
            metrics: Value::Object(Map::new()),
            errors: Vec::new(),
            health: BTreeMap::new(),
        }
    }

    /// Every job row of every run, flattened in run order — exactly what the Runs table renders.
    pub fn jobs(&self) -> impl Iterator<Item = &JobRow> + '_ {
        self.runs.iter().flat_map(|run| run.jobs.iter())
    }

    /// How many job rows the pass produced. Cheaper than collecting `jobs()`.
    pub fn job_count(&self) -> usize {
        self.runs.iter().map(|run| run.jobs.len()).sum()
    }

    /// Record a source failure, keeping insertion order and last-write-wins per key.
    pub fn set_error(&mut self, source: impl Into<String>, message: impl Into<String>) {
        let source = source.into();
        let message = message.into();
        match self.errors.iter_mut().find(|e| e.source == source) {
            Some(existing) => existing.message = message,
            None => self.errors.push(SourceError { source, message }),
        }
    }

    /// The failure recorded for one source, if any.
    pub fn error(&self, source: &str) -> Option<&str> {
        self.errors
            .iter()
            .find(|e| e.source == source)
            .map(|e| e.message.as_str())
    }

    /// True when every source answered.
    pub fn healthy(&self) -> bool {
        self.errors.is_empty()
    }

    /// The run a job row belongs to, for a detail pane that was handed only an identity.
    pub fn run(&self, id: &RunRef) -> Option<&Run> {
        self.runs
            .iter()
            .find(|r| r.dag_id == id.dag_id && r.run_id == id.run_id)
    }

    /// The job row for one identity, or `None` if the pass no longer contains it.
    pub fn job(&self, id: &JobId) -> Option<&JobRow> {
        self.jobs()
            .find(|j| j.dag_id == id.dag_id && j.run_id == id.run_id && j.map_index == id.map_index)
    }

    /// The gate for one identity, or `None` if it has been answered since the pass.
    pub fn gate(&self, id: &GateId) -> Option<&Gate> {
        self.gates.iter().find(|g| {
            g.dag_id == id.job.dag_id
                && g.run_id == id.job.run_id
                && g.map_index == id.job.map_index
                && g.task_id == id.task_id
        })
    }
}

/// Emit `errors` as a JSON object, in collection order. `serde_json`'s writer preserves the order
/// entries are fed to it, which is the whole reason the field is a `Vec`.
fn serialize_errors<S: Serializer>(
    errors: &[SourceError],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(errors.len()))?;
    for error in errors {
        map.serialize_entry(&error.source, &error.message)?;
    }
    map.end()
}

/// Read `errors` back from a JSON object, keeping the order it was written in.
fn deserialize_errors<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<SourceError>, D::Error> {
    struct OrderedErrors;

    impl<'de> Visitor<'de> for OrderedErrors {
        type Value = Vec<SourceError>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a map of source name to failure text")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
            let mut out = Vec::with_capacity(access.size_hint().unwrap_or(0));
            while let Some((source, message)) = access.next_entry::<String, String>()? {
                out.push(SourceError { source, message });
            }
            Ok(out)
        }
    }

    deserializer.deserialize_map(OrderedErrors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn at(seconds: i64) -> DateTime<Utc> {
        match Utc.timestamp_opt(seconds, 0) {
            chrono::LocalResult::Single(dt) => dt,
            _ => Utc::now(),
        }
    }

    fn run_with_conf(conf: Value) -> Run {
        let mut run = Run::new("factory", "r1", "running");
        if let Value::Object(map) = conf {
            run.conf = map;
        }
        run
    }

    #[test]
    fn issues_puts_the_list_first_and_the_legacy_key_last() {
        let run = run_with_conf(json!({"issues": [42, "43"], "issue": 42}));
        assert_eq!(run.issues(), vec!["42", "43"]);
        assert_eq!(run_with_conf(json!({"issue": 7})).issues(), vec!["7"]);
        assert_eq!(run_with_conf(json!({})).issues(), Vec::<String>::new());
        assert_eq!(
            run_with_conf(json!({"issues": ["42"], "issue": "44"})).issues(),
            vec!["42", "44"]
        );
    }

    #[test]
    fn a_non_list_issues_key_yields_nothing_rather_than_a_guess() {
        assert_eq!(
            run_with_conf(json!({"issues": "42"})).issues(),
            Vec::<String>::new()
        );
        assert_eq!(
            run_with_conf(json!({"issues": {"a": 1}})).issues(),
            Vec::<String>::new()
        );
        assert_eq!(
            run_with_conf(json!({"issues": null})).issues(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn the_legacy_singular_is_skipped_only_for_null_and_empty_string() {
        assert_eq!(
            run_with_conf(json!({"issue": null})).issues(),
            Vec::<String>::new()
        );
        assert_eq!(
            run_with_conf(json!({"issue": ""})).issues(),
            Vec::<String>::new()
        );
        // Python's `not in (None, "")` lets these through; the fixtures pin it.
        assert_eq!(run_with_conf(json!({"issue": 0})).issues(), vec!["0"]);
        assert_eq!(
            run_with_conf(json!({"issue": false})).issues(),
            vec!["False"]
        );
        assert_eq!(run_with_conf(json!({"issue": true})).issues(), vec!["True"]);
    }

    #[test]
    fn only_queued_and_running_runs_are_active() {
        assert!(Run::new("f", "r", "running").active());
        assert!(Run::new("f", "r", "queued").active());
        assert!(!Run::new("f", "r", "success").active());
        assert!(!Run::new("f", "r", "unknown").active());
    }

    #[test]
    fn a_job_row_without_an_index_is_not_job_minus_one() {
        assert!(!JobRow::new("f", "r", -1).mapped());
        assert!(JobRow::new("f", "r", 0).mapped());
        assert!(JobRow::new("f", "r", 3).mapped());
        assert_eq!(JobRow::new("f", "r", -1).issue, NO_ISSUE);
        assert_eq!(JobRow::new("f", "r", -1).state, DEFAULT_JOB_STATE);
        assert_eq!(JobRow::new("f", "r", 2).id().to_string(), "f/r#2");
    }

    #[test]
    fn factory_names_are_matched_exactly_as_the_regex_does() {
        for good in [
            "swf-42-abcd1234",
            "swf-a-00000000",
            "swf-my_slug-name-deadbeef",
        ] {
            assert!(is_factory_name(good), "{good} should be factory-named");
        }
        for bad in [
            "SWF-42-ABCD1234",
            "swf-42-ABCD1234",
            "prod-db",
            "swf--abcd1234",
            "swf-_x-abcd1234",
            "swf-42-abcd123",
            "swf-42-abcd12345",
            "swf-42-abcdefgh",
            "swf-",
            "swf-42-abcd1234\n",
            "xswf-42-abcd1234",
        ] {
            assert!(!is_factory_name(bad), "{bad} must not be factory-named");
        }
        assert!(SandboxRef::new("swf-42-abcd1234", "running", "me", None).factory_named());
    }

    #[test]
    fn factory_name_matching_survives_non_ascii() {
        assert!(!is_factory_name("swf-日本語-abcd1234"));
        assert!(!is_factory_name("swf-é-abcd1234"));
    }

    #[test]
    fn a_gate_revision_changes_when_the_evidence_does() {
        let a = Gate::revision_of("Approve plan?", "body");
        assert_eq!(a.len(), 16);
        assert_eq!(a, Gate::revision_of("Approve plan?", "body"));
        assert_ne!(a, Gate::revision_of("Approve plan?", "body "));
        assert_ne!(a, Gate::revision_of("Approve plan", "?body"));
        assert_eq!(Gate::revision_of("", ""), Gate::revision_of("", ""));
    }

    #[test]
    fn a_gate_knows_its_identity_and_its_answers() {
        let gate = Gate::new(
            "factory",
            "manual__2026-09-06T12:00:00+00:00",
            "job.approve_plan",
            2,
            "Approve the plan?",
            "3 steps",
            None,
            vec!["Approve".into(), "Reject".into()],
        );
        assert_eq!(gate.short_name(), "approve_plan");
        assert_eq!(
            gate.id().to_string(),
            "factory/manual__2026-09-06T12:00:00+00:00#2:job.approve_plan"
        );
        assert_eq!(gate.job().map_index, 2);
        assert!(
            !gate.ready,
            "a fresh gate is never answerable until the task parks"
        );
        assert_eq!(gate.revision, gate.current_revision());
        assert!(gate.accepts("Approve"));
        assert!(!gate.accepts("Maybe"));
    }

    #[test]
    fn a_gate_with_no_declared_options_accepts_anything() {
        let gate = Gate::new("f", "r", "job.approve_intent", 0, "s", "b", None, vec![]);
        assert!(gate.accepts("Approve"));
        assert!(gate.accepts("whatever"));
    }

    #[test]
    fn source_health_distinguishes_loading_from_stale_from_broken() {
        let now = at(3_000);
        let fresh = SourceHealth::fresh(now);
        assert!(fresh.ok());
        assert!(!fresh.is_stale(now, Duration::seconds(30)));
        assert!(fresh.is_stale(at(3_100), Duration::seconds(30)));

        let broken = SourceHealth::failed(now, "gh: not found");
        assert!(!broken.ok());
        assert_eq!(broken.error.as_deref(), Some("gh: not found"));

        let never = SourceHealth::default();
        assert!(never.age(now).is_none());
        assert!(
            never.is_stale(now, Duration::seconds(30)),
            "never-fetched reads as stale"
        );
        assert!(!never.in_flight);
        assert!(!never.truncated);
    }

    #[test]
    fn snapshot_jobs_flatten_in_run_order() {
        let mut snap = Snapshot::new(at(0));
        let mut first = Run::new("factory", "r1", "running");
        first.jobs = vec![
            JobRow::new("factory", "r1", 0),
            JobRow::new("factory", "r1", 1),
        ];
        let mut second = Run::new("factory", "r2", "success");
        second.jobs = vec![JobRow::new("factory", "r2", -1)];
        snap.runs = vec![first, second];

        let indexes: Vec<i32> = snap.jobs().map(|j| j.map_index).collect();
        assert_eq!(indexes, vec![0, 1, -1]);
        assert_eq!(snap.job_count(), 3);
        assert_eq!(
            snap.job(&JobId::new("factory", "r2", -1))
                .map(|j| j.run_id.as_str()),
            Some("r2")
        );
        assert!(snap.job(&JobId::new("factory", "r9", 0)).is_none());
        assert_eq!(
            snap.run(&RunRef::new("factory", "r2"))
                .map(|r| r.state.as_str()),
            Some("success")
        );
    }

    #[test]
    fn errors_keep_collection_order_through_json() {
        let mut snap = Snapshot::new(at(0));
        snap.set_error("airflow", "boom");
        snap.set_error("airflow:factory/r-running", "run boom");
        snap.set_error("airflow:factory/r-done", "run boom 2");
        snap.set_error("gates", "gate boom");
        assert!(!snap.healthy());
        assert_eq!(snap.error("gates"), Some("gate boom"));
        assert_eq!(snap.error("github"), None);

        // Writing to a string preserves insertion order; a sorted map would not.
        let text = serde_json::to_string(&snap).expect("serialize");
        let errors_at = text.find("\"errors\"").expect("errors key");
        let tail = &text[errors_at..];
        let running = tail.find("r-running").expect("first run key");
        let done = tail.find("r-done").expect("second run key");
        assert!(running < done, "insertion order lost: {tail}");

        let back: Snapshot = serde_json::from_str(&text).expect("round trip");
        let keys: Vec<&str> = back.errors.iter().map(|e| e.source.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "airflow",
                "airflow:factory/r-running",
                "airflow:factory/r-done",
                "gates"
            ]
        );
    }

    #[test]
    fn set_error_is_last_write_wins_without_moving_the_key() {
        let mut snap = Snapshot::new(at(0));
        snap.set_error("airflow", "first");
        snap.set_error("gates", "gate");
        snap.set_error("airflow", "second");
        assert_eq!(snap.errors.len(), 2);
        assert_eq!(snap.errors[0].source, "airflow");
        assert_eq!(snap.errors[0].message, "second");
    }

    #[test]
    fn the_dataclasses_round_trip_through_serde_with_python_key_names() {
        let mut run = Run::new("factory", "r1", "running");
        run.start = Some(at(1_000));
        run.conf = match json!({"issues": ["42"]}) {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        run.jobs = vec![JobRow::new("factory", "r1", 0)];
        let text = serde_json::to_string(&run).expect("serialize");
        for key in ["dag_id", "run_id", "state", "start", "end", "conf", "jobs"] {
            assert!(
                text.contains(&format!("\"{key}\"")),
                "missing key {key} in {text}"
            );
        }
        assert_eq!(serde_json::from_str::<Run>(&text).expect("round trip"), run);
    }

    #[test]
    fn task_state_normalises_missing_states_for_the_rollup() {
        assert_eq!(TaskState::new("job.setup", 0, None).state_or_none(), "none");
        assert_eq!(
            TaskState::new("job.setup", 0, Some(String::new())).state_or_none(),
            "none"
        );
        assert_eq!(
            TaskState::new("job.setup", 0, Some("running".into())).state_or_none(),
            "running"
        );
    }
}
