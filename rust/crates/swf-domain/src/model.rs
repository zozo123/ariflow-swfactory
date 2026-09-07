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

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
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

/// A timestamp as Python carries one: an instant **plus the UTC offset it arrived with**.
///
/// `herd._as_datetime` never converts — it only *stamps* a naive value with UTC — and `_iso` then
/// prints `dt.isoformat()`, which spells the datetime's own offset. So a source that reported
/// `2026-09-03T14:00:00+02:00` must come back out of the snapshot as `+02:00`; normalising it to
/// UTC is the same instant and a different document, and the byte-diff against
/// `swfactory herd --once --json` is the whole point of this module. `DateTime<Utc>` structurally
/// cannot remember that offset, so every timestamp Python round-trips through `_iso` is a
/// `DateTime<FixedOffset>` here.
pub type Timestamp = DateTime<FixedOffset>;

/// Read a timestamp exactly as `herd._as_datetime` does, including its tolerance.
///
/// Python's is `datetime.fromisoformat(value.strip().replace("Z", "+00:00"))` inside a
/// `try/except ValueError` that answers `None` — so anything unparseable, `null`, a number, or an
/// empty string is "not known", never an error. Mirroring that here is deliberate: the snapshot is
/// a report about services that may be broken, and one unreadable `created_at` must not refuse the
/// whole document (rule 4 — one dead source never blanks another's pane).
pub fn parse_timestamp(value: &Value) -> Option<Timestamp> {
    let raw = value.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    // `replace("Z", "+00:00")` replaces *every* `Z`, not just a trailing one — Python's does too.
    let text = as_fromisoformat_spells_it(&raw.replace('Z', "+00:00"))?;
    if let Ok(dt) = DateTime::parse_from_rfc3339(&text) {
        return no_leap_second(dt);
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f%:z", "%Y-%m-%dT%H:%M:%S%.f%z"] {
        if let Ok(dt) = DateTime::parse_from_str(&text, format) {
            return no_leap_second(dt);
        }
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(&text, format) {
            return no_leap_second(stamped_utc(naive));
        }
    }
    NaiveDate::parse_from_str(&text, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(stamped_utc)
}

/// `fromisoformat` has no leap second: `...T12:00:60` is a `ValueError`, so it is `None` here.
///
/// Chrono spells a leap second as a nanosecond field at or past 1e9, which [`iso`] would render as
/// a *seven*-digit fraction — a string that is neither valid ISO-8601 nor anything Python can
/// produce. Refusing it at the parse boundary is where Python refuses it.
fn no_leap_second(at: Timestamp) -> Option<Timestamp> {
    (at.nanosecond() < 1_000_000_000).then_some(at)
}

/// Reconcile the spellings `datetime.fromisoformat` accepts with the ones chrono does.
///
/// The two are close but not the same, and each direction of the difference is a divergence in the
/// snapshot document — a `start` that Python reads and this does not comes out `null`, and one this
/// reads and Python does not comes out as a timestamp. So the three cheap cases are aligned here:
///
/// * **Any single separator character.** `fromisoformat` splits the date from the time at index 10
///   whatever the character is — `T`, a space, `t`, even `_`. Chrono only knows `T`.
/// * **An hour-only offset.** `+02` is ISO-8601 and Python takes it; chrono wants `+02:00`.
/// * **Two spellings chrono takes and Python refuses**: a leading sign (chrono's expanded year,
///   `ValueError` in Python) and a lower-case `z` (Python only rewrites the upper-case one, and
///   `fromisoformat` then raises on what is left).
///
/// A *digit* at index 10 is left alone rather than rewritten. Python does treat it as a separator,
/// but rewriting it would hand chrono `2026-09-03T2:00:00`, whose single-digit hour chrono accepts
/// and `fromisoformat` rejects — and inventing a timestamp where the Python printed `null` is the
/// one direction of this difference that corrupts the document rather than thinning it.
///
/// What is deliberately *not* reconciled, because no source the collector reads emits it: the
/// basic format (`20260903T120000`), ISO week dates (`2026-W36-4`), sub-minute UTC offsets
/// (`+00:00:30`), and a fraction on a seconds-less time (`12:00.123`). Python accepts all of them
/// and this answers `None` — the safe direction: "not known", never a value Python did not have.
/// A 1463-input differential run against CPython 3.12 leaves those as the only differences, and
/// every one of them is `None` here rather than a fabricated timestamp.
fn as_fromisoformat_spells_it(text: &str) -> Option<String> {
    if text.starts_with('+') || text.starts_with('-') || text.ends_with('z') {
        return None;
    }
    let mut out = text.to_string();
    match out.as_bytes().get(10).copied() {
        // A digit is left as-is (see above); a non-ASCII byte there may not even be a character
        // boundary, and no source spells a separator that way. Either is `None`, not a panic.
        Some(b) if b.is_ascii_digit() || !b.is_ascii() => return None,
        Some(_) => out.replace_range(10..11, "T"),
        None => {}
    }
    // The offset sign, if any, is the first `+`/`-` *after* the separator — index 10 itself can be
    // a `-`, which `fromisoformat` reads as the separator and not as a sign.
    if let Some(sign) = out
        .bytes()
        .enumerate()
        .find(|(i, b)| *i > 10 && (*b == b'+' || *b == b'-'))
        .map(|(i, _)| i)
    {
        let tail = &out[sign + 1..];
        if tail.len() == 2 && tail.bytes().all(|b| b.is_ascii_digit()) {
            out.push_str(":00");
        }
    }
    Some(out)
}

/// `_as_datetime`'s `dt.replace(tzinfo=UTC)`: a naive stamp is *labelled* UTC, never shifted.
fn stamped_utc(naive: NaiveDateTime) -> Timestamp {
    Utc.from_utc_datetime(&naive).fixed_offset()
}

/// Render an instant the way Python's `datetime.isoformat()` does.
///
/// Seconds precision when there are no microseconds, exactly six fractional digits when there
/// are, and the datetime's own offset — `+00:00`, never `Z`. Chrono's RFC-3339 helpers emit `Z`
/// and 0/3/6/9 fractional digits, which is a different string for the same instant and would show
/// up as a diff on every line of the snapshot.
pub fn iso(at: Timestamp) -> String {
    // Chrono reports a leap second as a nanosecond field at or past 1e9, which would format as a
    // seven-digit fraction. [`parse_timestamp`] already refuses one, but a `Timestamp` assigned
    // field-by-field elsewhere has not been through it, and this must never emit a string Python
    // could not have written.
    let micros = at.nanosecond().min(999_999_999) / 1_000;
    if micros == 0 {
        at.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
    } else {
        format!(
            "{}.{micros:06}{}",
            at.format("%Y-%m-%dT%H:%M:%S"),
            at.format("%:z")
        )
    }
}

/// The same, for a timestamp a source may not have reported. `None` becomes JSON `null`, which is
/// what `_iso` returns for one.
pub fn iso_opt(at: Option<Timestamp>) -> Option<String> {
    at.map(iso)
}

/// Deserialise `Option<Timestamp>` through [`parse_timestamp`], so `null`, an absent key and an
/// unreadable string all read as "not known" instead of failing the whole snapshot.
fn de_timestamp_opt<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Timestamp>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    Ok(parse_timestamp(&value))
}

/// Serialise `Option<Timestamp>` in Python's `isoformat()` spelling, so the model's own JSON and
/// the snapshot document agree character for character.
fn ser_timestamp_opt<S: Serializer>(
    at: &Option<Timestamp>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match at {
        Some(at) => serializer.serialize_str(&iso(*at)),
        None => serializer.serialize_none(),
    }
}

/// Read a collection field with Python's `... or []` tolerance.
///
/// Every list and dict the collector reads is spelled `list(g.options or [])`,
/// `dict(snapshot.errors or {})`, `self.conf.get("issues") or []` — a JSON `null` where a
/// collection belongs means *empty*, and so does an absent key (the dataclasses all use
/// `field(default_factory=list)`). Airflow and `gh` both emit `null` for "none of these", so
/// rejecting it would fail on real payloads, not just on fixtures.
fn de_null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

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
    #[serde(default, deserialize_with = "de_null_as_default")]
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
    #[serde(
        default,
        serialize_with = "ser_timestamp_opt",
        deserialize_with = "de_timestamp_opt"
    )]
    pub start: Option<Timestamp>,
    /// When the run finished, if it has.
    #[serde(
        default,
        serialize_with = "ser_timestamp_opt",
        deserialize_with = "de_timestamp_opt"
    )]
    pub end: Option<Timestamp>,
    /// The `conf` the run was triggered with. This is where the issue list lives.
    #[serde(default, deserialize_with = "de_null_as_default")]
    pub conf: Map<String, Value>,
    /// Job rows, filled by the collector — one per mapped job, or a single collapsed row.
    #[serde(default, deserialize_with = "de_null_as_default")]
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
    #[serde(
        default,
        serialize_with = "ser_timestamp_opt",
        deserialize_with = "de_timestamp_opt"
    )]
    pub created_at: Option<Timestamp>,
    /// The answers Airflow will accept, e.g. `["Approve", "Reject"]`.
    ///
    /// `null` reads as "none declared", mirroring `list(g.options or [])`; an empty list means the
    /// gate accepts anything (see [`Gate::accepts`]).
    #[serde(default, deserialize_with = "de_null_as_default")]
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
        // The constructor keeps taking `DateTime<Utc>`: every adapter has already normalised to
        // UTC by the time it builds a gate, and a caller that genuinely has an offset to preserve
        // assigns the field directly.
        let created_at = created_at.map(|at| at.fixed_offset());
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

    /// How long this gate has existed, according to the clock that stamped it.
    ///
    /// This is the *only* measure of a gate's age that does not depend on when this process
    /// happened to look. [`Gate::created_at`] is written by the server when the operator task
    /// creates the HITL detail — a beat before that task defers — so `now - created_at` bounds
    /// the window in which the scheduler is still reconciling the worker process that parked it,
    /// no matter how many processes have polled, whether a dry run went first, or how long the
    /// poll interval is. A wall clock started at *our* first sighting measures none of that.
    ///
    /// `None` means the server did not stamp one, or the stamp did not parse: the caller must
    /// have another rule for that gate rather than treat "unknown" as "old". A `created_at` in
    /// the future (clock skew between the stamping server and `now`) reads as zero, which is the
    /// safe direction to be wrong in — a gate that looks brand new is waited out, not written to.
    pub fn age_at(&self, now: Timestamp) -> Option<std::time::Duration> {
        let created = self.created_at?;
        // `to_std` refuses a negative span, which is exactly the `created_at` in the future case.
        Some(
            now.signed_duration_since(created)
                .to_std()
                .unwrap_or(std::time::Duration::ZERO),
        )
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
    /// Label names, flattened from `gh`'s objects. `null` reads as none, per `list(p.labels or [])`.
    #[serde(default, deserialize_with = "de_null_as_default")]
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
    /// Label names. `null` reads as none.
    #[serde(default, deserialize_with = "de_null_as_default")]
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
    #[serde(
        default,
        serialize_with = "ser_timestamp_opt",
        deserialize_with = "de_timestamp_opt"
    )]
    pub created_at: Option<Timestamp>,
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
            // As in `Gate::new`: `islo` reports UTC, so the constructor stays UTC-shaped.
            created_at: created_at.map(|at| at.fixed_offset()),
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
    /// When the pass started, if it is known.
    ///
    /// Optional because `snapshot_data` reads it as `_iso(getattr(snapshot, "collected_at", None))`
    /// and prints `null` for a snapshot that has none — a shape a fixture pins. A pass built by
    /// [`Snapshot::new`] always has one.
    #[serde(
        default,
        serialize_with = "ser_timestamp_opt",
        deserialize_with = "de_timestamp_opt"
    )]
    pub collected_at: Option<Timestamp>,
    /// Runs, newest first per DAG, in the order the API returned them.
    #[serde(default, deserialize_with = "de_null_as_default")]
    pub runs: Vec<Run>,
    /// Gates awaiting an answer.
    #[serde(default, deserialize_with = "de_null_as_default")]
    pub gates: Vec<Gate>,
    /// Pull requests the factory opened.
    #[serde(default, deserialize_with = "de_null_as_default")]
    pub prs: Vec<PullRequest>,
    /// Sandboxes the configured owner created.
    #[serde(default, deserialize_with = "de_null_as_default")]
    pub sandboxes: Vec<SandboxRef>,
    /// The committed metrics summary, verbatim. Left as a `Value` because its key order is
    /// `metrics::summarize`'s and belongs to that module, not this one.
    ///
    /// Absent or `null` reads as `{}`, which is what `field(default_factory=dict)` plus
    /// `dict(snapshot.metrics or {})` amounts to on the Python side.
    #[serde(default = "empty_object", deserialize_with = "de_metrics")]
    pub metrics: Value,
    /// Per-source failures, in the order the collector hit them.
    #[serde(
        default,
        serialize_with = "serialize_errors",
        deserialize_with = "deserialize_errors"
    )]
    pub errors: Vec<SourceError>,
    /// Per-source freshness. Not part of the Python JSON; the snapshot emitter must not print it.
    #[serde(
        default,
        deserialize_with = "de_null_as_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub health: BTreeMap<String, SourceHealth>,
}

impl Snapshot {
    /// An empty pass stamped with the collection time.
    ///
    /// Takes `DateTime<Utc>` because a pass is stamped from the product's own clock, which is UTC;
    /// the offset [`Timestamp`] preserves matters only for values *read back* from a source.
    pub fn new(collected_at: DateTime<Utc>) -> Self {
        Self {
            collected_at: Some(collected_at.fixed_offset()),
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

/// The `{}` a snapshot with no metrics carries — `field(default_factory=dict)`.
fn empty_object() -> Value {
    Value::Object(Map::new())
}

/// Read `metrics` with `dict(snapshot.metrics or {})` tolerance: `null` is an empty summary.
fn de_metrics<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
    let value = Value::deserialize(deserializer)?;
    Ok(if value.is_null() {
        empty_object()
    } else {
        value
    })
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
            f.write_str("a map of source name to failure text, or null")
        }

        // `dict(snapshot.errors or {})`: a `null` errors map is an empty one, not a parse error.
        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_some<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_any(self)
        }

        fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
            let mut out = Vec::with_capacity(access.size_hint().unwrap_or(0));
            while let Some((source, message)) = access.next_entry::<String, String>()? {
                out.push(SourceError { source, message });
            }
            Ok(out)
        }
    }

    deserializer.deserialize_any(OrderedErrors)
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
        run.start = Some(at(1_000).fixed_offset());
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
    fn a_null_collection_reads_as_empty_because_python_writes_or_default() {
        // Every one of these is `null` on the wire from Airflow or `gh`, and Python's
        // `list(... or [])` / `dict(... or {})` reads each as empty. Rejecting them would fail on
        // real payloads, not just on fixtures.
        let snap: Snapshot = serde_json::from_value(json!({
            "collected_at": "2026-09-03T12:00:00+00:00",
            "runs": null,
            "gates": null,
            "prs": null,
            "sandboxes": null,
            "metrics": null,
            "errors": null,
            "health": null,
        }))
        .expect("nulls are empty, not errors");
        assert!(snap.runs.is_empty() && snap.gates.is_empty() && snap.prs.is_empty());
        assert!(snap.sandboxes.is_empty() && snap.errors.is_empty() && snap.health.is_empty());
        assert_eq!(snap.metrics, json!({}));

        let gate: Gate = serde_json::from_value(json!({
            "dag_id": "f", "run_id": "r", "task_id": "job.approve_plan", "map_index": -1,
            "subject": "", "body": "", "created_at": null, "options": null,
        }))
        .expect("a gate with no options");
        assert!(gate.options.is_empty() && gate.created_at.is_none());

        let pr: PullRequest = serde_json::from_value(json!({
            "number": 7, "title": "t", "url": "u", "labels": null,
            "state": "OPEN", "checks": "", "head": "h",
        }))
        .expect("a PR with no labels");
        assert!(pr.labels.is_empty());
    }

    #[test]
    fn an_absent_key_is_the_dataclass_default() {
        // `Snapshot` is `field(default_factory=list)` all the way down, so a document carrying
        // only `collected_at` is a complete, empty snapshot.
        let snap: Snapshot =
            serde_json::from_value(json!({"collected_at": "2026-09-03T12:00:00+00:00"}))
                .expect("a bare snapshot");
        assert!(snap.runs.is_empty() && snap.errors.is_empty());
        assert_eq!(snap.metrics, json!({}));
        assert!(snap.collected_at.is_some());

        let bare: Snapshot =
            serde_json::from_value(json!({})).expect("even collected_at is optional");
        assert!(bare.collected_at.is_none());
    }

    #[test]
    fn a_reported_offset_is_preserved_rather_than_normalised() {
        // `_as_datetime` returns the parsed datetime untouched when it is already aware, and
        // `_iso` prints *its* offset. `14:00+02:00` is the same instant as `12:00Z` and a
        // different document.
        let at = parse_timestamp(&json!("2026-09-03T14:00:00+02:00")).expect("aware");
        assert_eq!(iso(at), "2026-09-03T14:00:00+02:00");
        let same_instant = parse_timestamp(&json!("2026-09-03T12:00:00Z")).expect("utc");
        assert_eq!(at, same_instant);
        assert_ne!(iso(at), iso(same_instant));
    }

    #[test]
    fn a_naive_timestamp_is_stamped_utc_not_shifted() {
        // `dt.replace(tzinfo=UTC)` — the wall clock stays put and gains a UTC label.
        let at = parse_timestamp(&json!("2026-09-03T12:00:00")).expect("naive");
        assert_eq!(iso(at), "2026-09-03T12:00:00+00:00");
    }

    #[test]
    fn microseconds_render_six_digits_and_finer_precision_is_dropped() {
        let at = parse_timestamp(&json!("2026-09-03T12:00:00.123456+00:00")).expect("micros");
        assert_eq!(iso(at), "2026-09-03T12:00:00.123456+00:00");
        // Python's `datetime` has no sub-microsecond field, so neither does the rendering.
        let nanos = parse_timestamp(&json!("2026-09-03T12:00:00.000000999+00:00")).expect("nanos");
        assert_eq!(iso(nanos), "2026-09-03T12:00:00+00:00");
    }

    #[test]
    fn an_unreadable_timestamp_is_unknown_rather_than_an_error() {
        // `_as_datetime` swallows `ValueError` and answers `None`; a broken `created_at` in one
        // `islo ls` row must not refuse the whole snapshot.
        for value in [
            json!("not a date"),
            json!(""),
            json!(null),
            json!(1_756_900_800),
        ] {
            assert!(parse_timestamp(&value).is_none(), "{value}");
        }
        let sandbox: SandboxRef = serde_json::from_value(json!({
            "name": "swf-x-0123abcd", "status": "running", "created_by": "me",
            "created_at": "nonsense",
        }))
        .expect("a sandbox with a broken stamp still reads");
        assert!(sandbox.created_at.is_none());
    }

    #[test]
    fn the_spellings_fromisoformat_takes_and_chrono_does_not_are_reconciled() {
        // Verified against CPython 3.12 `datetime.fromisoformat` one string at a time.
        for (input, want) in [
            // Any single character separates the date from the time.
            ("2026-09-03t12:00:00", "2026-09-03T12:00:00+00:00"),
            ("2026-09-03 12:00:00", "2026-09-03T12:00:00+00:00"),
            ("2026-09-03_12:00:00", "2026-09-03T12:00:00+00:00"),
            ("2026-09-03-12:00:00", "2026-09-03T12:00:00+00:00"),
            // An hour-only offset is ISO-8601 and `fromisoformat` keeps it.
            ("2026-09-03T12:00:00+02", "2026-09-03T12:00:00+02:00"),
            ("2026-09-03T12:00:00-02", "2026-09-03T12:00:00-02:00"),
            (
                "2026-09-03T12:00:00.123456+02",
                "2026-09-03T12:00:00.123456+02:00",
            ),
        ] {
            let at = parse_timestamp(&json!(input)).unwrap_or_else(|| panic!("{input}"));
            assert_eq!(iso(at), want, "{input}");
        }

        // And the ones chrono takes that Python refuses, so the Rust cannot invent a timestamp
        // where `swfactory herd --once --json` printed `null`.
        for input in [
            // A leap second: `ValueError` in Python, and a seven-digit fraction if it got through.
            "2026-09-03T12:00:60",
            // `fromisoformat` only rewrites an upper-case `Z`.
            "2026-09-03T12:00:00z",
            // Chrono's expanded year; `fromisoformat` has no such form.
            "+002026-09-03T12:00:00",
            // Index 10 is a digit, so this is not a date followed by a separator.
            "2026-09-0312:00:00",
        ] {
            assert!(parse_timestamp(&json!(input)).is_none(), "{input}");
        }
    }

    #[test]
    fn a_leap_second_never_reaches_the_document_as_a_seven_digit_fraction() {
        // The parser refuses one, and `iso` refuses to spell one even if a caller assigns it.
        let leap = Utc
            .with_ymd_and_hms(2026, 12, 31, 23, 59, 59)
            .single()
            .and_then(|dt| dt.with_nanosecond(1_500_000_000))
            .expect("chrono spells a leap second as nanos past 1e9")
            .fixed_offset();
        let rendered = iso(leap);
        if let Some((_, rest)) = rendered.split_once('.') {
            let digits = rest.chars().take_while(char::is_ascii_digit).count();
            assert_eq!(digits, 6, "{rendered} is not Python's six-digit fraction");
        }
        assert!(parse_timestamp(&json!("2026-12-31T23:59:60")).is_none());
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
