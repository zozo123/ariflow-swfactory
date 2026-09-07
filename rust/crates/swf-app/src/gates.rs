//! Approvals: which gate is genuinely answerable, and the re-read that happens before the write.
//!
//! Two pieces of hard-won operational knowledge live here, and nowhere else in the product.
//!
//! **Readiness.** A HITL detail exists from the moment the operator task *creates* it, which is a
//! beat before the task defers. Answering inside that window makes the scheduler see a stale
//! executor event and **fail the gate** — the failure `scripts/stress_airflow.sh` was written to
//! reproduce. So a gate is `ready` only when its own task instance is parked in `awaiting_input`
//! (or `deferred` on older builds), which means joining the HITL details against the task states
//! rather than trusting the detail list alone.
//!
//! **Settling.** Readiness is necessary and not sufficient, because the window does not close when
//! the task instance flips to `awaiting_input`; it closes when the scheduler has finished
//! reconciling the worker process that parked it. A task instance can read `awaiting_input` while
//! the executor event for that same process is still in the scheduler's queue — answer there and
//! the response re-queues the task under an event that is about to be judged stale, and the gate
//! is marked failed. So a gate must be seen parked at **two moments at least
//! [`CONFIRM_INTERVAL`] apart** (`00-architecture.md` §C.3). Two *reads* are not two moments: a
//! selection and the re-read that follows it milliseconds later are one observation of the world
//! made twice, and counting them as two is exactly how a gate gets answered inside the window.
//!
//! **Re-validation.** [`answer`] re-reads the gate immediately before the PATCH and refuses if it
//! has been answered meanwhile, or if the evidence the operator read has moved under them. Another
//! operator getting there first is a normal outcome (exit 6), not a crash — and approving text you
//! did not see is the outcome this refuses to allow at all.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use swf_adapters::airflow::{GATE_APPROVE, GATE_REJECT};
use swf_adapters::traits::Runs;
use swf_domain::ids::{GateId, JobId};
use swf_domain::model::{Gate, JobRow, Snapshot, TaskState};
use swf_domain::rollup::{job_state, stage_progress};
use swf_domain::sanitize::{sanitize_block, sanitize_line};
use swf_domain::states::is_gate_parked;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::ops::{ErrorKind, OpsError, Result};

/// How long a gate must have been under observation, parked, before it may be answered.
///
/// This is a window and not a pause for luck. The scheduler marks a HITL task failed when it
/// reconciles the executor event of the worker process that parked it *after* the response has
/// already re-queued the task ("finished with state success, but the task instance's state
/// attribute is queued"). On a live standalone that lag was measured at up to ~0.9 s; three
/// seconds is the interval `scripts/stress_airflow.sh` — the reference harness that does not
/// produce this failure — leaves between the poll that first sees a gate parked and the poll that
/// answers it. It costs a scripted approval one pause; it costs an interactive one nothing at all,
/// because the TUI sighted the gate on a refresh long before the operator pressed a key.
pub const CONFIRM_INTERVAL: Duration = Duration::from_secs(3);

/// How many times a gate must be seen parked before it may be answered.
///
/// A count on its own is not the rule — [`CONFIRM_INTERVAL`] is — but two reads remain the minimum
/// because the write has to be preceded by a read that was not the one that selected the gate.
pub const REQUIRED_SIGHTINGS: u32 = 2;

/// Approve or reject. There is no third answer, and no free-text option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Let the job continue.
    Approve,
    /// Stop the job here.
    Reject,
}

impl Decision {
    /// The word Airflow's `chosen_options` carries.
    pub fn word(self) -> &'static str {
        match self {
            Self::Approve => GATE_APPROVE,
            Self::Reject => GATE_REJECT,
        }
    }

    /// True for an approval — the shape the adapter takes.
    pub fn approves(self) -> bool {
        matches!(self, Self::Approve)
    }
}

impl std::fmt::Display for Decision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.word())
    }
}

/// Every gate awaiting an answer, and whether the list is the whole list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GateList {
    /// The gates, readiness already established.
    pub gates: Vec<Gate>,
    /// True when the read stopped at its page bound (rule 3).
    pub truncated: bool,
}

/// What an operator is shown before they answer.
///
/// The `revision` is the load-bearing field: it is what [`answer`] compares against, so the thing
/// that was read and the thing that is approved are provably the same text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateReview {
    /// The gate's identity.
    pub id: GateId,
    /// The gate itself, with subject and body already sanitised for a terminal.
    pub gate: Gate,
    /// The digest of the evidence as reviewed. Pass it back to [`answer`] as `expect`.
    pub revision: String,
    /// True only when the task instance is genuinely parked.
    pub ready: bool,
    /// The task instance's state, so a not-ready gate can say *why* it is not ready.
    pub task_state: String,
    /// The job's rolled-up state.
    pub job_state: String,
    /// Where the job is in the pipeline.
    pub stage: String,
    /// The answers Airflow will accept.
    pub options: Vec<String>,
    /// The deep link to the run, for `o` in the TUI and `--json` consumers.
    pub url: String,
}

/// What actually happened when a gate was answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateAnswer {
    /// Which gate.
    pub id: GateId,
    /// What was chosen.
    pub decision: Decision,
    /// The evidence revision at the moment of the PATCH.
    pub revision: String,
    /// True when readiness was overridden with `--force`, so the output can say so.
    pub forced: bool,
    /// How many times the gate was observed parked before the write.
    pub sightings: u32,
}

/// When a gate was first observed parked, and how many times since.
#[derive(Debug, Clone, Copy)]
struct Sighting {
    /// How many observations, including the first.
    count: u32,
    /// The moment of the first one. The clock the settle is measured on.
    first: Instant,
}

/// What one observation of a gate establishes.
///
/// `settled` is the load-bearing half. A gate observed twice inside a millisecond has a `count` of
/// two and has established nothing: both reads can be answered by the same instant of the world,
/// and that instant can be inside the window in which answering fails the gate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Seen {
    /// How many times this gate has now been observed parked.
    pub count: u32,
    /// How long ago it was **first** observed parked. Zero for the first observation.
    pub settled: Duration,
}

/// When each gate was first seen parked, and how often since, for the life of this process.
///
/// Kept per-`Ops` rather than globally: a context switch points at a different factory, where the
/// same `dag/run#index:task` is a different gate, and carrying a sighting across would be exactly
/// the wrong kind of memory.
#[derive(Debug, Default)]
pub struct Sightings {
    seen: Mutex<HashMap<String, Sighting>>,
}

impl Sightings {
    /// Note that a gate has been observed parked, and answer what that establishes.
    pub fn record(&self, id: &GateId) -> Seen {
        let now = Instant::now();
        let mut guard = match self.seen.lock() {
            Ok(guard) => guard,
            // A poisoned lock means another thread panicked while counting sightings. The count is
            // advisory; losing the reason to panic again is the better trade.
            Err(poisoned) => poisoned.into_inner(),
        };
        let entry = guard.entry(id.to_string()).or_insert(Sighting {
            count: 0,
            first: now,
        });
        entry.count = entry.count.saturating_add(1);
        Seen {
            count: entry.count,
            settled: now.saturating_duration_since(entry.first),
        }
    }

    /// How many times this gate has been observed parked.
    pub fn count(&self, id: &GateId) -> u32 {
        self.get(id).map(|seen| seen.count).unwrap_or(0)
    }

    /// How long this gate has been under observation, or `None` if it has never been seen parked.
    pub fn settled(&self, id: &GateId) -> Option<Duration> {
        self.get(id)
            .map(|seen| Instant::now().saturating_duration_since(seen.first))
    }

    /// This gate's record, if it has one.
    fn get(&self, id: &GateId) -> Option<Sighting> {
        match self.seen.lock() {
            Ok(guard) => guard.get(&id.to_string()).copied(),
            Err(poisoned) => poisoned.into_inner().get(&id.to_string()).copied(),
        }
    }

    /// Forget a gate, once it has been answered or has disappeared.
    pub fn forget(&self, id: &GateId) {
        let mut guard = match self.seen.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.remove(&id.to_string());
    }
}

/// True when this gate's own task instance is parked, waiting for a human.
///
/// The join is on `(task_id, map_index)` and not on `task_id` alone: a fanned-out run has one
/// `job.approve_plan` per job, and job 3 being parked says nothing about job 5.
pub fn is_ready(gate: &Gate, tasks: &[TaskState]) -> bool {
    tasks.iter().any(|task| {
        task.task_id == gate.task_id
            && task.map_index == gate.map_index
            && is_gate_parked(task.state_or_none())
    })
}

/// The task instance's state, for a message that explains a refusal.
fn task_state_of(gate: &Gate, tasks: &[TaskState]) -> String {
    tasks
        .iter()
        .find(|task| task.task_id == gate.task_id && task.map_index == gate.map_index)
        .map(|task| task.state_or_none().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Every pending gate, with readiness established against the task states.
pub async fn list(runs: &dyn Runs, cancel: &CancellationToken) -> Result<GateList> {
    Ok(select(runs, &GateFilter::default(), cancel)
        .await?
        .into_list())
}

/// Establish readiness for the gates of a snapshot that has already been collected.
///
/// The job rows a pass just read already carry their task instances, so most gates cost nothing
/// extra; only a run that collapsed (or that failed its job-row read) needs a call of its own. A
/// run we cannot read leaves its gates `ready: false`, which is the safe direction to be wrong in.
pub async fn mark_ready(runs: &dyn Runs, snap: &mut Snapshot, cancel: &CancellationToken) {
    let mut cache: Vec<(String, Vec<TaskState>)> = Vec::new();
    for run in &snap.runs {
        let tasks: Vec<TaskState> = run
            .jobs
            .iter()
            .flat_map(|job| job.tasks.iter().cloned())
            .collect();
        if !tasks.is_empty() {
            cache.push((key(&run.dag_id, &run.run_id), tasks));
        }
    }
    let mut gates = std::mem::take(&mut snap.gates);
    for gate in &mut gates {
        if cancel.is_cancelled() {
            break;
        }
        let tasks = tasks_for(runs, &mut cache, &gate.dag_id, &gate.run_id, cancel).await;
        gate.ready = is_ready(gate, &tasks);
    }
    snap.gates = gates;
}

/// The cache key for one run's task instances.
fn key(dag_id: &str, run_id: &str) -> String {
    format!("{dag_id}/{run_id}")
}

/// One run's task instances, read at most once per pass.
async fn tasks_for(
    runs: &dyn Runs,
    cache: &mut Vec<(String, Vec<TaskState>)>,
    dag_id: &str,
    run_id: &str,
    cancel: &CancellationToken,
) -> Vec<TaskState> {
    let cache_key = key(dag_id, run_id);
    if let Some((_, tasks)) = cache.iter().find(|(k, _)| *k == cache_key) {
        return tasks.clone();
    }
    let run = swf_domain::ids::RunRef::new(dag_id, run_id);
    // A run whose task instances cannot be read leaves its gates un-ready. Refusing to answer a
    // gate we could not verify is the failure mode we want.
    let tasks = runs
        .task_states(&run, cancel)
        .await
        .map(|page| page.rows)
        .unwrap_or_default();
    cache.push((cache_key, tasks.clone()));
    tasks
}

/// Everything an operator needs in front of them before they decide.
pub async fn review(
    runs: &dyn Runs,
    id: &GateId,
    cancel: &CancellationToken,
) -> Result<GateReview> {
    let listing = runs.pending_gates(cancel).await?;
    let gate = listing
        .gates_matching(id)
        .ok_or_else(|| not_pending(id, &listing.rows))?;
    let tasks = runs
        .task_states(&id.job.run(), cancel)
        .await
        .map(|page| page.rows)
        .unwrap_or_default();

    let job_tasks: Vec<TaskState> = tasks
        .iter()
        .filter(|task| task.map_index == id.job.map_index)
        .cloned()
        .collect();
    let ready = is_ready(&gate, &tasks);
    let mut shown = gate.clone();
    // Sanitised here, at the point the evidence stops being a service payload and becomes
    // something a terminal will render (rule 7). The revision is computed from the *sanitised*
    // text so that what is compared is what was shown.
    shown.subject = sanitize_line(&gate.subject);
    shown.body = sanitize_block(&gate.body);
    shown.ready = ready;
    shown.revision = shown.current_revision();

    Ok(GateReview {
        id: id.clone(),
        revision: shown.revision.clone(),
        ready,
        task_state: task_state_of(&gate, &tasks),
        job_state: job_state(&job_tasks),
        stage: stage_progress(&job_tasks),
        options: shown.options.clone(),
        url: runs.run_url(&id.job.run()),
        gate: shown,
    })
}

/// What may override the readiness rule, and what must match before the write.
#[derive(Debug, Clone, Default)]
pub struct AnswerOpts {
    /// The evidence revision the operator reviewed. A mismatch is a conflict, not a warning.
    pub expect: Option<String>,
    /// Skip the readiness rule. Say so in the output — this is the flag that can fail a gate.
    pub force: bool,
    /// How long a gate must have been observed parked before the write. `None` uses
    /// [`CONFIRM_INTERVAL`]; the tests are the only caller with a reason to shorten it.
    pub confirm_delay: Option<Duration>,
}

/// Answer one gate, after proving it is still the gate that was reviewed.
///
/// The order is deliberate: re-read, then compare the evidence, then check the options, then
/// readiness, and only then write. Each step is cheaper to fail than the one after it, and the
/// last one is the only one that cannot be undone.
pub async fn answer(
    runs: &dyn Runs,
    sightings: &Sightings,
    id: &GateId,
    decision: Decision,
    opts: &AnswerOpts,
    cancel: &CancellationToken,
) -> Result<GateAnswer> {
    let (gate, ready, state) = read(runs, id, cancel).await?;
    let revision = Gate::revision_of(&sanitize_line(&gate.subject), &sanitize_block(&gate.body));

    if let Some(expected) = opts.expect.as_deref() {
        if expected != revision {
            return Err(OpsError::conflict(format!(
                "the evidence for {id} changed since it was reviewed \
                 (reviewed {expected}, now {revision})"
            ))
            .with_hint(format!("swf gates review {id}")));
        }
    }

    if !gate.accepts(decision.word()) {
        return Err(OpsError::operational(format!(
            "{id} does not accept {:?}; it offers {}",
            decision.word(),
            gate.options.join(", ")
        )));
    }

    let seen = if ready {
        sightings.record(id)
    } else {
        Seen::default()
    };
    if !ready && !opts.force {
        return Err(OpsError::operational(format!(
            "{id} {NOT_READY}: its task instance is {state:?}, not awaiting_input. \
             Answering now would make the scheduler fail the gate"
        ))
        .with_hint("wait for the task to park, or pass --force if you accept the risk"));
    }

    let settle = opts.confirm_delay.unwrap_or(CONFIRM_INTERVAL);
    let mut count = seen.count;
    if ready && !opts.force && (count < REQUIRED_SIGHTINGS || seen.settled < settle) {
        // What has to elapse is time under observation, not reads. `seen.settled` is measured from
        // the FIRST sighting — the listing or the review that put this gate in front of somebody —
        // so an operator who has been watching the gate waits for nothing, and a batch that
        // selected it a millisecond ago waits out the rest of the window rather than mistaking its
        // own selection for a second look at the world.
        let wait = settle.saturating_sub(seen.settled);
        if !wait.is_zero() {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(OpsError::cancelled()),
                () = tokio::time::sleep(wait) => {}
            }
        }
        let (again, still_ready, state) = read(runs, id, cancel).await?;
        let now_revision =
            Gate::revision_of(&sanitize_line(&again.subject), &sanitize_block(&again.body));
        if now_revision != revision {
            return Err(OpsError::conflict(format!(
                "the evidence for {id} changed while it was being confirmed"
            )));
        }
        if !still_ready {
            return Err(OpsError::operational(format!(
                "{id} {NOT_READY} any more: it stopped being parked while it was \
                 being confirmed (now {state:?})"
            )));
        }
        count = sightings.record(id).count;
    }

    runs.respond(id, decision.approves(), cancel).await?;
    sightings.forget(id);
    Ok(GateAnswer {
        id: id.clone(),
        decision,
        revision,
        forced: opts.force,
        sightings: count,
    })
}

/// Re-read one gate and its task instance. The read that stands between a review and a write.
async fn read(
    runs: &dyn Runs,
    id: &GateId,
    cancel: &CancellationToken,
) -> Result<(Gate, bool, String)> {
    let listing = runs.pending_gates(cancel).await?;
    let gate = listing
        .gates_matching(id)
        .ok_or_else(|| not_pending(id, &listing.rows))?;
    let tasks = runs
        .task_states(&id.job.run(), cancel)
        .await
        .map(|page| page.rows)
        .unwrap_or_default();
    let ready = is_ready(&gate, &tasks);
    let state = task_state_of(&gate, &tasks);
    Ok((gate, ready, state))
}

/// Why a gate is not in the pending list any more.
///
/// A gate whose run still has other pending gates was answered by someone; a gate whose run is not
/// represented at all was probably never there. The two need different words, because only one of
/// them means "you lost a race".
fn not_pending(id: &GateId, pending: &[Gate]) -> OpsError {
    let run_seen = pending
        .iter()
        .any(|gate| gate.dag_id == id.job.dag_id && gate.run_id == id.job.run_id);
    if run_seen {
        OpsError::conflict(format!(
            "{id} is no longer waiting for an answer; someone answered it first"
        ))
    } else {
        OpsError::not_found(format!("no gate {id} is waiting for an answer"))
            .with_hint("swf gates list")
    }
}

/// Find one gate in a page by identity, so the lookup is written once.
trait GateLookup {
    /// The gate matching `id`, if the page has it.
    fn gates_matching(&self, id: &GateId) -> Option<Gate>;
}

impl GateLookup for swf_adapters::traits::Page<Gate> {
    fn gates_matching(&self, id: &GateId) -> Option<Gate> {
        self.rows
            .iter()
            .find(|gate| {
                gate.dag_id == id.job.dag_id
                    && gate.run_id == id.job.run_id
                    && gate.map_index == id.job.map_index
                    && gate.task_id == id.task_id
            })
            .cloned()
    }
}

/// The job a gate belongs to, for a caller holding only the gate.
pub fn job_of(gate: &Gate) -> JobId {
    gate.job()
}

// ---------------------------------------------------------------------------- selection

/// Which gates an operator means, in the words the command line uses.
///
/// Every field **narrows** and none widens: an unmatched filter yields an empty selection rather
/// than the whole factory. That is the only safe direction for a filter that is also the selector
/// of a bulk write — a filter that fell back to "everything" on a typo would answer a hundred
/// gates nobody asked about.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GateFilter {
    /// One DAG id, exactly as Airflow spells it.
    pub dag: Option<String>,
    /// A blueprint name.
    ///
    /// A blueprint *is* its DAG id — `submit` triggers `blueprint.name` — so this is the same
    /// selector spelled the way an operator who submitted the work thinks of it. Giving both is
    /// legal and narrows twice, which is why two different names disagreeing selects nothing.
    pub blueprint: Option<String>,
    /// The issue reference the gate's job answers.
    ///
    /// A gate row does not carry its issue, so this is the one filter that has to read each
    /// matched run's job rows; it is therefore only ever paid for when it was asked for.
    pub issue: Option<String>,
    /// A gate stage: `plan`, `approve_plan` and `job.approve_plan` all name the same gate.
    pub gate: Option<String>,
    /// Keep only gates that can be answered right now.
    pub ready: bool,
    /// At most this many gates. A selection the limit cut is reported truncated (rule 3).
    pub limit: Option<usize>,
}

impl GateFilter {
    /// True when nothing was asked for, so the selection is every pending gate.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The filter as the operator typed it, for a confirmation prompt and a report.
    ///
    /// It is echoed rather than summarised as a count on purpose: a person about to answer a
    /// hundred gates has to be shown *what produced that number*, because the failure mode being
    /// guarded against is a filter that means something other than what its author thought.
    pub fn describe(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(dag) = &self.dag {
            parts.push(format!("--dag {dag}"));
        }
        if let Some(blueprint) = &self.blueprint {
            parts.push(format!("--blueprint {blueprint}"));
        }
        if let Some(issue) = &self.issue {
            parts.push(format!("--issue {issue}"));
        }
        if let Some(gate) = &self.gate {
            parts.push(format!("--gate {gate}"));
        }
        if self.ready {
            parts.push("--ready".to_string());
        }
        if let Some(limit) = self.limit {
            parts.push(format!("--limit {limit}"));
        }
        if parts.is_empty() {
            "no filter: every pending gate".to_string()
        } else {
            parts.join(" ")
        }
    }

    /// True when this DAG id survives `--dag` and `--blueprint`.
    pub fn matches_dag(&self, dag_id: &str) -> bool {
        let wanted = [self.dag.as_deref(), self.blueprint.as_deref()];
        wanted.iter().flatten().all(|want| want.trim() == dag_id)
    }

    /// True when this task id is the gate `--gate` named.
    pub fn matches_gate(&self, task_id: &str) -> bool {
        match &self.gate {
            None => true,
            Some(want) => stage_of(want).eq_ignore_ascii_case(stage_of(task_id)),
        }
    }

    /// True when this job's issue is the one `--issue` named.
    ///
    /// An issue that could not be established does **not** match: the filter selects a set that is
    /// about to be answered, and "we could not tell" has to fall outside it.
    pub fn matches_issue(&self, issue: Option<&str>) -> bool {
        match &self.issue {
            None => true,
            Some(want) => issue.is_some_and(|have| have.trim().eq_ignore_ascii_case(want.trim())),
        }
    }

    /// True when this filter needs the job rows, i.e. one extra read per run.
    fn needs_issues(&self) -> bool {
        self.issue.is_some()
    }
}

/// The bare stage a gate name means: `job.approve_plan`, `approve_plan` and `plan` all give `plan`.
///
/// Written here rather than reusing `GateId::parse`'s expansion because this compares two names
/// that may both be short, and neither of them is an identity.
fn stage_of(name: &str) -> &str {
    let tail = name.trim().rsplit('.').next().unwrap_or("").trim();
    tail.strip_prefix("approve_").unwrap_or(tail)
}

/// One gate a filter matched, with the facts that decided whether it can be answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selected {
    /// The gate, `ready` already established against its own task instance.
    pub gate: Gate,
    /// That task instance's state, so a skipped gate can say *why* and not merely that it was.
    pub task_state: String,
    /// The issue this gate's job answers — `None` when nothing asked us to look it up.
    pub issue: Option<String>,
}

/// What one filtered read of the gate list found, and whether it is the whole of it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// The matching gates, in the order Airflow returned them.
    pub rows: Vec<Selected>,
    /// True when the page bound or the limit shortened this selection (rule 3).
    pub truncated: bool,
}

impl Selection {
    /// How many gates matched.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when nothing matched.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// How many of them can be answered now.
    pub fn ready_count(&self) -> usize {
        self.rows.iter().filter(|row| row.gate.ready).count()
    }

    /// The gates alone, for a listing.
    pub fn into_list(self) -> GateList {
        GateList {
            gates: self.rows.into_iter().map(|row| row.gate).collect(),
            truncated: self.truncated,
        }
    }
}

/// The gates a filter selects, with readiness established against the task states.
///
/// The cheap filters are applied to the gate rows **before** any per-run read, because that join
/// is what a listing actually costs: one or two calls per distinct run. Paying it for gates the
/// operator excluded is the "fetch ten thousand rows to discard nine thousand" this filter set
/// exists to stop, and `--limit` stops the loop rather than trimming its result.
pub async fn select(
    runs: &dyn Runs,
    filter: &GateFilter,
    cancel: &CancellationToken,
) -> Result<Selection> {
    let page = runs.pending_gates(cancel).await?;
    let candidates: Vec<Gate> = page
        .rows
        .into_iter()
        .filter(|gate| filter.matches_dag(&gate.dag_id) && filter.matches_gate(&gate.task_id))
        .collect();

    let mut tasks_cache: Vec<(String, Vec<TaskState>)> = Vec::new();
    let mut jobs_cache: Vec<(String, Vec<JobRow>)> = Vec::new();
    let mut rows: Vec<Selected> = Vec::new();
    let mut cut = false;

    for gate in candidates {
        if cancel.is_cancelled() {
            return Err(OpsError::cancelled());
        }
        let tasks = tasks_for(runs, &mut tasks_cache, &gate.dag_id, &gate.run_id, cancel).await;
        let issue = if filter.needs_issues() {
            // Readiness still comes from the task states above: `job_rows` answers the issue, and
            // a gate whose row is missing must not lose its readiness to a lookup it never needed.
            issue_for(runs, &mut jobs_cache, &gate, cancel).await
        } else {
            None
        };
        if !filter.matches_issue(issue.as_deref()) {
            continue;
        }
        let mut gate = gate;
        gate.ready = is_ready(&gate, &tasks);
        if filter.ready && !gate.ready {
            continue;
        }
        let task_state = task_state_of(&gate, &tasks);
        rows.push(Selected {
            gate,
            task_state,
            issue,
        });
        if filter.limit.is_some_and(|limit| rows.len() > limit) {
            // One row past the bound is how the limit is *known* to have cut something, rather
            // than inferred from a count that happened to land on it.
            rows.pop();
            cut = true;
            break;
        }
    }

    Ok(Selection {
        rows,
        truncated: page.truncated || cut,
    })
}

/// The issue one gate's job answers, reading each run's job rows at most once.
async fn issue_for(
    runs: &dyn Runs,
    cache: &mut Vec<(String, Vec<JobRow>)>,
    gate: &Gate,
    cancel: &CancellationToken,
) -> Option<String> {
    let cache_key = key(&gate.dag_id, &gate.run_id);
    if !cache.iter().any(|(k, _)| *k == cache_key) {
        let run = swf_domain::ids::RunRef::new(&gate.dag_id, &gate.run_id);
        // A run whose rows cannot be read has no issue we can prove, and `matches_issue` excludes
        // it. Being wrong towards "not selected" is the only acceptable direction before a write.
        let rows = runs
            .job_rows(&run, &[], cancel)
            .await
            .map(|page| page.rows)
            .unwrap_or_default();
        cache.push((cache_key.clone(), rows));
    }
    cache
        .iter()
        .find(|(k, _)| *k == cache_key)
        .and_then(|(_, rows)| rows.iter().find(|row| row.map_index == gate.map_index))
        .map(|row| row.issue.clone())
}

// ---------------------------------------------------------------------------- answering a batch

/// How many gates a batch answers at once.
///
/// Six is a compromise with two failure modes on either side of it. Every answer re-reads its own
/// gate before the PATCH, so a serial loop over a hundred and twenty gates is a coffee break; a
/// hundred simultaneous PATCHes against one scheduler is an outage of the batch's own making.
pub const BATCH_CONCURRENCY: usize = 6;

/// The words a readiness refusal carries.
///
/// A batch has to tell "this gate is not answerable yet" apart from "answering it went wrong",
/// because only the second is a failure. The phrase is a constant so the two sites that write it
/// and the one that recognises it cannot drift into disagreement over prose.
const NOT_READY: &str = "is not ready";

/// Where one gate's answer landed.
///
/// `Conflict` is deliberately not a failure: in a shared control room somebody else answering
/// first is the system working, and a batch that exited non-zero for it would teach operators to
/// ignore its exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    /// A dry run would have answered this gate. Nothing was written.
    Planned,
    /// Answered.
    Answered,
    /// Matched, but not answerable — nearly always a gate that has not parked yet.
    Skipped,
    /// Someone else answered it first, or it stopped being pending.
    Conflict,
    /// The write was attempted and went wrong.
    Failed,
}

impl BatchOutcome {
    /// The word the report and the `--json` document both use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Answered => "answered",
            Self::Skipped => "skipped",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
        }
    }

    /// True only for the outcome that means something actually went wrong.
    pub fn is_failure(self) -> bool {
        matches!(self, Self::Failed)
    }
}

/// One gate's line in a batch report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchItem {
    /// Which gate.
    pub id: GateId,
    /// The bare stage name, e.g. `approve_plan`.
    pub gate: String,
    /// The issue its job answers, when the filter made us read it.
    pub issue: Option<String>,
    /// Whether it was answerable when it was selected.
    pub ready: bool,
    /// What happened.
    pub outcome: BatchOutcome,
    /// Why, for anything that is not a plain answer. Empty otherwise.
    pub detail: String,
    /// The evidence revision that was answered, for an answered gate.
    pub revision: String,
    /// How many times the gate was seen parked before the write.
    pub sightings: u32,
    /// A failure's classification, so a batch that failed for one reason keeps that reason's exit
    /// code instead of flattening a dead credential into a generic 1.
    pub kind: Option<ErrorKind>,
}

impl BatchItem {
    /// The line for a gate that was never a candidate for a write.
    fn skipped(row: &Selected, detail: impl Into<String>) -> Self {
        Self {
            id: row.gate.id(),
            gate: row.gate.short_name().to_string(),
            issue: row.issue.clone(),
            ready: row.gate.ready,
            outcome: BatchOutcome::Skipped,
            detail: sanitize_line(&detail.into()),
            revision: String::new(),
            sightings: 0,
            kind: None,
        }
    }
}

/// What a batch did, or would do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReport {
    /// Approve or reject — one decision for the whole batch.
    pub decision: Decision,
    /// True when nothing was written.
    pub dry_run: bool,
    /// The filter that produced this set, echoed back.
    pub filter: String,
    /// True when the selection was shortened by the page bound or the limit.
    pub truncated: bool,
    /// One line per matched gate, in selection order.
    pub items: Vec<BatchItem>,
}

impl BatchReport {
    /// How many gates the filter matched.
    pub fn matched(&self) -> usize {
        self.items.len()
    }

    /// How many gates ended in one particular outcome.
    pub fn count(&self, outcome: BatchOutcome) -> usize {
        self.items
            .iter()
            .filter(|item| item.outcome == outcome)
            .count()
    }

    /// How many gates actually went wrong.
    pub fn failures(&self) -> usize {
        self.items
            .iter()
            .filter(|item| item.outcome.is_failure())
            .count()
    }

    /// What the shell is told.
    ///
    /// Zero unless something failed — a skipped gate arms within seconds and a conflict means the
    /// factory worked. When every failure shares one classification the batch keeps it, because a
    /// script that sees `4` must still learn it needs a credential; a mixed batch is a plain
    /// operational failure.
    pub fn exit_code(&self) -> i32 {
        let mut kinds = self
            .items
            .iter()
            .filter(|item| item.outcome.is_failure())
            .map(|item| item.kind);
        let Some(first) = kinds.next() else {
            return 0;
        };
        match first {
            Some(kind) if kinds.all(|other| other == Some(kind)) => kind.exit_code(),
            _ => 1,
        }
    }

    /// What a dry run answers: the whole selection, and not one write.
    ///
    /// It is built from the same [`Selection`] the real batch consumes and marks the same gates
    /// skipped for the same reasons, so "read this, then re-run it without `--dry-run`" is a
    /// promise about one list rather than two.
    pub fn dry(selection: &Selection, decision: Decision, filter: &GateFilter) -> Self {
        let items = selection
            .rows
            .iter()
            .map(|row| {
                if row.gate.ready {
                    BatchItem {
                        outcome: BatchOutcome::Planned,
                        revision: row.gate.current_revision(),
                        ..BatchItem::skipped(row, String::new())
                    }
                } else {
                    BatchItem::skipped(row, not_ready_reason(&row.task_state))
                }
            })
            .collect();
        Self {
            decision,
            dry_run: true,
            filter: filter.describe(),
            truncated: selection.truncated,
            items,
        }
    }
}

/// Why a gate that matched cannot be answered, in one clause.
fn not_ready_reason(task_state: &str) -> String {
    format!("{NOT_READY}: its task instance is {task_state:?}, not awaiting_input")
}

/// Answer every selected gate, one outcome per gate, with a bounded number in flight.
///
/// Four rules are structural here rather than left to the caller. A gate that is not ready is
/// never handed to [`answer`] at all — a batch has no equivalent of `--force`, because forcing is
/// a decision about one gate somebody read and it does not generalise to a set nobody has. One bad
/// gate cannot abandon the rest, because each answer is its own task and its own line in the
/// report. Every gate is still re-read immediately before its own PATCH, since [`answer`] is the
/// only writer: there is no bulk path that skips the re-validation a single answer performs. And
/// every gate still settles for [`CONFIRM_INTERVAL`] counted from the moment it was FIRST seen
/// parked — the selection that produced this batch is that moment, not a discharge of it, so a
/// wave of gates selected together settles once, concurrently, and not once per gate.
///
/// `settle` overrides that window; `None` is [`CONFIRM_INTERVAL`] and is what the product passes.
pub async fn answer_all(
    runs: Arc<dyn Runs>,
    sightings: Arc<Sightings>,
    selection: &Selection,
    decision: Decision,
    filter: &GateFilter,
    settle: Option<Duration>,
    cancel: &CancellationToken,
) -> BatchReport {
    let mut slots: Vec<Option<BatchItem>> = vec![None; selection.rows.len()];
    let mut queue: Vec<usize> = Vec::new();
    for (index, row) in selection.rows.iter().enumerate() {
        if row.gate.ready {
            queue.push(index);
        } else {
            slots[index] = Some(BatchItem::skipped(row, not_ready_reason(&row.task_state)));
        }
    }

    let mut running: JoinSet<(usize, BatchItem)> = JoinSet::new();
    // Which gate each in-flight task is answering. A task that *panics* answers nothing at all, so
    // the only way to give its gate an honest line is to know, from the outside, which gate it was.
    let mut in_flight: Vec<(tokio::task::Id, usize)> = Vec::new();
    // One write at a time per DAG run. Concurrency across runs is free, but two answers racing
    // inside ONE run are not: Airflow answered a second concurrent PATCH to the same run with
    // HTTP 500 and failed the gate, which then failed its job. The old one-gate-at-a-time loop
    // never produced two writes to a run at once and so never saw it. Gates of different runs
    // still overlap, which is where the speed actually comes from — a busy factory is wide, not
    // deep, and a single run only ever has a handful of gates open at a time anyway.
    let mut busy: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    let run_key = |index: usize| -> (String, String) {
        let gate = &selection.rows[index].gate;
        (gate.dag_id.clone(), gate.run_id.clone())
    };
    let mut pending: std::collections::VecDeque<usize> = queue.into_iter().collect();
    loop {
        let mut deferred: Vec<usize> = Vec::new();
        while running.len() < BATCH_CONCURRENCY && !cancel.is_cancelled() {
            let Some(index) = pending.pop_front() else {
                break;
            };
            if !busy.insert(run_key(index)) {
                // Its run already has a write in flight; take it on a later pass.
                deferred.push(index);
                continue;
            }
            let row = &selection.rows[index];
            let id = row.gate.id();
            let gate = row.gate.short_name().to_string();
            let issue = row.issue.clone();
            let runs = Arc::clone(&runs);
            let sightings = Arc::clone(&sightings);
            let cancel = cancel.clone();
            let handle = running.spawn(async move {
                let opts = AnswerOpts {
                    confirm_delay: settle,
                    ..AnswerOpts::default()
                };
                let outcome = answer(
                    runs.as_ref(),
                    sightings.as_ref(),
                    &id,
                    decision,
                    &opts,
                    &cancel,
                )
                .await;
                (index, item_of(id, gate, issue, outcome))
            });
            in_flight.push((handle.id(), index));
        }
        for index in deferred.into_iter().rev() {
            pending.push_front(index);
        }
        match running.join_next_with_id().await {
            None => break,
            Some(Ok((task, (index, item)))) => {
                in_flight.retain(|(id, _)| *id != task);
                busy.remove(&run_key(index));
                slots[index] = Some(item);
            }
            Some(Err(err)) => {
                // The task died mid-answer, which means this gate's write either did not happen or
                // did and was never confirmed. Neither is "skipped": a batch that exited 0 over a
                // gate whose fate it cannot state would be teaching operators to ignore its code.
                let task = err.id();
                if let Some(at) = in_flight.iter().position(|(id, _)| *id == task) {
                    let (_, index) = in_flight.remove(at);
                    // Release the run even though this gate's fate is unknown: holding the lock
                    // would strand every other gate of that run behind a task that is already dead.
                    busy.remove(&run_key(index));
                    let row = &selection.rows[index];
                    slots[index] = Some(BatchItem {
                        outcome: BatchOutcome::Failed,
                        detail: sanitize_line(&format!(
                            "the answer to this gate did not complete ({err}); \
                             re-read it before assuming it was not written"
                        )),
                        kind: Some(ErrorKind::Operational),
                        ..BatchItem::skipped(row, String::new())
                    });
                }
            }
        }
    }

    let interrupted = cancel.is_cancelled();
    let items = slots
        .into_iter()
        .zip(selection.rows.iter())
        .map(|(slot, row)| {
            slot.unwrap_or_else(|| {
                // A gate with no outcome was never written to, and saying "not answered" is the
                // only honest thing left: the report may not have a hole where a gate was.
                let why = if interrupted {
                    "not answered: the batch was interrupted"
                } else {
                    "not answered: its answer did not complete"
                };
                BatchItem::skipped(row, why)
            })
        })
        .collect();

    BatchReport {
        decision,
        dry_run: false,
        filter: filter.describe(),
        truncated: selection.truncated,
        items,
    }
}

/// Turn one answer into its line, keeping a lost race apart from a failure.
fn item_of(
    id: GateId,
    gate: String,
    issue: Option<String>,
    outcome: Result<GateAnswer>,
) -> BatchItem {
    let base = BatchItem {
        id,
        gate,
        issue,
        ready: true,
        outcome: BatchOutcome::Answered,
        detail: String::new(),
        revision: String::new(),
        sightings: 0,
        kind: None,
    };
    match outcome {
        Ok(answered) => BatchItem {
            revision: answered.revision,
            sightings: answered.sightings,
            ..base
        },
        Err(err) => {
            // A gate that vanished from the pending list between the selection and the write was
            // answered by somebody else; both spellings of that are a conflict, not a failure.
            let outcome = match err.kind {
                ErrorKind::Conflict | ErrorKind::NotFound => BatchOutcome::Conflict,
                _ if err.message.contains(NOT_READY) => BatchOutcome::Skipped,
                _ => BatchOutcome::Failed,
            };
            BatchItem {
                outcome,
                detail: err.message,
                kind: outcome.is_failure().then_some(err.kind),
                ..base
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> Gate {
        Gate::new(
            "factory",
            "manual__2026",
            "job.approve_plan",
            3,
            "Approve plan?",
            "the plan",
            None,
            vec!["Approve".into(), "Reject".into()],
        )
    }

    #[test]
    fn readiness_joins_on_the_job_index_and_not_just_the_task() {
        let gate = gate();
        let other_job = vec![TaskState::new(
            "job.approve_plan",
            5,
            Some("awaiting_input".into()),
        )];
        assert!(
            !is_ready(&gate, &other_job),
            "job 5 being parked says nothing about job 3"
        );

        let right_job = vec![TaskState::new(
            "job.approve_plan",
            3,
            Some("awaiting_input".into()),
        )];
        assert!(is_ready(&gate, &right_job));
    }

    #[test]
    fn a_detail_that_exists_before_its_task_defers_is_not_ready() {
        let gate = gate();
        for state in ["queued", "running", "scheduled", "none", "success"] {
            let tasks = vec![TaskState::new("job.approve_plan", 3, Some(state.into()))];
            assert!(
                !is_ready(&gate, &tasks),
                "{state} is not a parked gate; answering it fails the gate"
            );
        }
        // Older builds park HITL tasks in `deferred`; both spellings count.
        let deferred = vec![TaskState::new(
            "job.approve_plan",
            3,
            Some("deferred".into()),
        )];
        assert!(is_ready(&gate, &deferred));
    }

    #[test]
    fn a_gate_with_no_task_instance_at_all_is_not_ready() {
        assert!(!is_ready(&gate(), &[]));
        assert_eq!(task_state_of(&gate(), &[]), "unknown");
    }

    #[test]
    fn sightings_count_per_gate_and_are_forgotten_once_answered() {
        let seen = Sightings::default();
        let id = gate().id();
        assert_eq!(seen.count(&id), 0);
        assert!(seen.settled(&id).is_none(), "a gate nobody has seen");
        assert_eq!(seen.record(&id).count, 1);
        assert_eq!(seen.record(&id).count, 2);
        assert_eq!(seen.count(&id), 2);
        seen.forget(&id);
        assert_eq!(seen.count(&id), 0);
        assert!(seen.settled(&id).is_none(), "and its clock goes with it");
    }

    #[test]
    fn a_sighting_measures_from_the_first_look_and_not_from_the_last() {
        // The count is what two back-to-back reads inflate; the clock is what they cannot. This is
        // the arithmetic the batch path leans on to tell "seen twice" from "seen for long enough".
        let seen = Sightings::default();
        let id = gate().id();
        assert_eq!(seen.record(&id).settled, Duration::ZERO);
        let second = seen.record(&id);
        assert_eq!(second.count, 2);
        assert!(
            second.settled < CONFIRM_INTERVAL,
            "two immediate reads have established no time at all: {:?}",
            second.settled
        );
    }

    fn item(outcome: BatchOutcome, kind: Option<ErrorKind>) -> BatchItem {
        BatchItem {
            id: gate().id(),
            gate: "approve_plan".into(),
            issue: None,
            ready: true,
            outcome,
            detail: String::new(),
            revision: String::new(),
            sightings: 2,
            kind,
        }
    }

    fn report(items: Vec<BatchItem>) -> BatchReport {
        BatchReport {
            decision: Decision::Approve,
            dry_run: false,
            filter: String::new(),
            truncated: false,
            items,
        }
    }

    #[test]
    fn one_gate_is_the_same_gate_under_all_three_of_its_spellings() {
        let filter = GateFilter {
            gate: Some("plan".into()),
            ..GateFilter::default()
        };
        for spelling in ["job.approve_plan", "approve_plan", "plan", "PLAN"] {
            assert!(filter.matches_gate(spelling), "{spelling} is the plan gate");
        }
        assert!(!filter.matches_gate("job.approve_intent"));
        // No gate filter at all matches every gate, rather than none.
        assert!(GateFilter::default().matches_gate("job.approve_intent"));
    }

    #[test]
    fn a_dag_and_a_blueprint_narrow_the_same_field_and_both_have_to_agree() {
        let both = GateFilter {
            dag: Some("factory".into()),
            blueprint: Some("hotfix".into()),
            ..GateFilter::default()
        };
        assert!(!both.matches_dag("factory"), "two filters both narrow");
        assert!(!both.matches_dag("hotfix"));
        let one = GateFilter {
            blueprint: Some("factory".into()),
            ..GateFilter::default()
        };
        assert!(one.matches_dag("factory"));
        assert!(!one.matches_dag("factory-2"), "a prefix is not a match");
    }

    #[test]
    fn an_issue_that_could_not_be_established_is_outside_the_set() {
        let filter = GateFilter {
            issue: Some("42".into()),
            ..GateFilter::default()
        };
        assert!(filter.matches_issue(Some("42")));
        assert!(filter.matches_issue(Some(" 42 ")));
        assert!(!filter.matches_issue(Some("43")));
        assert!(
            !filter.matches_issue(None),
            "a set about to be answered cannot include a row we could not check"
        );
        assert!(GateFilter::default().matches_issue(None));
    }

    #[test]
    fn a_filter_echoes_itself_the_way_it_was_typed() {
        assert_eq!(
            GateFilter::default().describe(),
            "no filter: every pending gate"
        );
        let filter = GateFilter {
            dag: Some("factory".into()),
            gate: Some("plan".into()),
            ready: true,
            limit: Some(20),
            ..GateFilter::default()
        };
        assert_eq!(
            filter.describe(),
            "--dag factory --gate plan --ready --limit 20"
        );
        assert!(!filter.is_empty());
        assert!(GateFilter::default().is_empty());
    }

    #[test]
    fn a_batch_exits_zero_for_everything_that_is_not_a_failure() {
        assert_eq!(report(Vec::new()).exit_code(), 0);
        let fine = report(vec![
            item(BatchOutcome::Answered, None),
            item(BatchOutcome::Skipped, None),
            item(BatchOutcome::Conflict, None),
        ]);
        assert_eq!(
            fine.exit_code(),
            0,
            "a lost race and a gate still arming are both the system working"
        );
        assert_eq!(fine.count(BatchOutcome::Conflict), 1);
        assert_eq!(fine.matched(), 3);
        assert_eq!(fine.failures(), 0);
    }

    #[test]
    fn a_batch_that_failed_for_one_reason_keeps_that_reasons_exit_code() {
        // A script that sees 4 must know it needs a credential, and a batch every one of whose
        // writes was rejected by the same credential still has exactly one reason.
        let auth = report(vec![
            item(BatchOutcome::Failed, Some(ErrorKind::Auth)),
            item(BatchOutcome::Failed, Some(ErrorKind::Auth)),
            item(BatchOutcome::Answered, None),
        ]);
        assert_eq!(auth.exit_code(), 4);

        let mixed = report(vec![
            item(BatchOutcome::Failed, Some(ErrorKind::Auth)),
            item(BatchOutcome::Failed, Some(ErrorKind::Unreachable)),
        ]);
        assert_eq!(
            mixed.exit_code(),
            1,
            "two reasons is an operational failure"
        );
    }

    #[test]
    fn every_outcome_reads_as_a_word_and_only_one_of_them_is_a_failure() {
        let words = [
            (BatchOutcome::Planned, "planned"),
            (BatchOutcome::Answered, "answered"),
            (BatchOutcome::Skipped, "skipped"),
            (BatchOutcome::Conflict, "conflict"),
            (BatchOutcome::Failed, "failed"),
        ];
        for (outcome, word) in words {
            assert_eq!(outcome.as_str(), word);
            assert_eq!(outcome.is_failure(), outcome == BatchOutcome::Failed);
        }
    }

    #[test]
    fn a_readiness_refusal_is_recognisable_to_the_batch_that_has_to_skip_it() {
        // The batch tells "not answerable yet" from "went wrong" by this phrase, so the message
        // and the recogniser are pinned together here rather than left to drift apart.
        assert!(not_ready_reason("queued").contains(NOT_READY));
        assert!(not_ready_reason("queued").contains("awaiting_input"));
    }

    #[test]
    fn the_decision_words_are_the_ones_airflow_accepts() {
        assert_eq!(Decision::Approve.word(), "Approve");
        assert_eq!(Decision::Reject.word(), "Reject");
        assert!(Decision::Approve.approves());
        assert!(!Decision::Reject.approves());
        assert!(gate().accepts(Decision::Reject.word()));
    }
}
