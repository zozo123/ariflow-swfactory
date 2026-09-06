//! Approvals: which gate is genuinely answerable, and the re-read that happens before the write.
//!
//! Two pieces of hard-won operational knowledge live here, and nowhere else in the product.
//!
//! **Readiness.** A HITL detail exists from the moment the operator task *creates* it, which is a
//! beat before the task defers. Answering inside that window makes the scheduler see a stale
//! executor event and **fail the gate** — the failure `scripts/stress_airflow.sh` was written to
//! reproduce. So a gate is `ready` only when its own task instance is parked in `awaiting_input`
//! (or `deferred` on older builds), which means joining the HITL details against the task states
//! rather than trusting the detail list alone. And because the parked state itself can be observed
//! one poll too early, a gate must be seen ready **twice** before it is answered
//! (`00-architecture.md` §C.3).
//!
//! **Re-validation.** [`answer`] re-reads the gate immediately before the PATCH and refuses if it
//! has been answered meanwhile, or if the evidence the operator read has moved under them. Another
//! operator getting there first is a normal outcome (exit 6), not a crash — and approving text you
//! did not see is the outcome this refuses to allow at all.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use swf_adapters::airflow::{GATE_APPROVE, GATE_REJECT};
use swf_adapters::traits::Runs;
use swf_domain::ids::{GateId, JobId};
use swf_domain::model::{Gate, Snapshot, TaskState};
use swf_domain::rollup::{job_state, stage_progress};
use swf_domain::sanitize::{sanitize_block, sanitize_line};
use swf_domain::states::is_gate_parked;
use tokio_util::sync::CancellationToken;

use crate::ops::{OpsError, Result};

/// How long [`answer`] waits before its confirming re-read when it has only seen a gate once.
///
/// Three seconds is the interval `scripts/stress_airflow.sh` uses, and the window it is closing is
/// sub-second. It costs a scripted approval one pause; it costs an interactive one nothing at all,
/// because the TUI has already sighted the gate on a refresh.
pub const CONFIRM_INTERVAL: Duration = Duration::from_secs(3);

/// How many times a gate must be seen parked before it may be answered.
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

/// How many times each gate has been seen parked, for the life of this process.
///
/// Kept per-`Ops` rather than globally: a context switch points at a different factory, where the
/// same `dag/run#index:task` is a different gate, and carrying a sighting across would be exactly
/// the wrong kind of memory.
#[derive(Debug, Default)]
pub struct Sightings {
    seen: Mutex<HashMap<String, u32>>,
}

impl Sightings {
    /// Note that a gate has been observed parked, and answer the new count.
    pub fn record(&self, id: &GateId) -> u32 {
        let mut guard = match self.seen.lock() {
            Ok(guard) => guard,
            // A poisoned lock means another thread panicked while counting sightings. The count is
            // advisory; losing the reason to panic again is the better trade.
            Err(poisoned) => poisoned.into_inner(),
        };
        let entry = guard.entry(id.to_string()).or_insert(0);
        *entry = entry.saturating_add(1);
        *entry
    }

    /// How many times this gate has been observed parked.
    pub fn count(&self, id: &GateId) -> u32 {
        match self.seen.lock() {
            Ok(guard) => guard.get(&id.to_string()).copied().unwrap_or(0),
            Err(poisoned) => poisoned
                .into_inner()
                .get(&id.to_string())
                .copied()
                .unwrap_or(0),
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
    let page = runs.pending_gates(cancel).await?;
    let mut gates = page.rows;
    let mut cache: Vec<(String, Vec<TaskState>)> = Vec::new();
    for gate in &mut gates {
        let tasks = tasks_for(runs, &mut cache, &gate.dag_id, &gate.run_id, cancel).await;
        gate.ready = is_ready(gate, &tasks);
    }
    Ok(GateList {
        gates,
        truncated: page.truncated,
    })
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
    /// How long to wait for the confirming second sighting. `None` uses [`CONFIRM_INTERVAL`].
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

    let mut count = if ready { sightings.record(id) } else { 0 };
    if !ready && !opts.force {
        return Err(OpsError::operational(format!(
            "{id} is not ready: its task instance is {state:?}, not awaiting_input. \
             Answering now would make the scheduler fail the gate"
        ))
        .with_hint("wait for the task to park, or pass --force if you accept the risk"));
    }

    if ready && count < REQUIRED_SIGHTINGS && !opts.force {
        // The second sighting is the whole point: a detail can exist a beat before its task
        // defers, and one poll cannot tell the two apart.
        let delay = opts.confirm_delay.unwrap_or(CONFIRM_INTERVAL);
        if !delay.is_zero() {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(OpsError::cancelled()),
                () = tokio::time::sleep(delay) => {}
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
                "{id} stopped being ready while it was being confirmed (now {state:?})"
            )));
        }
        count = sightings.record(id);
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
        assert_eq!(seen.record(&id), 1);
        assert_eq!(seen.record(&id), 2);
        assert_eq!(seen.count(&id), 2);
        seen.forget(&id);
        assert_eq!(seen.count(&id), 0);
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
