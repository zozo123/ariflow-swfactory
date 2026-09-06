//! What needs a human, and the identity to act on it with.
//!
//! Everything else in the product answers "what is happening". This module answers the only
//! question an operator actually opens the tool with: *is anything waiting for me?* So the
//! boundary it defends is the one between noise and a decision. A row appears here only when a
//! person can do something about it, and every row carries the exact identity the corresponding
//! command takes — `dag/run#index:gate` for an approval, `dag/run#index` for a failure, a PR
//! number for a delivery, a name for a sandbox. A dashboard that says "3 problems" without saying
//! which three is a dashboard people learn to close.
//!
//! It is computed from a [`Snapshot`] rather than from its own reads, so what an operator is told
//! needs attention is exactly what the tables beside it are showing — and the sources that failed
//! travel along, because "nothing needs attention" from a pass that could not reach GitHub is a
//! sentence this module must never say.

use chrono::{DateTime, Utc};
use serde::Serialize;
use swf_domain::ids::{GateId, JobId};
use swf_domain::model::{
    is_factory_name, Gate, JobRow, PullRequest, SandboxRef, Snapshot, SourceError,
};
use swf_domain::rollup::{age, stage_progress};
use swf_domain::sanitize::sanitize_line;
use swf_domain::states::is_failed;

/// The labels a delivery carries when the factory stopped it on purpose.
pub const BLOCKED_LABELS: &[&str] = &["factory:blocked", "factory:rejected"];

/// The title banners that say the same thing (`06-delivery-evidence.md` §1.3).
pub const BLOCKED_BANNERS: &[&str] = &["[BLOCKED] ", "[REJECTED] "];

/// How long a factory sandbox may outlive its run before it is worth a human's attention.
///
/// Six hours, because the longest shipped gate timeout is measured in hours and a MicroVM that
/// outlives its job is money. It is a prompt to look, never an instruction to delete: nothing in
/// this module removes anything.
pub const ORPHAN_AFTER_S: i64 = 6 * 3600;

/// One approval waiting for a person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GateItem {
    /// `dag/run#index:task_id` — what `swf gates approve` takes.
    pub id: String,
    /// The job the gate belongs to.
    pub job: String,
    /// The stage name an operator recognises, e.g. `approve_plan`.
    pub gate: String,
    /// The one-line question, sanitised.
    pub subject: String,
    /// True when the task instance is genuinely parked and the gate can be answered now.
    pub ready: bool,
    /// How long it has been waiting.
    pub age: String,
}

/// One job that stopped on a failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FailedJob {
    /// `dag/run#index` — what `swf jobs inspect` and `swf logs` take.
    pub id: String,
    /// The issue this job answers.
    pub issue: String,
    /// The rolled-up job state.
    pub state: String,
    /// Where in the pipeline it stopped.
    pub stage: String,
}

/// One delivery the factory refused to ship, or that CI refuted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockedDelivery {
    /// The pull request number — what `swf deliveries verify` takes.
    pub number: i64,
    /// The web URL.
    pub url: String,
    /// The title, sanitised.
    pub title: String,
    /// The branch, i.e. `factory/<issue>-<run>`.
    pub branch: String,
    /// Why it is here: `blocked`, `rejected`, or `checks failing`.
    pub reason: String,
    /// The check roll-up, in `summarize_checks` form.
    pub checks: String,
}

/// One sandbox that has outlived whatever created it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrphanSandbox {
    /// The provider's name — what `swf sandboxes rm` takes.
    pub name: String,
    /// The provider's status.
    pub status: String,
    /// How old it is.
    pub age: String,
    /// Why it is being shown.
    pub reason: String,
}

/// Everything waiting for a person, and everything the pass could not see.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Attention {
    /// Approvals. Ready ones first — those are the ones that can be answered right now.
    pub gates: Vec<GateItem>,
    /// Jobs that failed.
    pub failures: Vec<FailedJob>,
    /// Deliveries the factory blocked, rejected, or whose checks are red.
    pub blocked: Vec<BlockedDelivery>,
    /// Sandboxes that outlived their run.
    pub orphans: Vec<OrphanSandbox>,
    /// The sources that failed in this pass. An empty attention list from a broken pass is a lie.
    pub errors: Vec<SourceError>,
}

impl Attention {
    /// How many rows a person is being asked to look at.
    pub fn count(&self) -> usize {
        self.gates.len() + self.failures.len() + self.blocked.len() + self.orphans.len()
    }

    /// True when nothing needs a person *and* every source answered.
    ///
    /// The second half is the point: a quiet screen has to mean "nothing is wrong", not "we could
    /// not tell".
    pub fn is_clear(&self) -> bool {
        self.count() == 0 && self.errors.is_empty()
    }

    /// The gates that can be answered right now.
    pub fn ready_gates(&self) -> impl Iterator<Item = &GateItem> {
        self.gates.iter().filter(|gate| gate.ready)
    }

    /// Read one pass and keep only what a person can act on.
    pub fn from_snapshot(snap: &Snapshot, now: DateTime<Utc>) -> Self {
        let mut gates: Vec<GateItem> = snap.gates.iter().map(|gate| gate_item(gate, now)).collect();
        // Ready first, then oldest first: the gate that can be answered and has been waiting
        // longest is the one to answer.
        gates.sort_by_key(|item| !item.ready);

        let failures = snap
            .runs
            .iter()
            .flat_map(|run| run.jobs.iter())
            .filter(|job| is_failed(&job.state))
            .map(failed_job)
            .collect();

        let blocked = snap.prs.iter().filter_map(blocked_delivery).collect();

        let has_active_run = snap.runs.iter().any(swf_domain::model::Run::active);
        let orphans = snap
            .sandboxes
            .iter()
            .filter_map(|sandbox| orphan(sandbox, now, has_active_run))
            .collect();

        Self {
            gates,
            failures,
            blocked,
            orphans,
            errors: snap.errors.clone(),
        }
    }
}

/// One gate, as a row somebody can act on.
fn gate_item(gate: &Gate, now: DateTime<Utc>) -> GateItem {
    let id = GateId::new(gate.job(), gate.task_id.clone());
    GateItem {
        id: id.to_string(),
        job: gate.job().to_string(),
        gate: gate.short_name().to_string(),
        subject: sanitize_line(&gate.subject),
        ready: gate.ready,
        age: age(gate.created_at.map(|at| at.with_timezone(&Utc)), now),
    }
}

/// One failed job, as a row somebody can act on.
fn failed_job(job: &JobRow) -> FailedJob {
    let id = JobId::new(job.dag_id.clone(), job.run_id.clone(), job.map_index);
    FailedJob {
        id: id.to_string(),
        issue: job.issue.clone(),
        state: job.state.clone(),
        stage: stage_progress(&job.tasks),
    }
}

/// One delivery, if there is anything wrong with it.
///
/// A `[BLOCKED]` delivery is a *correct* outcome of the process, not a bug — but it is still
/// waiting for a person to decide what happens next, which is exactly what this list is for.
fn blocked_delivery(pr: &PullRequest) -> Option<BlockedDelivery> {
    let label = BLOCKED_LABELS
        .iter()
        .find(|label| pr.labels.iter().any(|have| have == *label))
        .map(|label| label.trim_start_matches("factory:").to_string());
    let banner = BLOCKED_BANNERS
        .iter()
        .find(|banner| pr.title.starts_with(**banner))
        .map(|banner| banner.trim().trim_matches(['[', ']']).to_lowercase());
    let failing = failing_checks(&pr.checks);

    let reason = match (label.or(banner), failing) {
        (Some(reason), true) => format!("{reason}, checks failing"),
        (Some(reason), false) => reason,
        (None, true) => "checks failing".to_string(),
        (None, false) => return None,
    };
    Some(BlockedDelivery {
        number: pr.number,
        url: pr.url.clone(),
        title: sanitize_line(&pr.title),
        branch: pr.head.clone(),
        reason,
        checks: pr.checks.clone(),
    })
}

/// True when a `summarize_checks` string reports at least one failure.
///
/// It is parsed rather than recomputed because the roll-up already resolved every doubt toward
/// *pending*, and re-deciding here would be a second opinion nobody asked for.
fn failing_checks(summary: &str) -> bool {
    summary
        .split('/')
        .filter_map(|part| {
            let part = part.trim();
            part.strip_suffix(" fail")?.trim().parse::<u32>().ok()
        })
        .any(|failed| failed > 0)
}

/// One sandbox, if it looks like it outlived its job.
///
/// Only factory-named sandboxes are ever listed. A sandbox this tool did not create is somebody
/// else's, and putting it on a list headed "needs attention" next to a `swf sandboxes rm` command
/// is how a teammate's work gets deleted.
fn orphan(sandbox: &SandboxRef, now: DateTime<Utc>, has_active_run: bool) -> Option<OrphanSandbox> {
    if !is_factory_name(&sandbox.name) || sandbox.status.eq_ignore_ascii_case("deleted") {
        return None;
    }
    let created = sandbox.created_at.map(|at| at.with_timezone(&Utc));
    let seconds = created.map(|at| (now - at).num_seconds()).unwrap_or(0);
    let reason = if seconds >= ORPHAN_AFTER_S {
        format!("running for {}h with no run to finish", seconds / 3600)
    } else if !has_active_run {
        "no run is active".to_string()
    } else {
        return None;
    };
    Some(OrphanSandbox {
        name: sandbox.name.clone(),
        status: sandbox.status.clone(),
        age: age(created, now),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use swf_domain::model::{Run, TaskState};

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 6, 12, 0, 0)
            .single()
            .unwrap_or_default()
    }

    fn snapshot() -> Snapshot {
        Snapshot::new(now())
    }

    #[test]
    fn a_ready_gate_sorts_above_one_that_cannot_be_answered_yet() {
        let mut snap = snapshot();
        let mut waiting = Gate::new(
            "factory",
            "r1",
            "job.approve_plan",
            0,
            "Plan?",
            "b",
            None,
            vec![],
        );
        waiting.ready = false;
        let mut ready = Gate::new(
            "factory",
            "r1",
            "job.approve_intent",
            1,
            "Intent?",
            "b",
            None,
            vec![],
        );
        ready.ready = true;
        snap.gates = vec![waiting, ready];

        let attention = Attention::from_snapshot(&snap, now());
        assert_eq!(attention.gates[0].gate, "approve_intent");
        assert_eq!(attention.gates[0].id, "factory/r1#1:job.approve_intent");
        assert_eq!(attention.ready_gates().count(), 1);
        assert_eq!(attention.count(), 2);
    }

    #[test]
    fn a_gate_subject_cannot_repaint_the_terminal() {
        let mut snap = snapshot();
        snap.gates = vec![Gate::new(
            "factory",
            "r1",
            "job.approve_plan",
            0,
            "Approve\u{1b}[31m this?",
            "body",
            None,
            vec![],
        )];
        let attention = Attention::from_snapshot(&snap, now());
        assert_eq!(attention.gates[0].subject, "Approve this?");
    }

    #[test]
    fn only_failed_jobs_are_listed_and_they_carry_their_identity() {
        let mut snap = snapshot();
        let mut run = Run::new("factory", "r1", "running");
        run.jobs = vec![
            JobRow {
                dag_id: "factory".into(),
                run_id: "r1".into(),
                map_index: 0,
                issue: "42".into(),
                state: "failed".into(),
                tasks: vec![TaskState::new(
                    "job.build_and_test",
                    0,
                    Some("failed".into()),
                )],
            },
            JobRow {
                dag_id: "factory".into(),
                run_id: "r1".into(),
                map_index: 1,
                issue: "43".into(),
                state: "running".into(),
                tasks: vec![],
            },
        ];
        snap.runs = vec![run];

        let attention = Attention::from_snapshot(&snap, now());
        assert_eq!(attention.failures.len(), 1);
        assert_eq!(attention.failures[0].id, "factory/r1#0");
        assert_eq!(attention.failures[0].issue, "42");
        assert!(attention.failures[0].stage.contains("build_and_test"));
    }

    #[test]
    fn a_delivery_is_listed_for_a_banner_a_label_or_a_red_check_and_never_otherwise() {
        let base = PullRequest {
            number: 7,
            title: "42: do the thing".into(),
            url: "https://example.com/7".into(),
            labels: vec!["factory".into()],
            state: "OPEN".into(),
            checks: "3 pass / 0 fail / 0 pending".into(),
            head: "factory/42-abcd1234".into(),
        };
        assert!(
            blocked_delivery(&base).is_none(),
            "a healthy PR is not news"
        );

        let mut labelled = base.clone();
        labelled.labels.push("factory:blocked".into());
        assert_eq!(
            blocked_delivery(&labelled).map(|d| d.reason),
            Some("blocked".to_string())
        );

        let mut bannered = base.clone();
        bannered.title = "[REJECTED] 42: do the thing".into();
        assert_eq!(
            blocked_delivery(&bannered).map(|d| d.reason),
            Some("rejected".to_string())
        );

        let mut red = base.clone();
        red.checks = "1 pass / 2 fail / 0 pending".into();
        let row = blocked_delivery(&red).expect("red checks need a person");
        assert_eq!(row.reason, "checks failing");
        assert_eq!(row.number, 7);
        assert_eq!(row.branch, "factory/42-abcd1234");
    }

    #[test]
    fn the_check_summary_is_read_not_re_decided() {
        assert!(failing_checks("0 pass / 1 fail / 0 pending"));
        assert!(!failing_checks("2 pass / 0 fail / 3 pending"));
        assert!(!failing_checks("none"));
        assert!(!failing_checks(""));
    }

    #[test]
    fn a_sandbox_this_tool_did_not_create_is_never_offered_for_removal() {
        let mut snap = snapshot();
        snap.sandboxes = vec![
            SandboxRef::new("prod-db", "running", "someone", None),
            SandboxRef::new("swf-demo-0badf00d", "running", "me", None),
        ];
        let attention = Attention::from_snapshot(&snap, now());
        assert_eq!(attention.orphans.len(), 1);
        assert_eq!(attention.orphans[0].name, "swf-demo-0badf00d");
        assert_eq!(attention.orphans[0].reason, "no run is active");
    }

    #[test]
    fn an_old_sandbox_is_flagged_even_while_a_run_is_active() {
        let created = now() - chrono::Duration::seconds(ORPHAN_AFTER_S + 60);
        let sandbox = SandboxRef::new("swf-demo-0badf00d", "running", "me", Some(created));
        let row = orphan(&sandbox, now(), true).expect("six hours is long enough to ask");
        assert!(row.reason.contains("6h"), "{}", row.reason);
        assert_eq!(row.age, "6h");

        let fresh = SandboxRef::new("swf-demo-0badf00d", "running", "me", Some(now()));
        assert!(orphan(&fresh, now(), true).is_none());
        let deleted = SandboxRef::new("swf-demo-0badf00d", "deleted", "me", Some(created));
        assert!(orphan(&deleted, now(), false).is_none());
    }

    #[test]
    fn a_quiet_screen_from_a_broken_pass_is_never_reported_as_clear() {
        let mut snap = snapshot();
        assert!(Attention::from_snapshot(&snap, now()).is_clear());
        snap.set_error("github", "gh is unreachable");
        let attention = Attention::from_snapshot(&snap, now());
        assert_eq!(attention.count(), 0);
        assert!(
            !attention.is_clear(),
            "we could not tell is not nothing is wrong"
        );
        assert_eq!(attention.errors.len(), 1);
    }
}
