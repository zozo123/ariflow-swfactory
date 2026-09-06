//! The one place the Airflow state vocabulary is written down.
//!
//! `control.py` defines these sets once and `herd.py` imports them from there, precisely so the
//! roll-up and the frontier can never disagree about what "still working" means. This module keeps
//! that property in Rust: the roll-ups, the gate-readiness check and the TUI all ask here, and a
//! new Airflow state is a one-line change with a test, not a hunt through five call sites.

/// A DAG run Airflow has not finished with. Anything else is history.
pub const ACTIVE_RUN_STATES: &[&str] = &["queued", "running"];

/// Task states that mean "this job is still moving".
///
/// `awaiting_input` is in here and that is load-bearing. Airflow 3.3 parks a HITL task in
/// `awaiting_input` (older builds used `deferred`, kept for them). If it were not ACTIVE, a job
/// waiting on a gate would have no active task at all: the roll-up and the herd frontier would
/// fall back to the last *finished* task, and the Runs tab would show `intent` instead of
/// `approve_intent` for exactly the jobs an operator has to act on. That was observed on a live
/// standalone, and it is the reason this list is a constant and not an ad-hoc match arm.
pub const ACTIVE_TASK_STATES: &[&str] = &[
    "running",
    "queued",
    "scheduled",
    "deferred",
    "restarting",
    "awaiting_input",
    "up_for_retry",
    "up_for_reschedule",
];

/// Task states that condemn the whole job. Failure always wins the roll-up.
pub const FAILED_TASK_STATES: &[&str] = &["failed", "upstream_failed"];

/// Task states Airflow will not move away from on its own.
pub const FINAL_TASK_STATES: &[&str] =
    &["failed", "upstream_failed", "success", "skipped", "removed"];

/// What a missing state is called once it enters the roll-up. Airflow reports `null` for a task
/// instance it has created but not scheduled; `job_state` maps that to a word so the set
/// membership tests below have something to test.
pub const NONE_STATE: &str = "none";

/// Where Airflow parks a task that is waiting on a human, newest spelling first.
///
/// A HITL *detail* exists from the moment the operator task creates it — a beat before the task
/// actually defers. Answering inside that window makes the scheduler fail the gate, so
/// `Gate::ready` is true only for a task instance sitting in one of these.
pub const GATE_PARKED_STATES: &[&str] = &["awaiting_input", "deferred"];

/// Normalise Airflow's `state` field the way the Python roll-up does: `None` **and** `""` both
/// become `"none"`, because `t.state or "none"` treats them identically and the fixtures pin it.
pub fn or_none(state: Option<&str>) -> &str {
    match state {
        Some(s) if !s.is_empty() => s,
        _ => NONE_STATE,
    }
}

/// True while Airflow still intends to do something with this task.
pub fn is_active(state: &str) -> bool {
    ACTIVE_TASK_STATES.contains(&state)
}

/// True when this task has failed, directly or because an upstream did.
pub fn is_failed(state: &str) -> bool {
    FAILED_TASK_STATES.contains(&state)
}

/// True when Airflow has stopped moving this task, successfully or not.
pub fn is_final(state: &str) -> bool {
    FINAL_TASK_STATES.contains(&state)
}

/// True when the DAG run itself is still open. Mirrors `Run.active`.
pub fn run_is_active(state: &str) -> bool {
    ACTIVE_RUN_STATES.contains(&state)
}

/// True when a HITL task instance is genuinely parked and safe to answer.
pub fn is_gate_parked(state: &str) -> bool {
    GATE_PARKED_STATES.contains(&state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sets_have_the_sizes_the_python_pins() {
        assert_eq!(ACTIVE_TASK_STATES.len(), 8);
        assert_eq!(FAILED_TASK_STATES.len(), 2);
        assert_eq!(FINAL_TASK_STATES.len(), 5);
        assert_eq!(ACTIVE_RUN_STATES.len(), 2);
    }

    #[test]
    fn awaiting_input_is_active_or_gated_jobs_render_the_wrong_stage() {
        assert!(is_active("awaiting_input"));
        assert!(is_active("deferred"));
        assert!(!is_final("awaiting_input"));
        assert!(!is_failed("awaiting_input"));
    }

    #[test]
    fn failed_states_are_both_failed_and_final() {
        for state in FAILED_TASK_STATES {
            assert!(is_failed(state));
            assert!(is_final(state));
            assert!(!is_active(state));
        }
    }

    #[test]
    fn active_and_final_never_overlap() {
        for state in ACTIVE_TASK_STATES {
            assert!(!is_final(state), "{state} is both active and final");
        }
    }

    #[test]
    fn missing_and_blank_states_become_none() {
        assert_eq!(or_none(None), "none");
        assert_eq!(or_none(Some("")), "none");
        assert_eq!(or_none(Some("running")), "running");
        assert!(!is_active(NONE_STATE));
        assert!(!is_final(NONE_STATE));
    }

    #[test]
    fn unknown_states_are_neither_active_nor_final() {
        assert!(!is_active("banana"));
        assert!(!is_final("banana"));
        assert!(!is_failed("banana"));
    }

    #[test]
    fn only_queued_and_running_runs_are_active() {
        assert!(run_is_active("queued"));
        assert!(run_is_active("running"));
        for state in ["success", "failed", "unknown", ""] {
            assert!(!run_is_active(state));
        }
    }

    #[test]
    fn a_gate_is_answerable_only_where_the_scheduler_parked_it() {
        assert!(is_gate_parked("awaiting_input"));
        assert!(is_gate_parked("deferred"));
        for state in ["scheduled", "queued", "running", NONE_STATE] {
            assert!(
                !is_gate_parked(state),
                "{state} must not read as answerable"
            );
        }
    }
}
