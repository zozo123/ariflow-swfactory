//! What a script reads.
//!
//! These documents are public surface under the project's semver policy (`00-architecture.md`
//! §D-E), so they are written out key by key rather than derived from whatever `#[derive(Serialize)]`
//! happens to produce today: an internal field rename must not silently become a breaking change
//! for somebody's `jq` filter. Every document is built here so the whole machine-readable surface
//! is one file, and the boundary this module defends is that a command never invents a second
//! spelling of a fact it already renders for a human.
//!
//! Two shapes recur and are deliberate. A listing is a bare **array**, never an object wrapping
//! one, so it can be streamed and indexed; anything that had to be said *about* the listing —
//! that it was truncated, that a source was unavailable — goes to stderr, where it cannot
//! change the document's type.

use serde_json::{json, Map, Value};
use swf_app::context::{show_json, Context};
use swf_app::gates::{GateAnswer, GateReview};
use swf_app::logs::LogStream;
use swf_domain::evidence::DeliveryReport;
use swf_domain::model::{iso_opt, Gate, JobRow, Run};
use swf_domain::rollup::stage_progress;

/// One environment, with every credential redacted to the name of its variable.
pub fn context(ctx: &Context, active: bool) -> Value {
    let mut doc = show_json(ctx);
    if let Some(map) = doc.as_object_mut() {
        map.insert("active".into(), Value::Bool(active));
    }
    doc
}

/// One DAG run, without its jobs.
pub fn run_row(run: &Run) -> Value {
    json!({
        "id": format!("{}/{}", run.dag_id, run.run_id),
        "dag_id": run.dag_id,
        "run_id": run.run_id,
        "state": run.state,
        "start": iso_opt(run.start),
        "end": iso_opt(run.end),
        "issues": run.issues(),
        "jobs": run.jobs.len(),
    })
}

/// One DAG run and every job it fanned out into.
pub fn run_detail(run: &Run, url: &str) -> Value {
    json!({
        "id": format!("{}/{}", run.dag_id, run.run_id),
        "dag_id": run.dag_id,
        "run_id": run.run_id,
        "state": run.state,
        "start": iso_opt(run.start),
        "end": iso_opt(run.end),
        "issues": run.issues(),
        "url": url,
        "jobs": run.jobs.iter().map(job_row).collect::<Vec<_>>(),
    })
}

/// One mapped job. `id` is the complete identity — never a table row index.
pub fn job_row(job: &JobRow) -> Value {
    json!({
        "id": job.id().to_string(),
        "dag_id": job.dag_id,
        "run_id": job.run_id,
        "map_index": job.map_index,
        "issue": job.issue,
        "state": job.state,
        "stage": stage_progress(&job.tasks),
    })
}

/// One job with its task instances and the gates it is waiting on.
pub fn job_detail(job: &JobRow, gates: &[Gate], url: &str) -> Value {
    let mut doc = job_row(job);
    if let Some(map) = doc.as_object_mut() {
        map.insert("url".into(), url.into());
        map.insert(
            "tasks".into(),
            Value::Array(
                job.tasks
                    .iter()
                    .map(|task| {
                        json!({
                            "task_id": task.task_id,
                            "map_index": task.map_index,
                            "state": task.state_or_none(),
                        })
                    })
                    .collect(),
            ),
        );
        map.insert(
            "gates".into(),
            Value::Array(gates.iter().map(gate_row).collect()),
        );
    }
    doc
}

/// One gate. `ready` says whether it can be answered *now* — see `00-architecture.md` §C.3.
pub fn gate_row(gate: &Gate) -> Value {
    json!({
        "id": gate.id().to_string(),
        "job": gate.job().to_string(),
        "dag_id": gate.dag_id,
        "run_id": gate.run_id,
        "map_index": gate.map_index,
        "task_id": gate.task_id,
        "gate": gate.short_name(),
        "subject": swf_domain::sanitize::sanitize_line(&gate.subject),
        "options": gate.options,
        "created_at": iso_opt(gate.created_at),
        "ready": gate.ready,
    })
}

/// One gate with the evidence and the revision `--expect` takes.
pub fn gate_review(review: &GateReview) -> Value {
    let mut doc = gate_row(&review.gate);
    if let Some(map) = doc.as_object_mut() {
        map.insert("body".into(), review.gate.body.clone().into());
        map.insert("revision".into(), review.revision.clone().into());
        map.insert("task_state".into(), review.task_state.clone().into());
        map.insert("job_state".into(), review.job_state.clone().into());
        map.insert("stage".into(), review.stage.clone().into());
        map.insert("url".into(), review.url.clone().into());
    }
    doc
}

/// What was written, and on whose authority Airflow will record it.
pub fn gate_answer(answer: &GateAnswer, actor: &str) -> Value {
    json!({
        "id": answer.id.to_string(),
        "job": answer.id.job.to_string(),
        "gate": answer.id.short_name(),
        "decision": crate::render::verb(answer.decision),
        "chosen_option": answer.decision.word(),
        "answered": true,
        "actor": actor,
        "revision": answer.revision,
        "forced": answer.forced,
        "sightings": answer.sightings,
    })
}

/// One verification, with the three claims kept apart (non-negotiable 10).
///
/// `verdicts` is a nested object rather than three top-level booleans because a consumer that
/// wants "is this proven?" must have to name which of the three it means.
pub fn verification(report: &DeliveryReport) -> Value {
    let delivery = &report.delivery;
    let job = if delivery.issue_id.is_empty() || delivery.run_id.is_empty() {
        delivery.branch.clone()
    } else {
        format!("{}@{}", delivery.issue_id, delivery.run_id)
    };
    let mut doc = Map::new();
    doc.insert("job".into(), job.into());
    doc.insert("branch".into(), delivery.branch.clone().into());
    doc.insert("issue_id".into(), delivery.issue_id.clone().into());
    doc.insert("run_id".into(), delivery.run_id.clone().into());
    doc.insert("pr_url".into(), delivery.pr_url.clone().into());
    doc.insert("head_sha".into(), delivery.head_sha.clone().into());
    doc.insert("verdict".into(), report.verdict.as_str().into());
    doc.insert("attained".into(), report.attained.as_str().into());
    doc.insert(
        "verdicts".into(),
        json!({
            "workflow_succeeded": report.workflow_succeeded,
            "branch_published": report.branch_published,
            "independently_verified": report.independently_verified,
        }),
    );
    doc.insert("tests".into(), crate::render::tests_word(report).into());
    doc.insert("why".into(), crate::render::why_not(report).into());
    doc.insert(
        "checks".into(),
        serde_json::to_value(&report.checks).unwrap_or(Value::Null),
    );
    doc.insert(
        "test_run".into(),
        serde_json::to_value(&report.tests).unwrap_or(Value::Null),
    );
    Value::Object(doc)
}

/// What `swf runs stop` actually did — which is less than the verb suggests.
///
/// The two false booleans are not decoration. Airflow offers no way to stop a task, so a document
/// that only said `"stopped": true` would be read as "the agent has been halted and the sandbox is
/// gone", and neither is true (non-negotiable 9).
pub fn stopped(dag_id: &str, run_id: &str) -> Value {
    json!({
        "dag_id": dag_id,
        "run_id": run_id,
        "action": "marked_failed",
        "run_marked_failed": true,
        "processes_stopped": false,
        "sandboxes_removed": false,
        "note": "the Airflow run is marked failed; tasks already running keep running \
                 and no sandbox was removed",
    })
}

/// One task attempt's log, as one document.
pub fn logs(job: &str, stream: &LogStream) -> Value {
    json!({
        "job": job,
        "task": stream.task,
        "attempt": stream.attempt,
        "complete": stream.complete(),
        "lines": stream.lines,
    })
}

/// The version, for a packaging script that would otherwise parse `--version`.
pub fn version() -> Value {
    json!({
        "name": "swf",
        "version": env!("CARGO_PKG_VERSION"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use swf_domain::model::TaskState;

    #[test]
    fn a_job_document_carries_the_whole_identity() {
        // `(context, dag_id, run_id, map_index)` is the unit; an index into a table is not an
        // identity, and the e2e reads `id` back out of this document to address the job again.
        let mut job = JobRow::new("factory", "manual__2026:00+00", 3);
        job.issue = "42".into();
        job.state = "running".into();
        job.tasks = vec![TaskState::new(
            "job.build_and_test",
            3,
            Some("running".into()),
        )];
        let doc = job_row(&job);
        assert_eq!(doc["id"], "factory/manual__2026:00+00#3");
        assert_eq!(doc["map_index"], 3);
        assert_eq!(doc["stage"], "build_and_test");
    }

    #[test]
    fn a_gate_document_says_whether_it_can_be_answered_yet() {
        let mut gate = Gate::new(
            "factory",
            "manual__1",
            "job.approve_plan",
            0,
            "plan ready",
            "body",
            None,
            vec!["Approve".into(), "Reject".into()],
        );
        gate.ready = true;
        let doc = gate_row(&gate);
        assert_eq!(doc["id"], "factory/manual__1#0:job.approve_plan");
        assert_eq!(doc["dag_id"], "factory");
        assert_eq!(doc["run_id"], "manual__1");
        assert_eq!(doc["ready"], true);
        assert_eq!(doc["gate"], "approve_plan");
    }

    #[test]
    fn a_gate_subject_cannot_carry_an_escape_sequence_into_a_terminal() {
        let gate = Gate::new(
            "factory",
            "manual__1",
            "job.approve_plan",
            0,
            "plan\u{1b}[2Jready",
            "",
            None,
            vec![],
        );
        assert_eq!(gate_row(&gate)["subject"], "planready");
    }

    #[test]
    fn stopping_a_run_never_claims_more_than_airflow_did() {
        let doc = stopped("factory", "manual__1");
        assert_eq!(doc["run_marked_failed"], true);
        assert_eq!(doc["processes_stopped"], false);
        assert_eq!(doc["sandboxes_removed"], false);
        assert!(doc["note"]
            .as_str()
            .unwrap_or_default()
            .contains("keep running"));
    }

    #[test]
    fn the_version_document_matches_the_one_product_version() {
        assert_eq!(version()["version"], env!("CARGO_PKG_VERSION"));
    }
}
