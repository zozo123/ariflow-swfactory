//! The commands that talk to Airflow, against a server that behaves like Airflow.
//!
//! `tests/cli.rs` proves the process contract with nothing listening; this file proves the other
//! half — that a real status code arrives at the shell as the right number and at a pipe as the
//! right document. The server is `wiremock` rather than a mocked client because the failures being
//! guarded against live in URL building and status classification, which a mocked client skips.

use std::path::Path;
use std::process::Output;

use serde_json::{json, Value};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// A collection route that answers its rows once and then an empty page.
///
/// Pagination's real terminator is an empty page (`00-architecture.md` non-negotiable 3), so a
/// mock that answered the same rows at every offset would spin the client to its page cap and
/// make this file's failures look like the client's.
struct Once {
    key: &'static str,
    rows: Vec<Value>,
}

impl Respond for Once {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let offset: usize = request
            .url
            .query_pairs()
            .find(|(k, _)| k == "offset")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        let rows: Vec<Value> = if offset == 0 {
            self.rows.clone()
        } else {
            Vec::new()
        };
        ResponseTemplate::new(200).set_body_json(json!({
            self.key: rows,
            "total_entries": self.rows.len(),
        }))
    }
}

/// Mount one collection route.
async fn collection(server: &MockServer, route: &str, key: &'static str, rows: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path(route.to_string()))
        .respond_with(Once { key, rows })
        .mount(server)
        .await;
}

/// Mount one route that answers a fixed status and body.
async fn fixed(server: &MockServer, verb: &str, route: &str, status: u16, body: Value) {
    Mock::given(method(verb))
        .and(path(route.to_string()))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(server)
        .await;
}

/// One run, as `DAGRunResponse` spells it.
fn dag_run(run_id: &str, state: &str) -> Value {
    json!({
        "dag_id": "factory",
        "dag_run_id": run_id,
        "state": state,
        "start_date": "2026-09-06T12:00:00+00:00",
        "end_date": Value::Null,
        "conf": {"issues": ["demo/issue.md"]},
    })
}

/// One task instance.
fn task(task_id: &str, map_index: i32, state: &str) -> Value {
    json!({"task_id": task_id, "map_index": map_index, "state": state})
}

/// One HITL detail still waiting for an answer.
fn hitl(run_id: &str, task_id: &str, map_index: i32) -> Value {
    json!({
        "subject": "plan ready for review",
        "body": "the plan is in the artifact",
        "options": ["Approve", "Reject"],
        "created_at": "2026-09-06T12:01:00+00:00",
        "response_received": false,
        "task_instance": {
            "dag_id": "factory",
            "dag_run_id": run_id,
            "task_id": task_id,
            "map_index": map_index,
        },
    })
}

/// A home whose only context points at `url` and reads exactly one DAG.
///
/// Naming the DAG keeps the test off the `/dags` discovery route: what is being asserted here is
/// what a command does with an answer, not how it found one.
fn home_for(url: &str) -> TempDir {
    let home = TempDir::new().expect("tempdir");
    run(
        &home,
        &[
            "context",
            "add",
            "t",
            "--airflow-url",
            url,
            "--dag",
            "factory",
            "--use",
        ],
    );
    home
}

/// Run `swf` with this home's config and no inherited environment surprises.
fn run(home: &TempDir, args: &[&str]) -> Output {
    let mut cmd = assert_cmd::Command::cargo_bin("swf").expect("the binary builds");
    cmd.env("SWF_CONFIG", home.path().join("config.toml"))
        .env_remove("SWF_CONTEXT")
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("NO_COLOR")
        .env("SWF_BLUEPRINTS_DIR", nowhere())
        .args(args);
    cmd.output().expect("run")
}

/// A directory that exists and holds no blueprints, so resolution is deterministic.
fn nowhere() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .display()
        .to_string()
}

/// stdout, parsed as the one document `--json` promised.
fn document(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|err| {
        panic!(
            "stdout was not one JSON document ({err}): {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_listing_carries_the_identity_a_later_command_is_given() {
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns",
        "dag_runs",
        vec![dag_run("manual__2026-09-06T12:00:00+00:00", "running")],
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["runs", "list", "--dag", "factory", "--json"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let doc = document(&out);
    assert_eq!(doc[0]["id"], "factory/manual__2026-09-06T12:00:00+00:00");
    assert_eq!(doc[0]["state"], "running");
    assert_eq!(doc[0]["issues"][0], "demo/issue.md");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_is_not_there_is_not_found_and_not_an_empty_success() {
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns",
        "dag_runs",
        vec![dag_run("r1", "success")],
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["runs", "inspect", "factory/nope", "--json"]);
    assert_eq!(out.status.code(), Some(3), "{out:?}");
    assert_eq!(document(&out)["error"]["kind"], "not_found");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dag_the_server_does_not_know_is_not_found() {
    let server = MockServer::start().await;
    fixed(
        &server,
        "GET",
        "/api/v2/dags/ghost/dagRuns",
        404,
        json!({"detail": "DAG with dag_id: 'ghost' not found"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["runs", "list", "--dag", "ghost", "--json"]);
    assert_eq!(out.status.code(), Some(3), "{out:?}");
    assert_eq!(document(&out)["error"]["kind"], "not_found");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_credential_is_told_apart_from_a_dead_server() {
    // Exit 4 and exit 5 are different instructions to the operator: mint a token, or check the
    // URL. Collapsing them would make every failure a retry.
    let server = MockServer::start().await;
    fixed(
        &server,
        "GET",
        "/api/v2/dags/factory/dagRuns",
        401,
        json!({"detail": "Not authenticated"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["runs", "list", "--dag", "factory", "--json"]);
    assert_eq!(out.status.code(), Some(4), "{out:?}");
    assert_eq!(document(&out)["error"]["kind"], "auth");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gate_listing_says_which_gates_can_be_answered_yet() {
    // The harness reads exactly these four keys off every row, and answers only the ready ones:
    // a HITL detail exists a beat before its task defers, and answering inside that window makes
    // the scheduler fail the gate (§C.3).
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/~/dagRuns/~/hitlDetails",
        "hitl_details",
        vec![
            hitl("r1", "job.approve_intent", 0),
            hitl("r1", "job.approve_plan", 1),
        ],
    )
    .await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns/r1/taskInstances",
        "task_instances",
        vec![
            task("job.approve_intent", 0, "awaiting_input"),
            task("job.approve_plan", 1, "scheduled"),
        ],
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["gates", "list", "--json"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let doc = document(&out);
    let rows = doc.as_array().expect("an array of gates");
    assert_eq!(rows.len(), 2);
    for row in rows {
        for key in ["id", "dag_id", "run_id", "ready"] {
            assert!(row.get(key).is_some(), "gate row is missing {key}: {row}");
        }
        assert_eq!(row["dag_id"], "factory");
        assert_eq!(row["run_id"], "r1");
    }
    assert_eq!(rows[0]["id"], "factory/r1#0:job.approve_intent");
    assert_eq!(rows[0]["ready"], true, "a parked gate is answerable");
    assert_eq!(
        rows[1]["ready"], false,
        "a gate whose task has not parked is not"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gate_someone_else_answered_first_is_a_conflict_and_not_a_crash() {
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/~/dagRuns/~/hitlDetails",
        "hitl_details",
        vec![hitl("r1", "job.approve_intent", 0)],
    )
    .await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns/r1/taskInstances",
        "task_instances",
        vec![task("job.approve_intent", 0, "awaiting_input")],
    )
    .await;
    fixed(
        &server,
        "PATCH",
        "/api/v2/dags/factory/dagRuns/r1/taskInstances/job.approve_intent/0/hitlDetails",
        409,
        json!({"detail": "Human-in-the-loop detail has already been updated"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(
        &home,
        &["gates", "approve", "factory/r1#0:intent", "--yes", "--json"],
    );
    assert_eq!(out.status.code(), Some(6), "{out:?}");
    let doc = document(&out);
    assert_eq!(doc["error"]["kind"], "conflict");
    assert_eq!(doc["error"]["exit_code"], 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gate_review_prints_the_revision_an_answer_can_be_pinned_to() {
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/~/dagRuns/~/hitlDetails",
        "hitl_details",
        vec![hitl("r1", "job.approve_intent", 0)],
    )
    .await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns/r1/taskInstances",
        "task_instances",
        vec![task("job.approve_intent", 0, "awaiting_input")],
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["gates", "review", "factory/r1#0:intent"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(text.contains("plan ready for review"), "{text}");
    assert!(text.contains("revision"), "{text}");
    assert!(text.contains("ready"), "{text}");

    let out = run(&home, &["gates", "review", "factory/r1#0:intent", "--json"]);
    let doc = document(&out);
    assert!(doc["revision"].as_str().is_some_and(|r| r.len() == 16));
    assert_eq!(doc["task_state"], "awaiting_input");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gate_that_is_no_longer_pending_is_reported_by_name() {
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/~/dagRuns/~/hitlDetails",
        "hitl_details",
        vec![],
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["gates", "review", "factory/r1#0:intent", "--json"]);
    assert_eq!(out.status.code(), Some(3), "{out:?}");
    assert_eq!(document(&out)["error"]["kind"], "not_found");
}

#[tokio::test(flavor = "multi_thread")]
async fn submitting_answers_the_run_id_the_server_assigned() {
    let server = MockServer::start().await;
    fixed(
        &server,
        "POST",
        "/api/v2/dags/factory/dagRuns",
        200,
        json!({"dag_id": "factory", "dag_run_id": "manual__2026", "state": "queued"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(
        &home,
        &[
            "submit",
            "--blueprint",
            "factory",
            "--issue",
            "42",
            "--json",
        ],
    );
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let doc = document(&out);
    assert_eq!(doc["run_id"], "manual__2026");
    assert_eq!(doc["dag_id"], "factory");
    assert_eq!(doc["issues"][0], "42");
    // A blueprint that is not on this laptop is the normal case for a remote operator.
    assert_eq!(doc["blueprint"]["resolved"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_run_id_is_a_conflict_the_operator_can_act_on() {
    let server = MockServer::start().await;
    fixed(
        &server,
        "POST",
        "/api/v2/dags/factory/dagRuns",
        409,
        json!({"detail": "DAGRun with dag_id: 'factory' already exists"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["submit", "--issue", "42", "--json"]);
    assert_eq!(out.status.code(), Some(6), "{out:?}");
    assert_eq!(document(&out)["error"]["kind"], "conflict");
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_a_run_says_exactly_what_it_did_and_what_it_did_not() {
    // Non-negotiable 9. The output is the only place an operator learns that their sandbox is
    // still up and their agent is still working.
    let server = MockServer::start().await;
    fixed(
        &server,
        "PATCH",
        "/api/v2/dags/factory/dagRuns/r1",
        200,
        json!({"state": "failed"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["runs", "stop", "factory/r1", "--yes"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let text = String::from_utf8_lossy(&out.stdout).to_lowercase();
    assert!(text.contains("marked the airflow run"), "{text}");
    assert!(text.contains("keep running"), "{text}");
    assert!(text.contains("no sandbox was removed"), "{text}");

    let out = run(&home, &["runs", "stop", "factory/r1", "--yes", "--json"]);
    let doc = document(&out);
    assert_eq!(doc["run_marked_failed"], true);
    assert_eq!(doc["processes_stopped"], false);
    assert_eq!(doc["sandboxes_removed"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn unpausing_a_dag_is_one_call_and_says_so() {
    let server = MockServer::start().await;
    fixed(
        &server,
        "PATCH",
        "/api/v2/dags/factory",
        200,
        json!({"is_paused": false}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["runs", "unpause", "factory", "--json"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let doc = document(&out);
    assert_eq!(doc["dag_id"], "factory");
    assert_eq!(doc["paused"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_listing_names_every_job_by_its_whole_identity() {
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns",
        "dag_runs",
        vec![dag_run("r1", "running")],
    )
    .await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns/r1/taskInstances",
        "task_instances",
        vec![
            task("fan_out", -1, "success"),
            task("job.setup", 0, "success"),
            task("job.build_and_test", 0, "running"),
        ],
    )
    .await;
    collection(
        &server,
        "/api/v2/dags/~/dagRuns/~/hitlDetails",
        "hitl_details",
        vec![],
    )
    .await;
    fixed(
        &server,
        "GET",
        "/api/v2/dags/factory/dagRuns/r1/taskInstances/fan_out/xcomEntries/return_value",
        404,
        json!({"detail": "XCom entry not found"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["jobs", "list", "--json"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let doc = document(&out);
    let rows = doc.as_array().expect("an array of jobs");
    assert_eq!(rows.len(), 1, "{doc}");
    assert_eq!(rows[0]["id"], "factory/r1#0");
    assert_eq!(rows[0]["stage"], "build_and_test");
    assert_eq!(rows[0]["issue"], "demo/issue.md");

    let out = run(&home, &["jobs", "inspect", "factory/r1#0", "--json"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let doc = document(&out);
    assert_eq!(doc["id"], "factory/r1#0");
    assert_eq!(doc["tasks"].as_array().map(Vec::len), Some(2));

    let out = run(&home, &["jobs", "inspect", "factory/r1#7", "--json"]);
    assert_eq!(out.status.code(), Some(3), "{out:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_task_attempt_is_read_and_printed_as_lines() {
    let server = MockServer::start().await;
    fixed(
        &server,
        "GET",
        "/api/v2/dags/factory/dagRuns/r1/taskInstances/job.setup/logs/1",
        200,
        json!({
            "content": [
                {"event": "starting", "timestamp": "2026-09-06T12:00:00+00:00"},
                {"event": "done"}
            ],
            "continuation_token": Value::Null,
        }),
    )
    .await;
    let home = home_for(&server.uri());

    // `--task setup` is what the operator sees on their screen; `job.setup` is what Airflow
    // named it, and the mapped job settles which was meant.
    let out = run(&home, &["logs", "factory/r1#0", "--task", "setup"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(text.contains("starting"), "{text}");

    let out = run(
        &home,
        &["logs", "factory/r1#0", "--task", "setup", "--json"],
    );
    let doc = document(&out);
    assert_eq!(doc["job"], "factory/r1#0");
    assert_eq!(doc["task"], "job.setup");
    assert_eq!(doc["complete"], true);
    assert_eq!(doc["lines"].as_array().map(Vec::len), Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_snapshot_is_the_document_the_python_control_room_prints() {
    let server = MockServer::start().await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns",
        "dag_runs",
        vec![dag_run("r1", "running")],
    )
    .await;
    collection(
        &server,
        "/api/v2/dags/factory/dagRuns/r1/taskInstances",
        "task_instances",
        vec![task("job.setup", 0, "success")],
    )
    .await;
    collection(
        &server,
        "/api/v2/dags/~/dagRuns/~/hitlDetails",
        "hitl_details",
        vec![hitl("r1", "job.approve_intent", 0)],
    )
    .await;
    fixed(
        &server,
        "GET",
        "/api/v2/dags/factory/dagRuns/r1/taskInstances/fan_out/xcomEntries/return_value",
        404,
        json!({"detail": "not found"}),
    )
    .await;
    let home = home_for(&server.uri());

    let out = run(&home, &["snapshot", "--json"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let doc = document(&out);
    // Key order is part of the byte-diff target, not an implementation detail (`02` §3), so it
    // is asserted on the bytes: parsing them would sort the keys and prove nothing.
    let raw = String::from_utf8_lossy(&out.stdout).to_string();
    let mut at = 0usize;
    for key in [
        "collected_at",
        "runs",
        "gates",
        "prs",
        "sandboxes",
        "metrics",
        "errors",
    ] {
        let found = raw
            .find(&format!("\"{key}\""))
            .unwrap_or_else(|| panic!("snapshot is missing {key}:\n{raw}"));
        assert!(found > at, "{key} is out of order in:\n{raw}");
        at = found;
    }
    assert!(raw.starts_with('{') && raw.trim_end().ends_with('}'));
    assert_eq!(doc["runs"][0]["run_id"], "r1");
    assert_eq!(doc["runs"][0]["jobs"][0]["stage"], "setup");
    assert_eq!(doc["gates"][0]["gate"], "approve_intent");

    let out = run(&home, &["snapshot"]);
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(text.starts_with("collected "), "{text}");
    assert!(text.contains("run  factory/r1 running"), "{text}");
}
