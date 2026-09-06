//! What the Airflow client does when the server behaves the way the real one does.
//!
//! Every case here is one the spec says a reimplementation gets wrong: a `limit` the server
//! silently clamps, a collection that needs three pages, a JWT that expired overnight, a gate two
//! operators answered at once. They are written against a real HTTP server (`wiremock`) rather
//! than a mocked client, because the bugs being guarded against live in URL building and status
//! handling — exactly the layers a mocked client would skip.

use std::sync::Mutex;
use std::time::Duration;

use serde_json::{json, Value};
use swf_adapters::airflow::{AirflowApi, Auth};
use swf_adapters::error::AdapterError;
use swf_adapters::traits::Runs;
use swf_domain::ids::{JobId, RunRef};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{any, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// A responder that walks a script and then repeats its last step forever.
///
/// The alternative — mounting two mocks and hoping the framework picks them in the order the test
/// meant — makes the test's meaning depend on an implementation detail of `wiremock`.
struct Script(Mutex<Vec<(u16, Value)>>);

impl Script {
    fn new(steps: Vec<(u16, Value)>) -> Self {
        assert!(!steps.is_empty(), "a script needs at least one step");
        Self(Mutex::new(steps))
    }
}

impl Respond for Script {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let mut steps = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let (status, body) = if steps.len() > 1 {
            steps.remove(0)
        } else {
            steps[0].clone()
        };
        ResponseTemplate::new(status).set_body_json(body)
    }
}

/// A responder that answers a page of a collection, driven by the `offset` the client sent.
struct Pages {
    key: &'static str,
    total: Option<i64>,
    page: usize,
    rows: usize,
}

impl Respond for Pages {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let offset: usize = request
            .url
            .query_pairs()
            .find(|(k, _)| k == "offset")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        let remaining = self.rows.saturating_sub(offset);
        let count = remaining.min(self.page);
        let rows: Vec<Value> = (offset..offset + count)
            .map(
                |i| json!({"dag_id": "factory", "dag_run_id": format!("r{i}"), "state": "success"}),
            )
            .collect();
        let mut body = json!({ self.key: rows });
        if let Some(total) = self.total {
            body["total_entries"] = json!(total);
        }
        ResponseTemplate::new(200).set_body_json(body)
    }
}

fn client(server: &MockServer) -> AirflowApi {
    AirflowApi::new(&server.uri(), Auth::None, Duration::from_secs(5)).expect("client builds")
}

fn basic(server: &MockServer) -> AirflowApi {
    AirflowApi::new(
        &server.uri(),
        Auth::Basic {
            username: "admin".into(),
            password: "hunter2".into(),
        },
        Duration::from_secs(5),
    )
    .expect("client builds")
}

fn token_of(request: &Request) -> String {
    request
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn a_collection_is_read_across_every_page_it_takes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/dags/factory/dagRuns"))
        .respond_with(Pages {
            key: "dag_runs",
            total: Some(250),
            page: 100,
            rows: 250,
        })
        .mount(&server)
        .await;

    let runs = client(&server)
        .list_runs("factory", 250, &CancellationToken::new())
        .await
        .expect("three pages");

    assert_eq!(runs.len(), 250, "the client asked for all of them");
    assert!(!runs.truncated);
    assert_eq!(runs.rows[0].run_id, "r0");
    assert_eq!(runs.rows[249].run_id, "r249");
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        3,
        "100 + 100 + 50"
    );
}

#[tokio::test]
async fn total_entries_stops_the_loop_without_an_extra_empty_page() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(Pages {
            key: "dag_runs",
            total: Some(100),
            page: 100,
            rows: 100,
        })
        .mount(&server)
        .await;

    let runs = client(&server)
        .list_runs("factory", 10_000, &CancellationToken::new())
        .await
        .expect("one page");

    assert_eq!(runs.len(), 100);
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        1,
        "a full page plus `total_entries == 100` is the end; asking again is a wasted round trip"
    );
}

#[tokio::test]
async fn a_null_total_entries_still_terminates_on_an_empty_page() {
    // On `dagRuns` and `taskInstances` `total_entries` is `int | null` and not in the OpenAPI
    // `required` list. An empty page is the real terminator.
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(Pages {
            key: "task_instances",
            total: None,
            page: 100,
            rows: 150,
        })
        .mount(&server)
        .await;

    let run = RunRef::new("factory", "r1");
    let tasks = client(&server)
        .task_states(&run, &CancellationToken::new())
        .await
        .expect("two full pages and one empty one");

    assert_eq!(tasks.len(), 150);
    assert!(!tasks.truncated);
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        3
    );
}

#[tokio::test]
async fn a_read_that_hits_its_page_bound_says_so_instead_of_hiding_rows() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(Pages {
            key: "task_instances",
            total: Some(1_000_000),
            page: 100,
            rows: 1_000_000,
        })
        .mount(&server)
        .await;

    let tasks = client(&server)
        .task_states(&RunRef::new("factory", "r1"), &CancellationToken::new())
        .await
        .expect("a bounded read");

    assert!(
        tasks.truncated,
        "silently dropping rows is the failure mode this whole binary replaces"
    );
    assert_eq!(tasks.len(), swf_adapters::airflow::MAX_PAGES * 100);
}

#[tokio::test]
async fn an_expired_token_is_re_minted_once_and_the_operator_never_sees_it() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/token"))
        .respond_with(Script::new(vec![
            // The route is decorated 201, and a live probe once recorded 200. Any 2xx is accepted.
            (201, json!({"access_token": "stale"})),
            (200, json!({"access_token": "fresh"})),
        ]))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/dags"))
        .respond_with(move |request: &Request| {
            if token_of(request) == "Bearer fresh" {
                ResponseTemplate::new(200)
                    .set_body_json(json!({"dags": [{"dag_id": "factory"}], "total_entries": 1}))
            } else {
                ResponseTemplate::new(401).set_body_json(json!({"detail": "Token Expired"}))
            }
        })
        .mount(&server)
        .await;

    let dags = basic(&server)
        .list_dags("swfactory", &CancellationToken::new())
        .await
        .expect("the re-mint is invisible");

    assert_eq!(dags.rows, vec!["factory"]);
    let seen = server.received_requests().await.unwrap_or_default();
    assert_eq!(seen.len(), 4, "mint, 401, re-mint, success");
}

#[tokio::test]
async fn a_401_that_stays_a_401_gives_up_rather_than_minting_forever() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/token"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"access_token": "t"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/dags"))
        .respond_with(
            ResponseTemplate::new(401).set_body_json(json!({"detail": "Not authenticated"})),
        )
        .mount(&server)
        .await;

    let err = basic(&server)
        .list_dags("swfactory", &CancellationToken::new())
        .await
        .expect_err("a wrong password is the operator's to read");

    assert_eq!(err.exit_code(), 4);
    assert_eq!(err.kind(), "auth");
    assert!(err.to_string().contains("Not authenticated"), "{err}");
    let seen = server.received_requests().await.unwrap_or_default();
    assert_eq!(
        seen.len(),
        4,
        "exactly two attempts: mint, 401, one re-mint, 401 — never a login flood"
    );
}

#[tokio::test]
async fn a_static_token_that_expired_is_reported_and_not_retried() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"detail": "Token Expired"})))
        .mount(&server)
        .await;

    let api = AirflowApi::new(
        &server.uri(),
        Auth::Token("from-the-environment".into()),
        Duration::from_secs(5),
    )
    .expect("client builds");
    let err = api
        .list_dags("swfactory", &CancellationToken::new())
        .await
        .expect_err("nothing to re-mint with");

    assert_eq!(err.exit_code(), 4);
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        1,
        "there is no fresh credential to be had, so there is nothing to retry"
    );
}

#[tokio::test]
async fn a_403_that_is_not_an_invalid_jwt_is_operational_and_never_re_minted() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/token"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"access_token": "t"})))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(
                json!({"detail": "User=amy (id=7) is not a respondent for the task."}),
            ),
        )
        .mount(&server)
        .await;

    let gate = JobId::new("factory", "r1", 0).gate("job.approve_intent");
    let err = basic(&server)
        .respond(&gate, true, &CancellationToken::new())
        .await
        .expect_err("a permission decision");

    assert_eq!(
        err.exit_code(),
        1,
        "retrying a permission decision is never right"
    );
    assert_eq!(err.kind(), "operational");
    let seen = server.received_requests().await.unwrap_or_default();
    assert_eq!(seen.len(), 2, "mint, then one PATCH — no second attempt");
}

#[tokio::test]
async fn a_missing_run_is_not_found_and_not_a_generic_failure() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "detail": "DagRun with dag_id: `factory` and run_id: `nope` was not found"
        })))
        .mount(&server)
        .await;

    let err = client(&server)
        .task_states(&RunRef::new("factory", "nope"), &CancellationToken::new())
        .await
        .expect_err("no such run");

    assert_eq!(err.exit_code(), 3);
    assert_eq!(err.kind(), "not_found");
    assert!(err.to_string().contains("was not found"), "{err}");
}

#[tokio::test]
async fn a_missing_fan_out_xcom_is_a_state_of_the_world_and_not_an_error() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "detail": "XCom entry with key: `return_value` not found"
        })))
        .mount(&server)
        .await;

    let jobs = client(&server)
        .fan_out_jobs(&RunRef::new("factory", "r1"), &CancellationToken::new())
        .await
        .expect("a run that has not fanned out yet is normal");
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn the_fan_out_xcom_is_parsed_twice_because_the_api_hands_back_a_json_string() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "key": "return_value",
            "value": "[{\"issue\": \"42\"}, 7, {\"issue\": \"43\"}]"
        })))
        .mount(&server)
        .await;

    let jobs = client(&server)
        .fan_out_jobs(&RunRef::new("factory", "r1"), &CancellationToken::new())
        .await
        .expect("a fanned-out run");
    assert_eq!(jobs.len(), 2, "non-object entries are dropped");
    assert_eq!(jobs[0]["issue"], json!("42"));
}

#[tokio::test]
async fn answering_a_gate_someone_else_already_answered_is_a_conflict() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .respond_with(Script::new(vec![
            (
                200,
                json!({"chosen_options": ["Approve"], "params_input": {}}),
            ),
            (
                409,
                json!({"detail": "Human-in-the-loop detail has already been updated for Task \
                                  Instance with id abc and is not allowed to write again."}),
            ),
        ]))
        .mount(&server)
        .await;

    let api = client(&server);
    let gate = JobId::new("factory", "manual__2026-09-03T08:18:24+00:00", 0).gate("intent");
    let cancel = CancellationToken::new();

    api.respond(&gate, true, &cancel)
        .await
        .expect("first answer wins");
    let err = api
        .respond(&gate, true, &cancel)
        .await
        .expect_err("the second is a race, not a crash");

    assert_eq!(err.exit_code(), 6);
    assert_eq!(err.kind(), "conflict");
    assert!(err.to_string().contains("already been updated"), "{err}");
}

#[tokio::test]
async fn a_run_id_reaches_the_server_with_its_colons_and_pluses_encoded() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"task_instances": []})))
        .mount(&server)
        .await;

    let run = RunRef::new("factory", "manual__2026-09-03T08:18:24.904858+00:00");
    client(&server)
        .task_states(&run, &CancellationToken::new())
        .await
        .expect("an empty run");

    let seen = server.received_requests().await.unwrap_or_default();
    let url = seen[0].url.as_str();
    assert!(
        url.contains("%3A"),
        "a raw colon is a different path: {url}"
    );
    assert!(
        url.contains("%2B"),
        "a raw plus decodes to a space and addresses a run that does not exist: {url}"
    );
}

#[tokio::test]
async fn a_body_that_is_not_json_is_a_decode_failure_and_not_a_panic() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>proxy error</html>"))
        .mount(&server)
        .await;

    let err = client(&server)
        .list_dags("swfactory", &CancellationToken::new())
        .await
        .expect_err("a proxy in the way");

    assert!(matches!(err, AdapterError::Decode { .. }), "{err:?}");
    assert_eq!(err.exit_code(), 1);
    assert!(err.to_string().contains("proxy error"), "{err}");
}

#[tokio::test]
async fn an_empty_body_on_a_write_is_a_success_and_not_a_decode_failure() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    client(&server)
        .stop_run(&RunRef::new("factory", "r1"), &CancellationToken::new())
        .await
        .expect("204 with no body is how a write says nothing happened to report");
}

#[tokio::test]
async fn cancelling_mid_flight_abandons_the_request_instead_of_racing_the_next_one() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"dags": [], "total_entries": 0}))
                .set_delay(Duration::from_secs(30)),
        )
        .mount(&server)
        .await;

    let cancel = CancellationToken::new();
    let stopper = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        stopper.cancel();
    });

    let started = std::time::Instant::now();
    let err = client(&server)
        .list_dags("swfactory", &cancel)
        .await
        .expect_err("the operator moved on");

    assert!(err.is_cancelled());
    assert_eq!(err.kind(), "operational", "and never rendered as a failure");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a cancelled call must not wait out the server"
    );
}

#[tokio::test]
async fn a_cancelled_token_costs_no_round_trip_at_all() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"dags": []})))
        .mount(&server)
        .await;

    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = client(&server)
        .list_dags("swfactory", &cancel)
        .await
        .expect_err("cancelled before it began");

    assert!(err.is_cancelled());
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty(),
        "a context switch must not cost a round trip nobody will read"
    );
}

#[tokio::test]
async fn triggering_sends_an_explicit_null_logical_date_and_reads_back_the_run_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/dags/factory/dagRuns"))
        // The trigger route answers 200, not 201 — the API has no 2xx convention to lean on.
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "dag_id": "factory", "dag_run_id": "manual__2026-09-03T08:18:24+00:00"
        })))
        .mount(&server)
        .await;

    let run_id = client(&server)
        .trigger(
            "factory",
            &["  42  ".to_string(), String::new(), "43".to_string()],
            &CancellationToken::new(),
        )
        .await
        .expect("a triggered run");
    assert_eq!(run_id, "manual__2026-09-03T08:18:24+00:00");

    let seen = server.received_requests().await.unwrap_or_default();
    let body: Value = serde_json::from_slice(&seen[0].body).expect("a JSON body");
    assert_eq!(
        body,
        json!({"logical_date": null, "conf": {"issues": ["42", "43"]}}),
        "the key must be present and null, and blank refs are dropped on both sides"
    );
}

#[tokio::test]
async fn a_trigger_with_nothing_to_work_on_is_refused_before_the_request() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let err = client(&server)
        .trigger("factory", &["   ".to_string()], &CancellationToken::new())
        .await
        .expect_err("no issues is not a run");
    assert_eq!(err.exit_code(), 1);
    assert!(server
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty());
}

#[tokio::test]
async fn job_rows_survive_an_xcom_that_is_gone_and_name_the_issue_from_the_run_conf() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/v2/dags/factory/dagRuns/r1/taskInstances/fan_out/xcomEntries/return_value",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"detail": "not found"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/dags/factory/dagRuns/r1/taskInstances"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "task_instances": [
                {"task_id": "fan_out", "map_index": -1, "state": "success"},
                {"task_id": "job.build_and_test", "map_index": 0, "state": "running"}
            ],
            "total_entries": 2
        })))
        .mount(&server)
        .await;

    let rows = client(&server)
        .job_rows(
            &RunRef::new("factory", "r1"),
            &["42".to_string()],
            &CancellationToken::new(),
        )
        .await
        .expect("degradation, not an outage");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows.rows[0].map_index, 0);
    assert_eq!(
        rows.rows[0].issue, "42",
        "the run conf still knows what this is for"
    );
    assert_eq!(rows.rows[0].state, "running");
}

#[tokio::test]
async fn a_log_poll_follows_its_continuation_token_and_stops_when_it_is_null() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(Script::new(vec![
            (
                200,
                json!({
                    "content": [
                        {"event": "::group::Log message source details"},
                        {"timestamp": "2026-09-03T08:00:00Z", "event": "starting"}
                    ],
                    "continuation_token": "opaque-1"
                }),
            ),
            (
                200,
                json!({"content": ["done\u{1b}[2J"], "continuation_token": null}),
            ),
        ]))
        .mount(&server)
        .await;

    let api = client(&server);
    let job = JobId::new("factory", "manual__x+1", 0);
    let cancel = CancellationToken::new();

    let first = api
        .logs(&job, "job.build_and_test", 1, None, &cancel)
        .await
        .expect("the first chunk");
    assert!(!first.complete());
    assert_eq!(first.lines.len(), 2);
    assert_eq!(first.lines[1], "2026-09-03T08:00:00Z starting");

    let second = api
        .logs(
            &job,
            "job.build_and_test",
            1,
            first.continuation_token.as_deref(),
            &cancel,
        )
        .await
        .expect("the rest");
    assert!(
        second.complete(),
        "a null token is the only end-of-log signal"
    );
    assert_eq!(
        second.lines,
        vec!["done"],
        "log text is scrubbed at this boundary"
    );

    let seen = server.received_requests().await.unwrap_or_default();
    let second_url = seen[1].url.as_str();
    assert!(second_url.contains("token=opaque-1"), "{second_url}");
    assert!(second_url.contains("map_index=0"), "{second_url}");
}

#[tokio::test]
async fn pending_gates_come_back_identified_by_their_task_instance() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/dags/~/dagRuns/~/hitlDetails"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hitl_details": [{
                "options": ["Approve", "Reject"],
                "subject": "[factory] approve intent.md",
                "body": "job 0",
                "created_at": "2026-09-03T08:19:12.857207Z",
                "response_received": false,
                "task_instance": {
                    "dag_id": "factory", "dag_run_id": "manual__x",
                    "task_id": "job.approve_intent", "map_index": 0
                }
            }],
            "total_entries": 1
        })))
        .mount(&server)
        .await;

    let gates = client(&server)
        .pending_gates(&CancellationToken::new())
        .await
        .expect("one gate");

    assert_eq!(gates.len(), 1);
    assert_eq!(
        gates.rows[0].id().to_string(),
        "factory/manual__x#0:job.approve_intent"
    );
    assert!(
        !gates.rows[0].ready,
        "readiness is decided by polling, not by the payload"
    );

    let seen = server.received_requests().await.unwrap_or_default();
    let url = seen[0].url.as_str();
    assert!(
        url.contains("/dags/~/dagRuns/~/"),
        "the wildcard stays literal: {url}"
    );
    assert!(url.contains("response_received=false"), "{url}");
}

#[tokio::test]
async fn an_unreachable_server_is_told_apart_from_one_that_refused() {
    // Port 1 on loopback: nothing listens, so this is a connect failure and not a status.
    let api = AirflowApi::new("http://127.0.0.1:1", Auth::None, Duration::from_secs(2))
        .expect("client builds");
    let err = api
        .health(&CancellationToken::new())
        .await
        .expect_err("nothing is listening");

    assert_eq!(err.exit_code(), 5);
    assert_eq!(err.kind(), "unreachable");
}

#[tokio::test]
async fn the_health_probe_goes_out_without_a_credential() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/monitor/health"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}
        })))
        .mount(&server)
        .await;

    let api = AirflowApi::new(
        &server.uri(),
        Auth::Token("would-be-sent-elsewhere".into()),
        Duration::from_secs(5),
    )
    .expect("client builds");
    let health = api
        .health(&CancellationToken::new())
        .await
        .expect("healthy");
    assert_eq!(health["scheduler"]["status"], json!("healthy"));

    let seen = server.received_requests().await.unwrap_or_default();
    assert!(
        seen[0].headers.get("authorization").is_none(),
        "sending a credential collapses 'wrong URL' and 'wrong token' into one answer"
    );
}

#[tokio::test]
async fn a_base_url_behind_a_path_prefix_keeps_it_on_both_prefixes() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/airflow/auth/token"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"access_token": "t"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/airflow/api/v2/dags"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "dags": [{"dag_id": "factory"}], "total_entries": 1
        })))
        .mount(&server)
        .await;

    let api = AirflowApi::new(
        &format!("{}/airflow/", server.uri()),
        Auth::Basic {
            username: "admin".into(),
            password: String::new(),
        },
        Duration::from_secs(5),
    )
    .expect("client builds");

    let dags = api
        .list_dags("swfactory", &CancellationToken::new())
        .await
        .expect("both prefixes inherited the path");
    assert_eq!(dags.rows, vec!["factory"]);
}

#[tokio::test]
async fn a_token_endpoint_that_answers_without_a_token_is_an_auth_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/token"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"detail": "ok"})))
        .mount(&server)
        .await;

    let err = basic(&server)
        .list_dags("swfactory", &CancellationToken::new())
        .await
        .expect_err("no token, no requests");
    assert_eq!(err.exit_code(), 4);
    assert!(err.to_string().contains("no access_token"), "{err}");
}

#[tokio::test]
async fn the_runs_adapter_is_the_trait_object_that_ops_will_hold() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"dags": [], "total_entries": 0})),
        )
        .mount(&server)
        .await;

    // If `Runs` were not object-safe this line would not build, and `swf-app` could not hold an
    // environment it can swap at runtime.
    let runs: std::sync::Arc<dyn Runs> = std::sync::Arc::new(client(&server));
    assert!(runs
        .list_dags("swfactory", &CancellationToken::new())
        .await
        .expect("an empty server")
        .is_empty());
    assert_eq!(
        runs.run_url(&RunRef::new("factory", "manual__x:1")),
        format!("{}/dags/factory/runs/manual__x%3A1", server.uri())
    );
}
