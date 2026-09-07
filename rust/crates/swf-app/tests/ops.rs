//! The operations layer driven end to end with fake adapters and no network.
//!
//! Five behaviours are pinned here because they are the ones that cost real debugging and cannot
//! be seen from a unit test of any single function: per-source degradation, the gate readiness
//! rule, the conflict path before a write, redaction of a context, and the built-in context a
//! fresh machine gets.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use swf_adapters::error::{AdapterError, Result as AdapterResult};
use swf_adapters::traits::{
    CommandOutput, CommandRunner, Deliveries, LogPage, MetricsStore, Page, PrHead, Runs, Sandboxes,
};
use swf_app::attention::Attention;
use swf_app::context::{show, show_json, Auth, Context, ContextStore};
use swf_app::delivery::VerifyOpts;
use swf_app::gates::{
    AnswerOpts, BatchOutcome, BatchReport, Decision, GateFilter, Selection, CONFIRM_INTERVAL,
};
use swf_app::logs::LogOpts;
use swf_app::ops::{JobFilter, Ops};
use swf_app::snapshot::CollectOpts;
use swf_domain::evidence::Verdict;
use swf_domain::ids::{GateId, JobId, RunRef};
use swf_domain::metrics::{MetricsSummary, RunMetrics};
use swf_domain::model::{Gate, IssueRef, JobRow, PullRequest, Run, SandboxRef, TaskState};
use swf_domain::rollup::group_jobs;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------- fakes

/// Canned Airflow. Every method records that it was called, so call *order* is testable.
#[derive(Default)]
struct FakeRuns {
    dags: Vec<String>,
    runs: Vec<(String, Vec<Run>)>,
    tasks: Vec<(String, Vec<TaskState>)>,
    gates: Mutex<Vec<Gate>>,
    /// Method names that should answer an error instead of data.
    fails: Vec<&'static str>,
    calls: Mutex<Vec<String>>,
    responded: Mutex<Vec<(String, bool)>>,
    log_pages: Mutex<Vec<LogPage>>,
    /// Drop every gate after this many `pending_gates` calls — someone else answered first.
    gates_vanish_after: Option<usize>,
    pending_calls: Mutex<usize>,
    /// True when `pending_gates` must report that it stopped at its page bound.
    gates_truncated: bool,
    /// Gate ids whose `respond` answers 409: somebody else got there first.
    conflicts: Vec<String>,
    /// Gate ids whose `respond` fails outright.
    broken: Vec<String>,
    /// When true, any write at all is a test failure. A dry run is proved by construction.
    panics_on_write: bool,
    /// The issue each `(run, map_index)` answers, for the one filter that reads job rows.
    issues: Vec<(String, i32, String)>,
    /// Gate ids whose `respond` times out: a write that got no answer at all.
    times_out: Vec<String>,
    /// Gate ids whose `respond` dies on the transport, before the service is reached.
    unreachable: Vec<String>,
    /// Gate ids whose answering *task* panics, so the batch is left with no outcome for them.
    panics: Vec<String>,
    /// How many writes are in flight now, and the most there have ever been at once.
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
}

impl FakeRuns {
    fn record(&self, what: impl Into<String>) {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push(what.into());
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().map(|c| c.clone()).unwrap_or_default()
    }

    fn fails(&self, what: &str) -> bool {
        self.fails.contains(&what)
    }

    fn boom(what: &str) -> AdapterError {
        AdapterError::Status {
            code: Some(500),
            detail: format!("{what} fell over"),
        }
    }

    fn tasks_of(&self, key: &str) -> Vec<TaskState> {
        self.tasks
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl Runs for FakeRuns {
    async fn list_dags(&self, _tag: &str, _c: &CancellationToken) -> AdapterResult<Page<String>> {
        self.record("list_dags");
        if self.fails("list_dags") {
            return Err(Self::boom("list_dags"));
        }
        Ok(Page::whole(self.dags.clone()))
    }

    async fn list_runs(
        &self,
        dag_id: &str,
        _limit: usize,
        _c: &CancellationToken,
    ) -> AdapterResult<Page<Run>> {
        self.record("list_runs");
        if self.fails("list_runs") {
            return Err(Self::boom("list_runs"));
        }
        Ok(Page::whole(
            self.runs
                .iter()
                .find(|(dag, _)| dag == dag_id)
                .map(|(_, runs)| runs.clone())
                .unwrap_or_default(),
        ))
    }

    async fn task_states(
        &self,
        run: &RunRef,
        _c: &CancellationToken,
    ) -> AdapterResult<Page<TaskState>> {
        self.record(format!("task_states:{}", run.run_id));
        if self.fails("task_states") {
            return Err(Self::boom("task_states"));
        }
        Ok(Page::whole(
            self.tasks_of(&format!("{}/{}", run.dag_id, run.run_id)),
        ))
    }

    async fn fan_out_jobs(
        &self,
        _run: &RunRef,
        _c: &CancellationToken,
    ) -> AdapterResult<Vec<Value>> {
        Ok(Vec::new())
    }

    async fn job_rows(
        &self,
        run: &RunRef,
        fallback: &[String],
        _c: &CancellationToken,
    ) -> AdapterResult<Page<JobRow>> {
        self.record(format!("job_rows:{}", run.run_id));
        if self.fails(&format!("job_rows:{}", run.run_id)) {
            return Err(Self::boom("job_rows"));
        }
        let tasks = self.tasks_of(&format!("{}/{}", run.dag_id, run.run_id));
        let mut rows = group_jobs(&run.dag_id, &run.run_id, &tasks, &[], fallback);
        for row in &mut rows {
            if let Some((_, _, issue)) = self.issues.iter().find(|(key, index, _)| {
                *key == format!("{}/{}", run.dag_id, run.run_id) && *index == row.map_index
            }) {
                row.issue.clone_from(issue);
            }
        }
        Ok(Page::whole(rows))
    }

    async fn pending_gates(&self, _c: &CancellationToken) -> AdapterResult<Page<Gate>> {
        self.record("pending_gates");
        if self.fails("pending_gates") {
            return Err(Self::boom("pending_gates"));
        }
        let seen = {
            let mut count = self.pending_calls.lock().expect("lock");
            *count += 1;
            *count
        };
        if self.gates_vanish_after.is_some_and(|after| seen > after) {
            return Ok(Page::whole(Vec::new()));
        }
        let rows = self.gates.lock().map(|g| g.clone()).unwrap_or_default();
        Ok(if self.gates_truncated {
            Page::partial(rows)
        } else {
            Page::whole(rows)
        })
    }

    async fn logs(
        &self,
        _job: &JobId,
        _task: &str,
        _attempt: u32,
        _token: Option<&str>,
        _c: &CancellationToken,
    ) -> AdapterResult<LogPage> {
        self.record("logs");
        let mut pages = self.log_pages.lock().expect("lock");
        if pages.is_empty() {
            return Ok(LogPage::default());
        }
        Ok(pages.remove(0))
    }

    async fn respond(
        &self,
        gate: &GateId,
        approve: bool,
        _c: &CancellationToken,
    ) -> AdapterResult<()> {
        self.record("respond");
        assert!(
            !self.panics_on_write,
            "a dry run wrote: respond({gate}) reached the adapter"
        );
        let id = gate.to_string();
        // A write holds its slot across an await, so a batch that fanned out without a bound shows
        // up here as a peak rather than as a timing coincidence.
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);

        if self.panics.contains(&id) {
            panic!("the task answering {id} died");
        }
        if self.fails("respond") || self.conflicts.contains(&id) {
            return Err(AdapterError::from_status(
                409,
                "PATCH gate",
                "already answered",
            ));
        }
        if self.broken.contains(&id) {
            return Err(AdapterError::from_status(
                500,
                "PATCH gate",
                "the scheduler fell over",
            ));
        }
        if self.times_out.contains(&id) {
            return Err(AdapterError::Timeout {
                what: "PATCH gate".to_string(),
                after: Duration::from_secs(30),
            });
        }
        if self.unreachable.contains(&id) {
            return Err(AdapterError::Unreachable {
                what: "airflow".to_string(),
                detail: "connection reset by peer".to_string(),
            });
        }
        self.responded
            .lock()
            .expect("lock")
            .push((gate.to_string(), approve));
        Ok(())
    }

    async fn trigger(
        &self,
        dag_id: &str,
        _issues: &[String],
        _c: &CancellationToken,
    ) -> AdapterResult<String> {
        self.record("trigger");
        if self.fails("trigger") {
            return Err(Self::boom("trigger"));
        }
        Ok(format!("manual__{dag_id}"))
    }

    async fn stop_run(&self, _run: &RunRef, _c: &CancellationToken) -> AdapterResult<()> {
        self.record("stop_run");
        Ok(())
    }

    async fn unpause_dag(&self, _dag_id: &str, _c: &CancellationToken) -> AdapterResult<()> {
        Ok(())
    }

    async fn health(&self, _c: &CancellationToken) -> AdapterResult<Value> {
        self.record("health");
        if self.fails("health") {
            return Err(AdapterError::Unreachable {
                what: "http://localhost:8080".into(),
                detail: "connection refused".into(),
            });
        }
        Ok(serde_json::json!({
            "metadatabase": {"status": "healthy"},
            "scheduler": {"status": "healthy"},
        }))
    }

    fn run_url(&self, run: &RunRef) -> String {
        format!(
            "http://airflow.test/dags/{}/runs/{}",
            run.dag_id, run.run_id
        )
    }
}

/// Canned GitHub.
#[derive(Default)]
struct FakeGh {
    prs: Vec<PullRequest>,
    head: Option<PrHead>,
    fail: bool,
}

#[async_trait]
impl Deliveries for FakeGh {
    async fn prs(
        &self,
        _label: &str,
        _limit: u32,
        _c: &CancellationToken,
    ) -> AdapterResult<Vec<PullRequest>> {
        if self.fail {
            return Err(AdapterError::Unreachable {
                what: "gh".into(),
                detail: "not installed".into(),
            });
        }
        Ok(self.prs.clone())
    }

    async fn issues(
        &self,
        _label: &str,
        _limit: u32,
        _c: &CancellationToken,
    ) -> AdapterResult<Vec<IssueRef>> {
        Ok(Vec::new())
    }

    async fn pr_for_branch(
        &self,
        _branch: &str,
        _c: &CancellationToken,
    ) -> AdapterResult<Option<PrHead>> {
        if self.fail {
            return Err(AdapterError::Unreachable {
                what: "gh".into(),
                detail: "not installed".into(),
            });
        }
        Ok(self.head.clone())
    }

    async fn checks(&self, _number: i64, _c: &CancellationToken) -> AdapterResult<String> {
        Ok("none".to_string())
    }

    async fn pr_view(&self, number: i64, _c: &CancellationToken) -> AdapterResult<Vec<String>> {
        Ok(vec![
            "gh".into(),
            "pr".into(),
            "view".into(),
            number.to_string(),
        ])
    }
}

/// Canned islo.
#[derive(Default)]
struct FakeIslo {
    sandboxes: Vec<SandboxRef>,
    fail: bool,
}

#[async_trait]
impl Sandboxes for FakeIslo {
    async fn list(&self, _c: &CancellationToken) -> AdapterResult<Vec<SandboxRef>> {
        if self.fail {
            return Err(AdapterError::refused("islo said no"));
        }
        Ok(self.sandboxes.clone())
    }

    async fn remove(&self, name: &str, _c: &CancellationToken) -> AdapterResult<Vec<String>> {
        Ok(vec!["islo".into(), "rm".into(), name.to_string()])
    }
}

/// Canned metrics.
#[derive(Default)]
struct FakeMetrics {
    runs: Vec<RunMetrics>,
    fail: bool,
}

#[async_trait]
impl MetricsStore for FakeMetrics {
    async fn runs(&self, _c: &CancellationToken) -> AdapterResult<Vec<RunMetrics>> {
        if self.fail {
            return Err(AdapterError::Decode {
                what: "metrics.json".into(),
                detail: "not an object".into(),
            });
        }
        Ok(self.runs.clone())
    }

    async fn summary(&self, _c: &CancellationToken) -> AdapterResult<MetricsSummary> {
        if self.fail {
            return Err(AdapterError::Decode {
                what: "metrics.json".into(),
                detail: "not an object".into(),
            });
        }
        Ok(swf_domain::metrics::summarize(&self.runs))
    }
}

/// A subprocess runner that never forks.
#[derive(Default)]
struct FakeCommands {
    answers: Vec<(String, CommandOutput)>,
    missing: Vec<String>,
    seen: Mutex<Vec<Vec<String>>>,
    /// Files a given program "produces" — how a faked test run leaves a JUnit report behind.
    writes: Vec<(String, std::path::PathBuf, String)>,
}

#[async_trait]
impl CommandRunner for FakeCommands {
    async fn run(
        &self,
        argv: &[String],
        _timeout: Duration,
        _c: &CancellationToken,
    ) -> AdapterResult<CommandOutput> {
        self.seen.lock().expect("lock").push(argv.to_vec());
        let program = argv.first().cloned().unwrap_or_default();
        for (when, path, content) in &self.writes {
            if *when == program {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).expect("mkdir");
                }
                std::fs::write(path, content).expect("write");
            }
        }
        if self.missing.contains(&program) {
            return Err(AdapterError::Unreachable {
                what: program,
                detail: "No such file or directory".into(),
            });
        }
        Ok(self
            .answers
            .iter()
            .find(|(name, _)| *name == program)
            .map(|(_, out)| out.clone())
            .unwrap_or_default())
    }
}

// ---------------------------------------------------------------- fixtures

fn run(dag: &str, id: &str, state: &str) -> Run {
    Run::new(dag, id, state)
}

fn parked_gate() -> Gate {
    Gate::new(
        "factory",
        "r-running",
        "job.approve_plan",
        0,
        "Approve the plan?",
        "the evidence",
        None,
        vec!["Approve".into(), "Reject".into()],
    )
}

fn airflow_with_two_runs() -> FakeRuns {
    FakeRuns {
        dags: vec!["factory".into()],
        runs: vec![(
            "factory".to_string(),
            vec![
                run("factory", "r-running", "running"),
                run("factory", "r-done", "success"),
            ],
        )],
        tasks: vec![
            (
                "factory/r-running".to_string(),
                vec![
                    TaskState::new("job.build_and_test", 0, Some("success".into())),
                    TaskState::new("job.approve_plan", 0, Some("awaiting_input".into())),
                ],
            ),
            (
                "factory/r-done".to_string(),
                vec![TaskState::new("job.deliver", 0, Some("success".into()))],
            ),
        ],
        gates: Mutex::new(vec![parked_gate()]),
        ..FakeRuns::default()
    }
}

fn context() -> Context {
    let mut ctx = Context::new("test", "http://airflow.test");
    ctx.repo = Some("acme/widgets".into());
    ctx.owner = Some("me".into());
    ctx
}

fn build_ops(
    airflow: Arc<FakeRuns>,
    github: Option<Arc<FakeGh>>,
    islo: Option<Arc<FakeIslo>>,
    metrics: Option<Arc<FakeMetrics>>,
) -> Ops {
    let mut builder = Ops::builder(context()).runs(airflow);
    if let Some(github) = github {
        builder = builder.deliveries(github);
    }
    if let Some(islo) = islo {
        builder = builder.sandboxes(islo);
    }
    if let Some(metrics) = metrics {
        builder = builder.metrics(metrics);
    }
    builder.build()
}

// ---------------------------------------------------------------- per-source degradation

#[tokio::test]
async fn one_dead_source_never_blanks_another() {
    let airflow = Arc::new(FakeRuns {
        fails: vec!["list_dags", "pending_gates"],
        ..airflow_with_two_runs()
    });
    let github = Arc::new(FakeGh {
        fail: true,
        ..FakeGh::default()
    });
    let islo = Arc::new(FakeIslo {
        sandboxes: vec![SandboxRef::new("swf-demo-0badf00d", "running", "me", None)],
        ..FakeIslo::default()
    });
    let metrics = Arc::new(FakeMetrics {
        fail: true,
        ..FakeMetrics::default()
    });

    let ops = build_ops(airflow, Some(github), Some(islo), Some(metrics));
    let snap = ops.snapshot_default(&CancellationToken::new()).await;

    let keys: Vec<&str> = snap.errors.iter().map(|e| e.source.as_str()).collect();
    assert_eq!(keys, vec!["airflow", "gates", "github", "metrics"]);
    assert_eq!(
        snap.sandboxes.len(),
        1,
        "the one healthy source still has to show its data"
    );
    assert!(snap.runs.is_empty());
    assert!(!snap.healthy());
    assert!(!snap.health["airflow"].ok());
    assert!(snap.health["islo"].ok());
}

#[tokio::test]
async fn a_run_whose_jobs_cannot_be_read_keeps_its_row_and_gets_its_own_error_key() {
    let airflow = Arc::new(FakeRuns {
        fails: vec!["job_rows:r-running"],
        ..airflow_with_two_runs()
    });
    let ops = build_ops(airflow.clone(), None, None, None);
    let snap = ops.snapshot_default(&CancellationToken::new()).await;

    assert_eq!(snap.runs.len(), 2, "the table never lies by omission");
    assert_eq!(
        snap.error("airflow:factory/r-running")
            .map(|m| m.contains("job_rows")),
        Some(true)
    );
    assert!(
        snap.error("airflow").is_none(),
        "one run is not the whole source"
    );
    let collapsed = &snap.runs[0].jobs;
    assert_eq!(collapsed.len(), 1);
    assert_eq!(collapsed[0].map_index, -1);
    assert_eq!(
        collapsed[0].state, "running",
        "a collapsed row shows the run state"
    );
}

#[tokio::test]
async fn the_expensive_reads_are_bounded_but_an_active_run_is_always_expanded() {
    let airflow = Arc::new(airflow_with_two_runs());
    let ops = Ops::builder(context())
        .runs(airflow.clone())
        .collect_opts(CollectOpts {
            jobs_per_dag: 0,
            ..CollectOpts::default()
        })
        .build();
    let snap = ops.snapshot_default(&CancellationToken::new()).await;

    let calls = airflow.calls();
    assert!(calls.contains(&"job_rows:r-running".to_string()));
    assert!(
        !calls.contains(&"job_rows:r-done".to_string()),
        "a finished run past the bound collapses instead of costing two round trips"
    );
    assert_eq!(snap.runs[1].jobs[0].map_index, -1);
    assert_eq!(snap.runs[1].jobs[0].state, "success");
}

#[tokio::test]
async fn the_call_order_is_the_one_the_python_collector_makes() {
    let airflow = Arc::new(airflow_with_two_runs());
    let ops = build_ops(airflow.clone(), None, None, None);
    let _ = ops.snapshot_default(&CancellationToken::new()).await;
    assert_eq!(
        airflow.calls(),
        vec![
            "list_dags",
            "list_runs",
            "job_rows:r-running",
            "job_rows:r-done",
            "pending_gates",
        ]
    );
}

// ---------------------------------------------------------------- readiness

#[tokio::test]
async fn a_gate_is_ready_only_once_its_task_instance_is_parked() {
    let airflow = Arc::new(airflow_with_two_runs());
    let ops = build_ops(airflow.clone(), None, None, None);
    let listing = ops.gates(&CancellationToken::new()).await.expect("gates");
    assert_eq!(listing.gates.len(), 1);
    assert!(listing.gates[0].ready, "awaiting_input is a parked gate");

    // The same gate, one poll earlier: the HITL detail exists and the task has not deferred yet.
    let early = Arc::new(FakeRuns {
        tasks: vec![(
            "factory/r-running".to_string(),
            vec![TaskState::new("job.approve_plan", 0, Some("queued".into()))],
        )],
        ..airflow_with_two_runs()
    });
    let ops = build_ops(Arc::clone(&early), None, None, None);
    let listing = ops.gates(&CancellationToken::new()).await.expect("gates");
    assert!(
        !listing.gates[0].ready,
        "answering here makes the scheduler fail the gate"
    );
}

#[tokio::test]
async fn a_gate_that_is_not_ready_is_refused_and_says_why() {
    let airflow = Arc::new(FakeRuns {
        tasks: vec![(
            "factory/r-running".to_string(),
            vec![TaskState::new("job.approve_plan", 0, Some("queued".into()))],
        )],
        ..airflow_with_two_runs()
    });
    let ops = build_ops(airflow.clone(), None, None, None);
    let id = parked_gate().id();

    let opts = AnswerOpts {
        confirm_delay: Some(Duration::ZERO),
        ..AnswerOpts::default()
    };
    let Err(err) = ops
        .gate_answer(&id, Decision::Approve, &opts, &CancellationToken::new())
        .await
    else {
        panic!("an unparked gate must not be answered");
    };
    assert_eq!(err.exit_code(), 1);
    assert!(err.message.contains("queued"), "{}", err.message);
    assert!(err.message.contains("awaiting_input"), "{}", err.message);
    assert!(airflow.responded.lock().expect("lock").is_empty());

    // --force is the operator taking the risk knowingly, and the answer says so.
    let forced = AnswerOpts {
        force: true,
        confirm_delay: Some(Duration::ZERO),
        ..AnswerOpts::default()
    };
    let answer = ops
        .gate_answer(&id, Decision::Approve, &forced, &CancellationToken::new())
        .await
        .expect("force answers anyway");
    assert!(answer.forced);
    assert_eq!(airflow.responded.lock().expect("lock").len(), 1);
}

#[tokio::test]
async fn a_ready_gate_is_answered_after_a_second_sighting() {
    let airflow = Arc::new(airflow_with_two_runs());
    let ops = build_ops(airflow.clone(), None, None, None);
    let id = parked_gate().id();
    let opts = AnswerOpts {
        confirm_delay: Some(Duration::ZERO),
        ..AnswerOpts::default()
    };

    let answer = ops
        .gate_answer(&id, Decision::Approve, &opts, &CancellationToken::new())
        .await
        .expect("a parked gate can be answered");
    assert!(!answer.forced);
    assert!(
        answer.sightings >= 2,
        "a gate must be seen parked twice before it is answered"
    );
    assert_eq!(
        airflow.responded.lock().expect("lock").as_slice(),
        &[(id.to_string(), true)]
    );
}

// ---------------------------------------------------------------- the conflict path

#[tokio::test]
async fn a_gate_answered_by_someone_else_first_is_a_conflict_and_not_a_crash() {
    let airflow = Arc::new(FakeRuns {
        gates_vanish_after: Some(0),
        ..airflow_with_two_runs()
    });
    let ops = build_ops(airflow.clone(), None, None, None);
    let Err(err) = ops
        .gate_answer(
            &parked_gate().id(),
            Decision::Approve,
            &AnswerOpts::default(),
            &CancellationToken::new(),
        )
        .await
    else {
        panic!("a gate that is gone must not be answered");
    };
    assert_eq!(
        err.exit_code(),
        3,
        "a gate nobody is waiting on is not there"
    );
    assert!(airflow.responded.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn evidence_that_moved_under_the_operator_is_a_conflict() {
    let airflow = Arc::new(airflow_with_two_runs());
    let ops = build_ops(airflow.clone(), None, None, None);
    let id = parked_gate().id();

    let review = ops
        .gate_review(&id, &CancellationToken::new())
        .await
        .expect("review");
    assert!(review.ready);
    assert_eq!(review.task_state, "awaiting_input");

    // The gate's body is rewritten while the operator is reading it.
    {
        let mut gates = airflow.gates.lock().expect("lock");
        gates[0].body = "completely different evidence".into();
    }
    let opts = AnswerOpts {
        expect: Some(review.revision.clone()),
        confirm_delay: Some(Duration::ZERO),
        ..AnswerOpts::default()
    };
    let Err(err) = ops
        .gate_answer(&id, Decision::Approve, &opts, &CancellationToken::new())
        .await
    else {
        panic!("approving text the operator never saw must be refused");
    };
    assert_eq!(err.exit_code(), 6);
    assert_eq!(err.kind(), "conflict");
    assert!(airflow.responded.lock().expect("lock").is_empty());

    // The same revision, unchanged, goes through.
    let opts = AnswerOpts {
        expect: Some(Gate::revision_of(
            "Approve the plan?",
            "completely different evidence",
        )),
        confirm_delay: Some(Duration::ZERO),
        ..AnswerOpts::default()
    };
    ops.gate_answer(&id, Decision::Reject, &opts, &CancellationToken::new())
        .await
        .expect("the evidence in hand is the evidence on the wire");
    assert_eq!(
        airflow.responded.lock().expect("lock")[0],
        (id.to_string(), false)
    );
}

// ---------------------------------------------------------------- contexts

#[test]
fn a_fresh_machine_gets_a_built_in_context_and_is_told_what_to_add() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ContextStore::open_at(dir.path().join("config.toml")).expect("no file is fine");
    assert!(!store.exists());
    let ctx = store.resolve(None).expect("the built-in local context");
    assert_eq!(ctx.name, "local");
    assert_eq!(ctx.airflow_url, "http://localhost:8080");
    assert!(ctx.is_builtin());

    let ops = Ops::builder(ctx).build();
    let checks = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async { ops.doctor(&CancellationToken::new()).await });
    let context_row = checks
        .iter()
        .find(|c| c.name == "context")
        .expect("context row");
    assert!(!context_row.ok);
    assert!(!context_row.required, "a fresh machine is not a broken one");
    assert!(context_row.fix.contains("swf context add"));
    assert_eq!(swf_domain::doctor::exit_code(&checks[..1]), 0);
}

#[test]
fn a_context_never_stores_or_shows_a_secret() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");
    let mut store = ContextStore::open_at(&path).expect("open");
    let mut ctx = Context::new("prod", "https://airflow.example.com");
    ctx.auth = Auth::Basic {
        user: "admin".into(),
        password_env: "SWF_TEST_PASSWORD".into(),
    };
    store.add(ctx.clone(), false).expect("add");

    // Even with the secret sitting in the environment, nothing that leaves this process has it.
    std::env::set_var("SWF_TEST_PASSWORD", "hunter2");
    let text = std::fs::read_to_string(&path).expect("read");
    assert!(!text.contains("hunter2"), "{text}");
    assert!(text.contains("SWF_TEST_PASSWORD"), "{text}");
    assert!(!show(&ctx).contains("hunter2"));
    assert!(!show_json(&ctx).to_string().contains("hunter2"));
    assert_eq!(show_json(&ctx)["auth"]["password_env"], "SWF_TEST_PASSWORD");
    std::env::remove_var("SWF_TEST_PASSWORD");
}

// ---------------------------------------------------------------- attention, submit, logs

#[tokio::test]
async fn attention_lists_only_what_a_person_can_act_on_and_names_the_identity() {
    let airflow = Arc::new(FakeRuns {
        tasks: vec![(
            "factory/r-running".to_string(),
            vec![
                TaskState::new("job.build_and_test", 0, Some("failed".into())),
                TaskState::new("job.approve_plan", 1, Some("awaiting_input".into())),
            ],
        )],
        gates: Mutex::new(vec![Gate::new(
            "factory",
            "r-running",
            "job.approve_plan",
            1,
            "Approve?",
            "body",
            None,
            vec![],
        )]),
        ..airflow_with_two_runs()
    });
    let github = Arc::new(FakeGh {
        prs: vec![PullRequest {
            number: 7,
            title: "[BLOCKED] 42: do the thing".into(),
            url: "https://example.com/7".into(),
            labels: vec!["factory".into(), "factory:blocked".into()],
            state: "OPEN".into(),
            checks: "1 pass / 0 fail / 0 pending".into(),
            head: "factory/42-abcd1234".into(),
        }],
        ..FakeGh::default()
    });
    let ops = build_ops(airflow, Some(github), None, None);
    let attention: Attention = ops.attention(&CancellationToken::new()).await;

    assert_eq!(attention.gates.len(), 1);
    assert_eq!(
        attention.gates[0].id,
        "factory/r-running#1:job.approve_plan"
    );
    assert!(attention.gates[0].ready);
    assert_eq!(attention.failures[0].id, "factory/r-running#0");
    assert_eq!(attention.blocked[0].number, 7);
    assert_eq!(attention.blocked[0].reason, "blocked");
    assert!(!attention.is_clear());
}

#[tokio::test]
async fn a_submission_validates_before_it_posts_anything() {
    let airflow = Arc::new(airflow_with_two_runs());
    let ops = build_ops(airflow.clone(), None, None, None);

    let bad = swf_app::submit::SubmitRequest::for_issues(["../etc/passwd"]);
    let Err(err) = ops.submit(&bad, &CancellationToken::new()).await else {
        panic!("a path traversal must never reach the trigger route");
    };
    assert_eq!(err.exit_code(), 2);
    assert!(
        !airflow.calls().contains(&"trigger".to_string()),
        "nothing may be posted before the arguments are checked"
    );

    let good = swf_app::submit::SubmitRequest::for_issues(["42", "PROJ-17"]);
    let submission = ops
        .submit(&good, &CancellationToken::new())
        .await
        .expect("submit");
    assert_eq!(submission.dag_id, "factory");
    assert_eq!(submission.run_id, "manual__factory");
    assert_eq!(
        submission.issues,
        vec!["42".to_string(), "PROJ-17".to_string()]
    );
    assert!(submission.url.contains("manual__factory"));
}

#[tokio::test]
async fn following_a_log_emits_each_line_once() {
    let airflow = Arc::new(FakeRuns {
        log_pages: Mutex::new(vec![
            LogPage {
                lines: vec!["one".into(), "two".into()],
                continuation_token: Some("t1".into()),
            },
            LogPage {
                lines: vec!["three".into()],
                continuation_token: None,
            },
        ]),
        ..airflow_with_two_runs()
    });
    let ops = build_ops(airflow.clone(), None, None, None);
    let mut seen: Vec<String> = Vec::new();
    let opts = LogOpts {
        poll: Duration::ZERO,
        ..LogOpts::default()
    };
    let emitted = ops
        .follow_logs(
            &JobId::new("factory", "r-running", 0),
            &opts,
            &mut |line| seen.push(line.to_string()),
            &CancellationToken::new(),
        )
        .await
        .expect("follow");
    assert_eq!(emitted, 3);
    assert_eq!(seen, vec!["one", "two", "three"]);
}

// ---------------------------------------------------------------- delivery verdicts

fn metrics_for(run_id: &str) -> RunMetrics {
    RunMetrics {
        run_id: run_id.to_string(),
        issue_id: "42".to_string(),
        tests_passed: true,
        stage_status: BTreeMap::from([("deliver".to_string(), "ok".to_string())]),
        ..RunMetrics::default()
    }
}

fn head() -> PrHead {
    PrHead {
        url: "https://example.com/7".into(),
        state: "OPEN".into(),
        title: "42: do the thing".into(),
        labels: vec!["factory".into(), "agent-authored".into()],
        head_sha: "cafebabe".into(),
        base_ref: "main".into(),
    }
}

#[tokio::test]
async fn the_three_verdicts_stay_three_verdicts() {
    let id = swf_domain::ids::DeliveryId::parse("factory/42-abcd1234").expect("id");
    let airflow = Arc::new(airflow_with_two_runs());
    let metrics = Arc::new(FakeMetrics {
        runs: vec![metrics_for("abcd1234")],
        ..FakeMetrics::default()
    });

    // The forge answers and the branch checks out, but nothing above `published` was attempted.
    let github = Arc::new(FakeGh {
        head: Some(head()),
        ..FakeGh::default()
    });
    let ops = build_ops(airflow.clone(), Some(github), None, Some(metrics.clone()));
    let report = ops
        .verify_delivery(&id, &VerifyOpts::default(), &CancellationToken::new())
        .await
        .expect("verify");
    assert_eq!(report.verdict, Verdict::PublishedOnly);
    assert!(report.workflow_succeeded, "the run said so");
    assert!(report.branch_published, "the forge said so");
    assert!(
        !report.independently_verified,
        "nothing re-derived anything without --clone"
    );
    assert_eq!(report.exit_code(), 2, "not proven is not proven false");

    // The forge was asked and could not answer: inconclusive, never "unpublished".
    let broken = Arc::new(FakeGh {
        fail: true,
        ..FakeGh::default()
    });
    let ops = build_ops(airflow.clone(), Some(broken), None, Some(metrics.clone()));
    let report = ops
        .verify_delivery(&id, &VerifyOpts::default(), &CancellationToken::new())
        .await
        .expect("verify");
    assert_eq!(report.verdict, Verdict::Inconclusive);
    assert!(!report.branch_published);

    // No repo configured at all: the self-report is all there is, and it says so.
    let ops = Ops::builder(Context::new("bare", "http://airflow.test"))
        .runs(airflow.clone())
        .metrics(metrics.clone())
        .build();
    let report = ops
        .verify_delivery(&id, &VerifyOpts::default(), &CancellationToken::new())
        .await
        .expect("verify");
    assert_eq!(report.verdict, Verdict::ReportedOnly);
    assert!(report.workflow_succeeded);
    assert!(!report.branch_published);
}

#[tokio::test]
async fn a_delivery_the_forge_contradicts_is_refuted() {
    let id = swf_domain::ids::DeliveryId::parse("factory/42-abcd1234").expect("id");
    let mut wrong = head();
    wrong.labels = vec!["factory".into()];
    let github = Arc::new(FakeGh {
        head: Some(wrong),
        ..FakeGh::default()
    });
    let ops = build_ops(
        Arc::new(airflow_with_two_runs()),
        Some(github),
        None,
        Some(Arc::new(FakeMetrics {
            runs: vec![metrics_for("abcd1234")],
            ..FakeMetrics::default()
        })),
    );
    let report = ops
        .verify_delivery(&id, &VerifyOpts::default(), &CancellationToken::new())
        .await
        .expect("verify");
    assert_eq!(report.verdict, Verdict::Refuted);
    assert_eq!(report.exit_code(), 1);
    assert_eq!(report.refutations()[0].id, "pr.labels");
}

// ---------------------------------------------------------------- doctor and stack

#[tokio::test]
async fn doctor_tells_a_missing_tool_from_a_broken_one() {
    let airflow = Arc::new(airflow_with_two_runs());
    let commands = Arc::new(FakeCommands {
        missing: vec!["gh".into()],
        answers: vec![(
            "islo".to_string(),
            CommandOutput {
                code: 0,
                stdout: "islo 0.48.1\n".into(),
                stderr: String::new(),
            },
        )],
        ..FakeCommands::default()
    });
    let ops = Ops::builder(context())
        .runs(airflow)
        .commands(commands)
        .build();
    let checks = ops.doctor(&CancellationToken::new()).await;
    let by_name = |name: &str| {
        checks
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no {name} row"))
            .clone()
    };
    assert!(by_name("airflow api").ok);
    assert!(by_name("airflow auth").ok);
    assert!(by_name("islo cli").ok);
    let gh = by_name("gh auth");
    assert!(!gh.ok);
    assert!(gh.required, "this context has a repo, so gh is required");
    assert!(gh.detail.contains("not on PATH"), "{}", gh.detail);
    assert!(gh.fix.contains("install"), "{}", gh.fix);
}

#[tokio::test]
async fn stack_status_reports_what_is_running_and_not_what_was_asked_for() {
    let commands = Arc::new(FakeCommands {
        answers: vec![(
            "env".to_string(),
            CommandOutput {
                code: 0,
                stdout: r#"{"Service":"airflow","State":"running","Health":"healthy","Status":"Up 4 minutes (healthy)"}"#
                    .into(),
                stderr: String::new(),
            },
        )],
        ..FakeCommands::default()
    });
    let ops = Ops::builder(context())
        .runs(Arc::new(airflow_with_two_runs()))
        .commands(commands.clone())
        .build();
    let status = ops
        .stack(
            swf_app::stack::StackAction::Status,
            &CancellationToken::new(),
        )
        .await
        .expect("status");
    let row = |name: &str| {
        status
            .rows
            .iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("no {name} row"))
    };
    assert!(row("airflow").ok);
    assert!(
        !row("webhook").ok,
        "a service that is not there is not running"
    );
    assert!(row("api").ok);
    assert!(!status.ok);
    let argv = commands.seen.lock().expect("lock");
    assert_eq!(argv[0][0], "env", "compose is run with PWD exported");
    assert!(argv[0].iter().any(|a| a.starts_with("PWD=")));
}

#[tokio::test]
async fn with_clone_the_targets_own_test_command_is_re_run_from_the_published_branch() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let target_dir = scratch.path().join("checkout").join("demo").join("target");
    std::fs::create_dir_all(&target_dir).expect("mkdir");
    std::fs::write(
        target_dir.join("factory.toml"),
        "[commands]\ntest = \"pytest --junitxml=.factory/junit.xml\"\n",
    )
    .expect("write");

    let commands = Arc::new(FakeCommands {
        answers: vec![
            ("git".to_string(), CommandOutput::default()),
            ("sh".to_string(), CommandOutput::default()),
        ],
        writes: vec![(
            "sh".to_string(),
            target_dir.join(".factory").join("junit.xml"),
            r#"<testsuite tests="4" failures="0" errors="0" skipped="1"/>"#.to_string(),
        )],
        ..FakeCommands::default()
    });
    let github = Arc::new(FakeGh {
        head: Some(head()),
        ..FakeGh::default()
    });
    let ops = Ops::builder(context())
        .runs(Arc::new(airflow_with_two_runs()))
        .deliveries(github)
        .metrics(Arc::new(FakeMetrics {
            runs: vec![metrics_for("abcd1234")],
            ..FakeMetrics::default()
        }))
        .commands(commands.clone())
        .build();

    let opts = VerifyOpts {
        clone: true,
        workdir: Some(scratch.path().to_path_buf()),
        keep_checkout: true,
        base_branch: Some("main".to_string()),
        ..VerifyOpts::default()
    };
    let id = swf_domain::ids::DeliveryId::parse("factory/42-abcd1234").expect("id");
    let report = ops
        .verify_delivery(&id, &opts, &CancellationToken::new())
        .await
        .expect("verify");

    assert_eq!(report.verdict, Verdict::Verified);
    assert!(report.independently_verified);
    assert_eq!(report.exit_code(), 0);
    let tests = report.tests.expect("the re-run is the evidence");
    assert!(tests.ok);
    assert_eq!(tests.passed, 3);
    assert_eq!(tests.skipped, 1);
    assert!(tests.report_valid);
    assert_eq!(tests.cwd, target_dir.display().to_string());
    assert_eq!(tests.command, "pytest --junitxml=.factory/junit.xml");

    let argv = commands.seen.lock().expect("lock");
    assert_eq!(argv[0][..2], ["git".to_string(), "clone".to_string()]);
    let shell = argv.last().expect("the test run");
    assert_eq!(shell[0], "sh");
    assert!(
        shell.contains(&target_dir.display().to_string()),
        "the directory is an argument, never interpolated into the script"
    );
    assert!(shell.contains(&"pytest --junitxml=.factory/junit.xml".to_string()));
}

// ---------------------------------------------------------------- filtering and bulk answers

/// A gate on `factory/r1` for one job index, parked or not according to the task fixture below.
fn gate_at(dag: &str, run: &str, task: &str, index: i32) -> Gate {
    Gate::new(
        dag,
        run,
        task,
        index,
        format!("Approve {task} of job {index}?"),
        "the evidence",
        None,
        vec!["Approve".into(), "Reject".into()],
    )
}

/// Four gates over two DAGs, three of them parked and one still arming.
///
/// It is deliberately not uniform: every filter below has to be able to select a proper subset,
/// and a fixture where every row matches everything proves nothing about a filter.
fn a_board_of_gates() -> FakeRuns {
    FakeRuns {
        dags: vec!["factory".into(), "hotfix".into()],
        runs: vec![
            ("factory".to_string(), vec![run("factory", "r1", "running")]),
            ("hotfix".to_string(), vec![run("hotfix", "h1", "running")]),
        ],
        tasks: vec![
            (
                "factory/r1".to_string(),
                vec![
                    TaskState::new("job.approve_plan", 0, Some("awaiting_input".into())),
                    TaskState::new("job.approve_intent", 1, Some("awaiting_input".into())),
                    TaskState::new("job.approve_plan", 2, Some("queued".into())),
                ],
            ),
            (
                "hotfix/h1".to_string(),
                vec![TaskState::new(
                    "job.approve_plan",
                    0,
                    Some("awaiting_input".into()),
                )],
            ),
        ],
        gates: Mutex::new(vec![
            gate_at("factory", "r1", "job.approve_plan", 0),
            gate_at("factory", "r1", "job.approve_intent", 1),
            gate_at("factory", "r1", "job.approve_plan", 2),
            gate_at("hotfix", "h1", "job.approve_plan", 0),
        ]),
        issues: vec![
            ("factory/r1".to_string(), 0, "42".to_string()),
            ("factory/r1".to_string(), 1, "43".to_string()),
            ("factory/r1".to_string(), 2, "44".to_string()),
            ("hotfix/h1".to_string(), 0, "42".to_string()),
        ],
        ..FakeRuns::default()
    }
}

/// The gate ids a selection holds, in order, so a filter's answer is asserted as a set of names.
fn selected_ids(selection: &Selection) -> Vec<String> {
    selection
        .rows
        .iter()
        .map(|row| row.gate.id().to_string())
        .collect()
}

async fn select_with(ops: &Ops, filter: GateFilter) -> Selection {
    ops.gates_selection(&filter, &CancellationToken::new())
        .await
        .expect("the gate list is readable")
}

#[tokio::test]
async fn every_filter_narrows_and_two_of_them_narrow_twice() {
    let ops = build_ops(Arc::new(a_board_of_gates()), None, None, None);

    let all = select_with(&ops, GateFilter::default()).await;
    assert_eq!(all.len(), 4, "no filter is every pending gate");

    let by_dag = select_with(
        &ops,
        GateFilter {
            dag: Some("hotfix".into()),
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(selected_ids(&by_dag), vec!["hotfix/h1#0:job.approve_plan"]);

    // A blueprint *is* its DAG id, so the two spellings select the same set.
    let by_blueprint = select_with(
        &ops,
        GateFilter {
            blueprint: Some("hotfix".into()),
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(selected_ids(&by_blueprint), selected_ids(&by_dag));

    // `plan`, `approve_plan` and `job.approve_plan` are one gate under three names.
    for spelling in ["plan", "approve_plan", "job.approve_plan"] {
        let by_gate = select_with(
            &ops,
            GateFilter {
                gate: Some(spelling.into()),
                ..GateFilter::default()
            },
        )
        .await;
        assert_eq!(by_gate.len(), 3, "{spelling} selected the wrong set");
    }

    let ready_only = select_with(
        &ops,
        GateFilter {
            ready: true,
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(ready_only.len(), 3, "job 2 has not parked yet");

    // Composed: each one narrows, none widens.
    let composed = select_with(
        &ops,
        GateFilter {
            dag: Some("factory".into()),
            gate: Some("plan".into()),
            ready: true,
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(
        selected_ids(&composed),
        vec!["factory/r1#0:job.approve_plan"]
    );

    // Two filters that disagree select nothing at all rather than falling back to everything.
    let contradictory = select_with(
        &ops,
        GateFilter {
            dag: Some("factory".into()),
            blueprint: Some("hotfix".into()),
            ..GateFilter::default()
        },
    )
    .await;
    assert!(contradictory.is_empty());
    assert!(!contradictory.truncated, "empty is not truncated");
}

#[tokio::test]
async fn an_issue_filter_selects_by_the_job_a_gate_belongs_to() {
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);

    let by_issue = select_with(
        &ops,
        GateFilter {
            issue: Some("42".into()),
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(
        selected_ids(&by_issue),
        vec![
            "factory/r1#0:job.approve_plan",
            "hotfix/h1#0:job.approve_plan"
        ],
        "one issue across two targets is two jobs, and both of its gates are selected"
    );
    assert_eq!(
        by_issue.rows[0].issue.as_deref(),
        Some("42"),
        "the issue that matched is reported, not merely used"
    );

    let with_dag = select_with(
        &ops,
        GateFilter {
            issue: Some("42".into()),
            dag: Some("factory".into()),
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(with_dag.len(), 1);

    assert!(
        select_with(
            &ops,
            GateFilter {
                issue: Some("nobody".into()),
                ..GateFilter::default()
            }
        )
        .await
        .is_empty(),
        "an issue nothing answers selects nothing"
    );
}

#[tokio::test]
async fn a_filter_is_applied_before_the_reads_it_would_otherwise_pay_for() {
    // The whole point of filtering here rather than in `jq`: a DAG the operator excluded costs no
    // task-state read, and the excluded run is never asked about at all.
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let _ = select_with(
        &ops,
        GateFilter {
            dag: Some("factory".into()),
            ..GateFilter::default()
        },
    )
    .await;
    let calls = airflow.calls();
    assert!(calls.contains(&"task_states:r1".to_string()));
    assert!(
        !calls.contains(&"task_states:h1".to_string()),
        "a gate the filter excluded must not cost a read: {calls:?}"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| c.starts_with("task_states"))
            .count(),
        1,
        "one read per run, not one per gate: {calls:?}"
    );
}

#[tokio::test]
async fn a_limit_stops_the_read_and_says_the_set_was_shortened() {
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);

    let bounded = select_with(
        &ops,
        GateFilter {
            limit: Some(2),
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(bounded.len(), 2);
    assert!(
        bounded.truncated,
        "a listing the limit cut must never look like a listing that was simply short"
    );
    assert!(
        !airflow.calls().contains(&"task_states:h1".to_string()),
        "the limit stops the loop rather than trimming its result"
    );

    let whole = select_with(
        &ops,
        GateFilter {
            limit: Some(9),
            ..GateFilter::default()
        },
    )
    .await;
    assert_eq!(whole.len(), 4);
    assert!(!whole.truncated, "a limit nothing hit is not a truncation");
}

#[tokio::test]
async fn a_page_bound_upstream_is_carried_into_the_selection() {
    let airflow = Arc::new(FakeRuns {
        gates_truncated: true,
        ..a_board_of_gates()
    });
    let ops = build_ops(airflow, None, None, None);
    let selection = select_with(&ops, GateFilter::default()).await;
    assert_eq!(selection.len(), 4);
    assert!(
        selection.truncated,
        "a read that stopped at its page bound is not the whole set, whatever the filter said"
    );
}

#[tokio::test]
async fn a_dry_run_answers_the_whole_set_and_writes_nothing() {
    // The proof is structural: this adapter panics on any write at all, so a dry run that reached
    // the PATCH would fail this test rather than merely disagree with an assertion about counts.
    let airflow = Arc::new(FakeRuns {
        panics_on_write: true,
        ..a_board_of_gates()
    });
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter::default();
    let selection = select_with(&ops, filter.clone()).await;

    let report = BatchReport::dry(&selection, Decision::Approve, &filter);
    assert!(report.dry_run);
    assert_eq!(report.matched(), 4);
    assert_eq!(report.count(BatchOutcome::Planned), 3);
    assert_eq!(report.count(BatchOutcome::Skipped), 1);
    assert_eq!(report.exit_code(), 0);
    assert_eq!(report.filter, "no filter: every pending gate");
    assert!(
        !airflow.calls().contains(&"respond".to_string()),
        "a dry run must not even reach the write"
    );
    assert!(airflow.responded.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn one_gate_that_fails_never_abandons_the_rest() {
    let airflow = Arc::new(FakeRuns {
        broken: vec!["factory/r1#1:job.approve_intent".to_string()],
        ..a_board_of_gates()
    });
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter::default();
    let selection = select_with(&ops, filter.clone()).await;
    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Approve,
            &filter,
            Some(Duration::ZERO),
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");

    assert_eq!(report.matched(), 4);
    assert_eq!(report.count(BatchOutcome::Answered), 2);
    assert_eq!(report.count(BatchOutcome::Failed), 1);
    assert_eq!(report.count(BatchOutcome::Skipped), 1);
    assert_eq!(
        report.exit_code(),
        1,
        "something actually failed, so the shell has to hear about it"
    );
    let answered: Vec<String> = airflow
        .responded
        .lock()
        .expect("lock")
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    let mut answered = answered;
    // Sorted: the batch answers concurrently, so *which* gates were written is the promise and the
    // order they completed in is not.
    answered.sort();
    assert_eq!(
        answered,
        vec![
            "factory/r1#0:job.approve_plan",
            "hotfix/h1#0:job.approve_plan"
        ],
        "the gates either side of the failure were still answered"
    );
    // Every gate keeps its own line, in the order it was selected.
    let ids: Vec<String> = report
        .items
        .iter()
        .map(|item| item.id.to_string())
        .collect();
    assert_eq!(ids, selected_ids(&selection));
}

#[tokio::test]
async fn someone_else_answering_first_is_a_conflict_and_not_a_failure() {
    let airflow = Arc::new(FakeRuns {
        conflicts: vec!["factory/r1#0:job.approve_plan".to_string()],
        ..a_board_of_gates()
    });
    let ops = build_ops(airflow, None, None, None);
    let filter = GateFilter {
        ready: true,
        ..GateFilter::default()
    };
    let selection = select_with(&ops, filter.clone()).await;
    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Reject,
            &filter,
            Some(Duration::ZERO),
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");

    assert_eq!(report.count(BatchOutcome::Conflict), 1);
    assert_eq!(report.count(BatchOutcome::Answered), 2);
    assert_eq!(report.failures(), 0);
    assert_eq!(
        report.exit_code(),
        0,
        "in a shared control room a lost race is the system working"
    );
    let conflicted = report
        .items
        .iter()
        .find(|item| item.outcome == BatchOutcome::Conflict)
        .expect("the conflicted gate keeps its line");
    assert!(
        !conflicted.detail.is_empty(),
        "a conflict says what happened"
    );
}

#[tokio::test]
async fn a_gate_that_has_not_parked_is_skipped_with_its_reason_and_never_forced() {
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter::default();
    let selection = select_with(&ops, filter.clone()).await;
    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Approve,
            &filter,
            Some(Duration::ZERO),
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");

    let skipped = report
        .items
        .iter()
        .find(|item| item.outcome == BatchOutcome::Skipped)
        .expect("the arming gate is reported, not dropped");
    assert_eq!(skipped.id.to_string(), "factory/r1#2:job.approve_plan");
    assert!(!skipped.ready);
    assert!(
        skipped.detail.contains("queued") && skipped.detail.contains("awaiting_input"),
        "a skip has to say why it was skipped: {}",
        skipped.detail
    );
    assert_eq!(
        report.exit_code(),
        0,
        "a gate that is arming is not a failure"
    );
    let written: Vec<String> = airflow
        .responded
        .lock()
        .expect("lock")
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    assert!(
        !written.contains(&"factory/r1#2:job.approve_plan".to_string()),
        "a batch has no --force: an unparked gate is never written to"
    );
}

#[tokio::test]
async fn a_batch_answers_more_than_one_gate_at_a_time_and_still_re_reads_each_one() {
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter {
        ready: true,
        ..GateFilter::default()
    };
    let selection = select_with(&ops, filter.clone()).await;
    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Approve,
            &filter,
            Some(Duration::ZERO),
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");

    assert_eq!(report.count(BatchOutcome::Answered), 3);
    let calls = airflow.calls();
    let re_reads = calls.iter().filter(|c| *c == "pending_gates").count();
    assert!(
        re_reads >= 4,
        "one read for the selection and one per gate before its own write: {calls:?}"
    );
    for item in &report.items {
        assert!(
            item.sightings >= 2,
            "a bulk answer keeps the two-sighting rule: {item:?}"
        );
    }
}

/// The regression this file exists to keep fixed: a batch that sights a gate and answers it in the
/// same breath.
///
/// `swf gates approve --all` reads the gate list to build its selection and then re-reads each
/// gate inside `answer`, milliseconds later. Both reads describe the same instant of the world, so
/// counting them as the two required sightings answered gates fractions of a second after their
/// task instance parked — inside the window in which the scheduler is still reconciling the worker
/// process that parked it. It then judges that process's `success` against a task it has just
/// re-queued ("finished with state success, but the task instance's state attribute is queued"),
/// marks the gate FAILED, and the job dies `upstream_failed` behind it. Time under observation is
/// the rule; the count of reads is not.
#[tokio::test(start_paused = true)]
async fn a_batch_settles_from_the_first_sighting_and_not_from_the_read_that_follows_it() {
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter {
        ready: true,
        ..GateFilter::default()
    };
    let started = tokio::time::Instant::now();
    let selection = select_with(&ops, filter.clone()).await;
    assert_eq!(selection.ready_count(), 3);

    // `None` is exactly what `swf gates approve --all` passes: the product's own window.
    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Approve,
            &filter,
            None,
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");

    assert_eq!(report.count(BatchOutcome::Answered), 3);
    let waited = started.elapsed();
    assert!(
        waited >= CONFIRM_INTERVAL,
        "the batch answered {waited:?} after it first saw the gates parked; \
         the scheduler fails a gate answered inside {CONFIRM_INTERVAL:?}"
    );
    assert!(
        waited < CONFIRM_INTERVAL * 2,
        "a wave settles once, concurrently — not once per gate ({waited:?})"
    );
}

/// The other half of the same rule: an operator who has had the gate on screen waits for nothing.
///
/// The settle is a window since the gate was FIRST seen parked, not a pause bolted onto the write,
/// so a review followed by a decision that took longer than the window costs no delay at all.
#[tokio::test(start_paused = true)]
async fn a_gate_an_operator_has_been_looking_at_is_answered_with_no_pause_at_all() {
    let airflow = Arc::new(airflow_with_two_runs());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let cancel = CancellationToken::new();
    let id = parked_gate().id();

    let review = ops.gate_review(&id, &cancel).await.expect("review");
    assert!(review.ready);
    // The operator reads the evidence and makes up their mind.
    tokio::time::sleep(CONFIRM_INTERVAL * 2).await;

    let decided = tokio::time::Instant::now();
    let answer = ops
        .gate_answer(&id, Decision::Approve, &AnswerOpts::default(), &cancel)
        .await
        .expect("a gate that has been parked all along can be answered");
    assert!(answer.sightings >= 2);
    assert!(
        decided.elapsed() < CONFIRM_INTERVAL,
        "the window was already behind this gate; the write must not wait it out again \
         (waited {:?})",
        decided.elapsed()
    );
}

/// Two hundred gates, every one of them parked. The shape a fan-out of twenty issues reaches.
fn a_wall_of_gates(n: i32) -> FakeRuns {
    a_wall_of_gates_across(1, n)
}

/// `runs` DAG runs with `per_run` parked gates each.
///
/// The split matters to what a batch may overlap: answers to different runs may go at once, and
/// answers within one run may not, so a fixture that puts every gate in one run can only ever
/// measure the serial case.
fn a_wall_of_gates_across(runs: i32, per_run: i32) -> FakeRuns {
    let mut gates = Vec::new();
    let mut run_rows = Vec::new();
    let mut tasks = Vec::new();
    // Numbered from 1 so the single-run case keeps the id (`r1`) that the fixtures around it name.
    for r in 1..=runs {
        let run_id = format!("r{r}");
        let mut per = Vec::new();
        for index in 0..per_run {
            gates.push(gate_at("factory", &run_id, "job.approve_plan", index));
            per.push(TaskState::new(
                "job.approve_plan",
                index,
                Some("awaiting_input".into()),
            ));
        }
        run_rows.push(run("factory", &run_id, "running"));
        tasks.push((format!("factory/{run_id}"), per));
    }
    FakeRuns {
        dags: vec!["factory".into()],
        runs: vec![("factory".to_string(), run_rows)],
        tasks,
        gates: Mutex::new(gates),
        ..FakeRuns::default()
    }
}

#[tokio::test]
async fn a_filter_that_matched_everything_still_never_exceeds_the_concurrency_bound() {
    // The failure this pins is an outage of the batch's own making: an unbounded fan-out over a
    // filter that matched the whole factory would put two hundred simultaneous PATCHes on the one
    // scheduler this tool exists to help operate. The bound is counted, not assumed.
    let airflow = Arc::new(a_wall_of_gates_across(40, 5));
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter::default();
    let selection = select_with(&ops, filter.clone()).await;
    assert_eq!(selection.ready_count(), 200);

    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Approve,
            &filter,
            Some(Duration::ZERO),
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");
    assert_eq!(report.count(BatchOutcome::Answered), 200);

    let peak = airflow.peak_in_flight.load(Ordering::SeqCst);
    assert!(
        peak <= swf_app::gates::BATCH_CONCURRENCY,
        "{peak} writes were in flight at once against a bound of {}",
        swf_app::gates::BATCH_CONCURRENCY
    );
    assert!(
        peak > 1,
        "a bound that never reached two would be a serial loop wearing a batch's name"
    );
}

#[tokio::test]
async fn two_answers_to_the_same_run_never_overlap() {
    // Airflow answered a second concurrent PATCH to one DAG run with HTTP 500 and failed the gate,
    // which failed its job — seen on a live server, not theorised. Gates of one run are therefore
    // answered one at a time; the batch's speed comes from overlapping DIFFERENT runs.
    let airflow = Arc::new(a_wall_of_gates_across(1, 25));
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter::default();
    let selection = select_with(&ops, filter.clone()).await;
    assert_eq!(selection.ready_count(), 25);

    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Approve,
            &filter,
            Some(Duration::ZERO),
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");
    assert_eq!(report.count(BatchOutcome::Answered), 25);

    let peak = airflow.peak_in_flight.load(Ordering::SeqCst);
    assert_eq!(
        peak, 1,
        "two writes were in flight against one run at once; that is the race that 500s"
    );
}

#[tokio::test]
async fn each_way_a_single_write_can_go_wrong_costs_only_its_own_gate() {
    // 409 is the one that is not a failure; the other three are, and each keeps its own exit code
    // when it is the only kind in the batch. All four leave the other gates answered.
    let cases: [(&str, BatchOutcome, i32); 4] = [
        ("conflict", BatchOutcome::Conflict, 0),
        ("broken", BatchOutcome::Failed, 1),
        ("timeout", BatchOutcome::Failed, 5),
        ("unreachable", BatchOutcome::Failed, 5),
    ];
    let bad = "factory/r1#2:job.approve_plan".to_string();
    for (which, expected, code) in cases {
        let mut airflow = a_wall_of_gates(5);
        match which {
            "conflict" => airflow.conflicts = vec![bad.clone()],
            "broken" => airflow.broken = vec![bad.clone()],
            "timeout" => airflow.times_out = vec![bad.clone()],
            _ => airflow.unreachable = vec![bad.clone()],
        }
        let airflow = Arc::new(airflow);
        let ops = build_ops(Arc::clone(&airflow), None, None, None);
        let filter = GateFilter::default();
        let selection = select_with(&ops, filter.clone()).await;
        let report = ops
            .gate_answer_all(
                &selection,
                Decision::Approve,
                &filter,
                Some(Duration::ZERO),
                &CancellationToken::new(),
            )
            .await
            .expect("a batch reports rather than fails");

        assert_eq!(report.matched(), 5, "{which}: every gate keeps its line");
        assert_eq!(
            report.count(BatchOutcome::Answered),
            4,
            "{which}: one bad write must not cost the other four"
        );
        assert_eq!(airflow.responded.lock().expect("lock").len(), 4, "{which}");
        let item = report
            .items
            .iter()
            .find(|item| item.id.to_string() == bad)
            .expect("the failing gate has its own line");
        assert_eq!(item.outcome, expected, "{which}: {item:?}");
        assert_eq!(
            report.exit_code(),
            code,
            "{which}: the exit code has to name the cause, not flatten it"
        );
    }
}

#[tokio::test]
async fn an_answer_that_died_mid_write_is_a_failure_and_never_a_silent_zero() {
    // A task that panics answered nothing *and* cannot say whether its PATCH landed. Reporting it
    // as merely skipped would exit 0 over a gate whose fate the batch does not know, which is the
    // one thing that would teach an operator to stop reading the exit code.
    let airflow = Arc::new(FakeRuns {
        panics: vec!["factory/r1#2:job.approve_plan".to_string()],
        ..a_wall_of_gates(5)
    });
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let filter = GateFilter::default();
    let selection = select_with(&ops, filter.clone()).await;
    let report = ops
        .gate_answer_all(
            &selection,
            Decision::Approve,
            &filter,
            Some(Duration::ZERO),
            &CancellationToken::new(),
        )
        .await
        .expect("a batch reports rather than fails");

    assert_eq!(
        report.matched(),
        5,
        "no gate may be missing from the report"
    );
    assert_eq!(report.count(BatchOutcome::Answered), 4);
    let item = report
        .items
        .iter()
        .find(|item| item.id.to_string() == "factory/r1#2:job.approve_plan")
        .expect("the dead task's gate still has a line");
    assert_eq!(item.outcome, BatchOutcome::Failed, "{item:?}");
    assert!(
        item.detail.contains("did not complete"),
        "and says the write's fate is unknown: {}",
        item.detail
    );
    assert_ne!(report.exit_code(), 0, "a dead write task must not exit 0");
}

// ---------------------------------------------------------------- job and run listings

/// Two jobs in two different states, each answering a different issue.
fn a_board_of_jobs() -> FakeRuns {
    FakeRuns {
        issues: vec![
            ("factory/r-running".to_string(), 0, "42".to_string()),
            ("factory/r-done".to_string(), 0, "43".to_string()),
        ],
        ..airflow_with_two_runs()
    }
}

#[tokio::test]
async fn a_job_listing_narrows_by_state_issue_and_attention_and_says_when_it_was_cut() {
    let ops = build_ops(Arc::new(a_board_of_jobs()), None, None, None);
    let cancel = CancellationToken::new();

    let all = ops.jobs_matching(&JobFilter::default(), &cancel).await;
    assert_eq!(all.jobs.len(), 2);
    assert!(!all.truncated);

    let by_state = |state: &str| JobFilter {
        state: Some(state.to_string()),
        ..JobFilter::default()
    };
    assert_eq!(
        ops.jobs_matching(&by_state("running"), &cancel)
            .await
            .jobs
            .len(),
        1
    );
    assert_eq!(
        ops.jobs_matching(&by_state("success"), &cancel)
            .await
            .jobs
            .len(),
        1
    );
    assert!(ops
        .jobs_matching(&by_state("failed"), &cancel)
        .await
        .jobs
        .is_empty());

    let by_issue = ops
        .jobs_matching(
            &JobFilter {
                issue: Some("43".into()),
                ..JobFilter::default()
            },
            &cancel,
        )
        .await;
    assert_eq!(by_issue.jobs.len(), 1);
    assert_eq!(by_issue.jobs[0].run_id, "r-done");

    // The filters compose, and an issue in another state selects nothing rather than everything.
    let composed = ops
        .jobs_matching(
            &JobFilter {
                issue: Some("43".into()),
                state: Some("running".into()),
                ..JobFilter::default()
            },
            &cancel,
        )
        .await;
    assert!(composed.jobs.is_empty());

    let needs_a_person = ops
        .jobs_matching(
            &JobFilter {
                attention: true,
                ..JobFilter::default()
            },
            &cancel,
        )
        .await;
    assert_eq!(needs_a_person.jobs.len(), 1, "only the job on a gate");
    assert_eq!(needs_a_person.jobs[0].run_id, "r-running");

    let bounded = ops
        .jobs_matching(
            &JobFilter {
                limit: Some(1),
                ..JobFilter::default()
            },
            &cancel,
        )
        .await;
    assert_eq!(bounded.jobs.len(), 1);
    assert!(bounded.truncated, "a listing the limit cut has to say so");
}

#[tokio::test]
async fn a_dag_filter_on_jobs_narrows_the_read_and_not_just_the_table() {
    // The assertion is about what was *not* called: the excluded DAG costs no round trip at all.
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(Arc::clone(&airflow), None, None, None);
    let listing = ops
        .jobs_matching(
            &JobFilter {
                dag: Some("hotfix".into()),
                ..JobFilter::default()
            },
            &CancellationToken::new(),
        )
        .await;
    assert_eq!(listing.jobs.len(), 1);
    let calls = airflow.calls();
    assert!(calls.contains(&"job_rows:h1".to_string()));
    assert!(
        !calls.contains(&"job_rows:r1".to_string()),
        "factory was excluded, so factory was never read: {calls:?}"
    );
}

#[tokio::test]
async fn a_run_listing_narrows_by_state_and_reports_the_bound_it_stopped_at() {
    let airflow = Arc::new(a_board_of_gates());
    let ops = build_ops(airflow, None, None, None);
    let cancel = CancellationToken::new();

    let all = ops
        .runs_matching("factory", 20, None, &cancel)
        .await
        .expect("list");
    assert_eq!(all.runs.len(), 1);
    assert!(
        !all.truncated,
        "one run out of a bound of twenty is the lot"
    );

    let matching = ops
        .runs_matching("factory", 20, Some("running"), &cancel)
        .await
        .expect("list");
    assert_eq!(matching.runs.len(), 1);
    let other = ops
        .runs_matching("factory", 20, Some("success"), &cancel)
        .await
        .expect("list");
    assert!(
        other.runs.is_empty(),
        "a state nothing is in yields nothing"
    );

    let bounded = ops
        .runs_matching("factory", 1, None, &cancel)
        .await
        .expect("list");
    assert!(
        bounded.truncated,
        "a read that came back full may have left rows behind, and must not imply otherwise"
    );
}
