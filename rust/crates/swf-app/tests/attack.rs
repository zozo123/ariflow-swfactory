//! THROWAWAY adversarial tests for the bulk-gate path. Deleted before hand-off.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use swf_adapters::error::{AdapterError, Result as AdapterResult};
use swf_adapters::traits::{Deliveries, LogPage, Page, Runs, Sandboxes};
use swf_app::context::Context;
use swf_app::gates::{BatchOutcome, BatchReport, Decision, GateFilter, Selection};
use swf_app::ops::Ops;
use swf_domain::ids::{GateId, JobId, RunRef};
use swf_domain::model::{Gate, JobRow, Run, TaskState};
use tokio_util::sync::CancellationToken;

/// What a gate's write should do.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    Ok,
    Conflict409,
    Server500,
    Timeout,
    Transport,
    Panic,
}

#[derive(Default)]
struct Rig {
    gates: Mutex<Vec<Gate>>,
    tasks: Mutex<HashMap<String, Vec<TaskState>>>,
    faults: Mutex<HashMap<String, Fault>>,
    responded: Mutex<Vec<String>>,
    calls: Mutex<Vec<String>>,
    /// Any write at all sets this; a dry run that writes cannot hide behind a swallowed panic.
    wrote: AtomicUsize,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    /// After this many pending_gates calls, every gate's task flips to `running`.
    unpark_after: Option<usize>,
    pending_calls: AtomicUsize,
    truncated: bool,
}

impl Rig {
    fn record(&self, what: impl Into<String>) {
        self.calls.lock().expect("lock").push(what.into());
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("lock").clone()
    }
    fn responded(&self) -> Vec<String> {
        self.responded.lock().expect("lock").clone()
    }
}

#[async_trait]
impl Runs for Rig {
    async fn list_dags(&self, _t: &str, _c: &CancellationToken) -> AdapterResult<Page<String>> {
        Ok(Page::whole(vec!["factory".into()]))
    }

    async fn list_runs(
        &self,
        dag_id: &str,
        _l: usize,
        _c: &CancellationToken,
    ) -> AdapterResult<Page<Run>> {
        Ok(Page::whole(vec![Run::new(dag_id, "r1", "running")]))
    }

    async fn task_states(
        &self,
        run: &RunRef,
        _c: &CancellationToken,
    ) -> AdapterResult<Page<TaskState>> {
        self.record(format!("task_states:{}", run.run_id));
        let key = format!("{}/{}", run.dag_id, run.run_id);
        let unparked = self
            .unpark_after
            .is_some_and(|n| self.pending_calls.load(Ordering::SeqCst) > n);
        let rows = self
            .tasks
            .lock()
            .expect("lock")
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let rows = if unparked {
            rows.into_iter()
                .map(|t| TaskState::new(&t.task_id, t.map_index, Some("running".into())))
                .collect()
        } else {
            rows
        };
        Ok(Page::whole(rows))
    }

    async fn fan_out_jobs(
        &self,
        _r: &RunRef,
        _c: &CancellationToken,
    ) -> AdapterResult<Vec<Value>> {
        Ok(Vec::new())
    }

    async fn job_rows(
        &self,
        _r: &RunRef,
        _t: &[String],
        _c: &CancellationToken,
    ) -> AdapterResult<Page<JobRow>> {
        self.record("job_rows");
        Ok(Page::whole(Vec::new()))
    }

    async fn pending_gates(&self, _c: &CancellationToken) -> AdapterResult<Page<Gate>> {
        self.record("pending_gates");
        self.pending_calls.fetch_add(1, Ordering::SeqCst);
        let rows = self.gates.lock().expect("lock").clone();
        Ok(if self.truncated {
            Page::partial(rows)
        } else {
            Page::whole(rows)
        })
    }

    async fn logs(
        &self,
        _j: &JobId,
        _t: &str,
        _a: u32,
        _tok: Option<&str>,
        _c: &CancellationToken,
    ) -> AdapterResult<LogPage> {
        Ok(LogPage::default())
    }

    async fn respond(
        &self,
        gate: &GateId,
        _approve: bool,
        _c: &CancellationToken,
    ) -> AdapterResult<()> {
        self.wrote.fetch_add(1, Ordering::SeqCst);
        self.record("respond");
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
        // Hold the slot long enough that a join_all would show up as a peak of 200.
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);

        let id = gate.to_string();
        let fault = self
            .faults
            .lock()
            .expect("lock")
            .get(&id)
            .copied()
            .unwrap_or(Fault::Ok);
        match fault {
            Fault::Ok => {
                self.responded.lock().expect("lock").push(id);
                Ok(())
            }
            Fault::Conflict409 => Err(AdapterError::from_status(409, "PATCH", "already answered")),
            Fault::Server500 => Err(AdapterError::from_status(500, "PATCH", "scheduler fell over")),
            Fault::Timeout => Err(AdapterError::Timeout {
                what: "PATCH gate".into(),
                after: Duration::from_secs(30),
            }),
            Fault::Transport => Err(AdapterError::Unreachable {
                what: "airflow".into(),
                detail: "connection reset by peer".into(),
            }),
            Fault::Panic => panic!("write task panicked"),
        }
    }

    async fn trigger(
        &self,
        _d: &str,
        _i: &[String],
        _c: &CancellationToken,
    ) -> AdapterResult<String> {
        Ok("r1".into())
    }

    fn run_url(&self, _r: &RunRef) -> String {
        String::new()
    }
}

fn gate_at(dag: &str, run: &str, task: &str, index: i32) -> Gate {
    Gate::new(
        dag,
        run,
        task,
        index,
        format!("Approve {task} #{index}?"),
        "the evidence",
        None,
        vec!["Approve".into(), "Reject".into()],
    )
}

/// `n` gates on one run, the first `parked` of them awaiting_input and the rest queued.
fn rig_with(n: i32, parked: i32) -> Rig {
    let mut gates = Vec::new();
    let mut tasks = Vec::new();
    for i in 0..n {
        gates.push(gate_at("factory", "r1", "job.approve_plan", i));
        let state = if i < parked { "awaiting_input" } else { "queued" };
        tasks.push(TaskState::new("job.approve_plan", i, Some(state.into())));
    }
    let mut map = HashMap::new();
    map.insert("factory/r1".to_string(), tasks);
    Rig {
        gates: Mutex::new(gates),
        tasks: Mutex::new(map),
        ..Rig::default()
    }
}

fn ops_for(rig: Arc<Rig>) -> Ops {
    Ops::builder(Context::new("test", "http://airflow.test"))
        .runs(rig)
        .build()
}

async fn select(ops: &Ops, filter: &GateFilter) -> Selection {
    ops.gates_selection(filter, &CancellationToken::new())
        .await
        .expect("selectable")
}

async fn run_batch(ops: &Ops, selection: &Selection, filter: &GateFilter) -> BatchReport {
    ops.gate_answer_all(
        selection,
        Decision::Approve,
        filter,
        &CancellationToken::new(),
    )
    .await
    .expect("a batch reports")
}

// ------------------------------------------------------------------ 2. dry run writes nothing

#[tokio::test]
async fn a_dry_run_over_the_real_batch_shape_reaches_no_write_at_all() {
    let rig = Arc::new(rig_with(20, 20));
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    let report = BatchReport::dry(&selection, Decision::Approve, &filter);
    assert_eq!(report.count(BatchOutcome::Planned), 20);
    assert_eq!(rig.wrote.load(Ordering::SeqCst), 0, "a dry run wrote");
    assert!(!rig.calls().contains(&"respond".to_string()));
}

// ------------------------------------------------------------------ 3. readiness cannot be beaten

#[tokio::test]
async fn a_batch_never_answers_a_gate_that_has_not_parked() {
    let rig = Arc::new(rig_with(10, 4)); // 4 parked, 6 queued
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    assert_eq!(selection.len(), 10);
    assert_eq!(selection.ready_count(), 4);
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.count(BatchOutcome::Answered), 4);
    assert_eq!(report.count(BatchOutcome::Skipped), 6);
    assert_eq!(rig.responded().len(), 4);
    for id in rig.responded() {
        let idx: i32 = id
            .split('#')
            .nth(1)
            .and_then(|s| s.split(':').next())
            .and_then(|s| s.parse().ok())
            .expect("index");
        assert!(idx < 4, "answered an unparked gate: {id}");
    }
    assert_eq!(report.exit_code(), 0);
}

#[tokio::test]
async fn a_filter_that_matches_only_unparked_gates_writes_nothing() {
    let rig = Arc::new(rig_with(10, 0)); // nothing parked at all
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter {
        gate: Some("plan".into()),
        ..GateFilter::default()
    };
    let selection = select(&ops, &filter).await;
    assert_eq!(selection.len(), 10);
    assert_eq!(selection.ready_count(), 0);
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.count(BatchOutcome::Skipped), 10);
    assert_eq!(rig.wrote.load(Ordering::SeqCst), 0);
    assert_eq!(report.exit_code(), 0);
}

#[tokio::test]
async fn an_empty_match_set_writes_nothing_and_exits_zero() {
    let rig = Arc::new(rig_with(10, 10));
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter {
        dag: Some("nope".into()),
        ..GateFilter::default()
    };
    let selection = select(&ops, &filter).await;
    assert!(selection.is_empty(), "a typo must not select the factory");
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.matched(), 0);
    assert_eq!(rig.wrote.load(Ordering::SeqCst), 0);
    assert_eq!(report.exit_code(), 0);
}

#[tokio::test]
async fn a_limit_never_promotes_an_unparked_gate_into_the_answered_set() {
    // 3 parked then 7 queued; --limit 5 takes the first five, of which only three are ready.
    let rig = Arc::new(rig_with(10, 3));
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter {
        limit: Some(5),
        ..GateFilter::default()
    };
    let selection = select(&ops, &filter).await;
    assert_eq!(selection.len(), 5);
    assert!(selection.truncated, "the limit cut a row and must say so");
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.count(BatchOutcome::Answered), 3);
    assert_eq!(report.count(BatchOutcome::Skipped), 2);
    assert!(report.truncated);
}

#[tokio::test]
async fn a_gate_that_stops_being_parked_between_the_selection_and_the_write_is_not_answered() {
    // Ready at selection time; every task flips to `running` before the per-gate re-read.
    let mut rig = rig_with(6, 6);
    rig.unpark_after = Some(1); // the selection is call 1; every re-read after it sees `running`
    let rig = Arc::new(rig);
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    assert_eq!(selection.ready_count(), 6, "all six looked ready when read");
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(
        rig.wrote.load(Ordering::SeqCst),
        0,
        "the re-read must stop every one of them"
    );
    assert_eq!(report.count(BatchOutcome::Answered), 0);
    assert_eq!(report.count(BatchOutcome::Skipped), 6);
    assert_eq!(report.exit_code(), 0);
}

#[tokio::test]
async fn a_run_whose_task_states_cannot_be_read_leaves_its_gates_unanswerable() {
    // `tasks` map is empty: no task instance for any gate, so nothing is provably parked.
    let rig = Arc::new(Rig {
        gates: Mutex::new((0..5).map(|i| gate_at("factory", "r1", "job.approve_plan", i)).collect()),
        ..Rig::default()
    });
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    assert_eq!(selection.ready_count(), 0);
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(rig.wrote.load(Ordering::SeqCst), 0);
    assert_eq!(report.count(BatchOutcome::Skipped), 5);
}

// ------------------------------------------------------------------ 4. partial failure

async fn batch_with_fault(fault: Fault) -> (Arc<Rig>, BatchReport) {
    let rig = rig_with(5, 5);
    rig.faults
        .lock()
        .expect("lock")
        .insert("factory/r1#2:job.approve_plan".to_string(), fault);
    let rig = Arc::new(rig);
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    let report = run_batch(&ops, &selection, &filter).await;
    (rig, report)
}

#[tokio::test]
async fn gate_three_of_five_failing_never_costs_the_other_four() {
    for (fault, name, expect_outcome, expect_code) in [
        (Fault::Conflict409, "409", BatchOutcome::Conflict, 0),
        (Fault::Server500, "500", BatchOutcome::Failed, 1),
        (Fault::Timeout, "timeout", BatchOutcome::Failed, 5),
        (Fault::Transport, "transport", BatchOutcome::Failed, 5),
    ] {
        let (rig, report) = batch_with_fault(fault).await;
        assert_eq!(
            report.count(BatchOutcome::Answered),
            4,
            "{name}: the other four must still be answered"
        );
        assert_eq!(rig.responded().len(), 4, "{name}");
        let bad = report
            .items
            .iter()
            .find(|i| i.id.to_string() == "factory/r1#2:job.approve_plan")
            .expect("the bad gate keeps its own line");
        assert_eq!(bad.outcome, expect_outcome, "{name}: {bad:?}");
        assert_eq!(report.exit_code(), expect_code, "{name}");
        assert_eq!(report.matched(), 5, "{name}: every gate keeps a line");
    }
}

#[tokio::test]
async fn a_mixed_batch_of_failures_flattens_to_one_and_a_lone_kind_keeps_its_code() {
    let rig = rig_with(5, 5);
    {
        let mut f = rig.faults.lock().expect("lock");
        f.insert("factory/r1#1:job.approve_plan".into(), Fault::Server500);
        f.insert("factory/r1#3:job.approve_plan".into(), Fault::Timeout);
    }
    let rig = Arc::new(rig);
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.count(BatchOutcome::Answered), 3);
    assert_eq!(report.failures(), 2);
    assert_eq!(report.exit_code(), 1, "two different causes are a plain 1");
}

#[tokio::test]
async fn every_gate_conflicting_is_still_exit_zero() {
    let rig = rig_with(4, 4);
    {
        let mut f = rig.faults.lock().expect("lock");
        for i in 0..4 {
            f.insert(format!("factory/r1#{i}:job.approve_plan"), Fault::Conflict409);
        }
    }
    let rig = Arc::new(rig);
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.count(BatchOutcome::Conflict), 4);
    assert_eq!(report.failures(), 0);
    assert_eq!(report.exit_code(), 0, "a lost race is the system working");
}

// ------------------------------------------------------------------ 5. concurrency is bounded

#[tokio::test]
async fn two_hundred_gates_never_put_more_than_the_bound_in_flight() {
    let rig = Arc::new(rig_with(200, 200));
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    assert_eq!(selection.ready_count(), 200);
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.count(BatchOutcome::Answered), 200);
    let peak = rig.peak_in_flight.load(Ordering::SeqCst);
    assert!(
        peak <= swf_app::gates::BATCH_CONCURRENCY,
        "{peak} writes in flight against a bound of {}",
        swf_app::gates::BATCH_CONCURRENCY
    );
    assert!(peak > 1, "a bound of one would be a serial loop, not a batch");
}

// ------------------------------------------------------------------ a panicking answer

#[tokio::test]
async fn a_panicking_answer_is_reported_as_a_failure_and_not_as_merely_unanswered() {
    let rig = rig_with(4, 4);
    rig.faults
        .lock()
        .expect("lock")
        .insert("factory/r1#1:job.approve_plan".into(), Fault::Panic);
    let rig = Arc::new(rig);
    let ops = ops_for(Arc::clone(&rig));
    let filter = GateFilter::default();
    let selection = select(&ops, &filter).await;
    let report = run_batch(&ops, &selection, &filter).await;
    assert_eq!(report.count(BatchOutcome::Answered), 3);
    let bad = report
        .items
        .iter()
        .find(|i| i.id.to_string() == "factory/r1#1:job.approve_plan")
        .expect("line");
    assert_eq!(
        bad.outcome,
        BatchOutcome::Failed,
        "a task that died mid-write is not a gate that was merely skipped: {bad:?}"
    );
    assert_ne!(report.exit_code(), 0, "a dead write task must not exit 0");
}

// unused-trait silencers
#[allow(dead_code)]
fn _unused(_: &dyn Deliveries, _: &dyn Sandboxes) {}
