//! The operations both interfaces call, and the one place an error becomes an exit code.
//!
//! `Ops` holds the resolved [`Context`] and the adapters that context implies. Every command the
//! CLI exposes and every keystroke the TUI binds goes through a method here, which is what makes
//! the two faces of the product the same product: there is no validation in the argument parser
//! and none in a widget, so neither can be skipped by using the other.
//!
//! The boundary this module defends is the exit-code table (`00-architecture.md` §6, §C.2). It is
//! documented public surface — a script that sees `4` must know it needs a credential and not a
//! retry — so classification happens once, in [`OpsError`], and the renderer only prints what it
//! is handed. An adapter's `AdapterError`, a config error, a bad identity and a refused blueprint
//! all arrive here and leave with the same six-word vocabulary.
//!
//! Adapters are `Option` on purpose. A context with no `repo` has no GitHub adapter, and that is
//! *not configured* rather than *broken*: the two must never render the same way, so asking for a
//! source that was never wired up is an operational error naming the setting that is missing.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use swf_adapters::airflow::AirflowApi;
use swf_adapters::error::AdapterError;
use swf_adapters::factory::FactoryApi;
use swf_adapters::gh::GhCli;
use swf_adapters::islo::IsloCli;
use swf_adapters::metrics_store::FsMetrics;
use swf_adapters::traits::{
    CommandRunner, Deliveries, MetricsStore, Runs, Sandboxes, SystemRunner, DEFAULT_HTTP_TIMEOUT,
};
use swf_domain::blueprint::BlueprintError;
use swf_domain::doctor::Check;
use swf_domain::evidence::DeliveryReport;
use swf_domain::ids::{DeliveryId, GateId, IdError, JobId, RunRef};
use swf_domain::metrics::{MetricsSummary, RunMetrics};
use swf_domain::model::{Gate, JobRow, Run, SandboxRef, Snapshot, SourceError, SOURCE_AIRFLOW};
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::attention::Attention;
use crate::context::{Context, ContextError};
use crate::delivery::{Delivery, VerifyOpts};
use crate::gates::{
    AnswerOpts, BatchReport, Decision, GateAnswer, GateFilter, GateList, GateReview, Selection,
    Sightings,
};
use crate::logs::{LogOpts, LogStream};
use crate::snapshot::{CollectOpts, Sources};
use crate::stack::{StackAction, StackOpts, StackStatus};
use crate::submit::{Submission, SubmitRequest};

/// The six outcomes the exit-code table names, plus the one that never reaches a process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// A check is red, a gate answer was refused by policy, verification was refuted. Exit 1.
    Operational,
    /// The operator asked for something that is not a request. Exit 2.
    Usage,
    /// No such run, job, gate, delivery, sandbox or context. Exit 3.
    NotFound,
    /// A credential is missing, expired or rejected. Exit 4.
    Auth,
    /// Nothing answered: DNS, connect, TLS, a timeout, a missing tool. Exit 5.
    Unreachable,
    /// Someone else got there first, or the evidence moved. Exit 6.
    Conflict,
    /// The caller stopped wanting the answer. Never rendered as a failure.
    Cancelled,
}

impl ErrorKind {
    /// The `kind` string of the `--json` error envelope.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Operational | Self::Cancelled => "operational",
            Self::Usage => "usage",
            Self::NotFound => "not_found",
            Self::Auth => "auth",
            Self::Unreachable => "unreachable",
            Self::Conflict => "conflict",
        }
    }

    /// The process exit code.
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Operational | Self::Cancelled => 1,
            Self::Usage => 2,
            Self::NotFound => 3,
            Self::Auth => 4,
            Self::Unreachable => 5,
            Self::Conflict => 6,
        }
    }
}

/// One failed operation, already classified.
///
/// `message` is one sentence and is sanitised on construction: half of these strings started life
/// as a service payload, and a log line that repositions the cursor is a security bug (rule 7).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct OpsError {
    /// Which of the six this is.
    pub kind: ErrorKind,
    /// What went wrong, in one sentence.
    pub message: String,
    /// The `fix:` line, when there is an obvious one.
    pub hint: Option<String>,
}

/// Every operation answers this.
pub type Result<T> = std::result::Result<T, OpsError>;

impl OpsError {
    /// Build an error of a given kind.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: sanitize_line(&message.into()),
            hint: None,
        }
    }

    /// A check is red, a policy refused, verification was refuted. Exit 1.
    pub fn operational(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Operational, message)
    }

    /// The request itself does not make sense. Exit 2.
    pub fn usage(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Usage, message)
    }

    /// The thing addressed is not there. Exit 3.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    /// A credential problem. Exit 4.
    pub fn auth(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Auth, message)
    }

    /// Nothing answered. Exit 5.
    pub fn unreachable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unreachable, message)
    }

    /// Someone else got there first. Exit 6.
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }

    /// The caller abandoned the call.
    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "cancelled")
    }

    /// Attach the literal command that would fix this.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(sanitize_line(&hint.into()));
        self
    }

    /// The `kind` string of the `--json` envelope.
    pub fn kind(&self) -> &'static str {
        self.kind.as_str()
    }

    /// The process exit code this error implies.
    pub fn exit_code(&self) -> i32 {
        self.kind.exit_code()
    }

    /// True when this was the caller's own cancellation and must not be shown as a failure.
    pub fn is_cancelled(&self) -> bool {
        self.kind == ErrorKind::Cancelled
    }

    /// The complete `--json` error document (`00-architecture.md` §C.2).
    ///
    /// `exit_code` is duplicated inside the object on purpose, so a piped consumer never has to
    /// inspect `$?` to find out what happened.
    pub fn envelope(&self) -> serde_json::Value {
        let mut error = serde_json::Map::new();
        error.insert("kind".into(), self.kind().into());
        error.insert("message".into(), self.message.clone().into());
        error.insert("exit_code".into(), self.exit_code().into());
        if let Some(hint) = &self.hint {
            error.insert("hint".into(), hint.clone().into());
        }
        serde_json::json!({ "error": serde_json::Value::Object(error) })
    }
}

impl From<AdapterError> for OpsError {
    fn from(err: AdapterError) -> Self {
        let message = err.to_string();
        match err {
            AdapterError::Unreachable { .. } | AdapterError::Timeout { .. } => {
                Self::unreachable(message)
            }
            AdapterError::Auth { .. } => Self::auth(message)
                .with_hint("check the credential this context names: swf context show"),
            AdapterError::NotFound { .. } => Self::not_found(message),
            AdapterError::Conflict { .. } => Self::conflict(message),
            AdapterError::Cancelled => Self::cancelled(),
            AdapterError::Status { .. } | AdapterError::Decode { .. } => Self::operational(message),
        }
    }
}

impl From<ContextError> for OpsError {
    fn from(err: ContextError) -> Self {
        let hint = err.hint();
        let mut out = match err.exit_code() {
            3 => Self::not_found(err.to_string()),
            4 => Self::auth(err.to_string()),
            _ => Self::operational(err.to_string()),
        };
        if let Some(hint) = hint {
            out = out.with_hint(hint);
        }
        out
    }
}

impl From<BlueprintError> for OpsError {
    fn from(err: BlueprintError) -> Self {
        Self::operational(err.to_string())
    }
}

impl From<IdError> for OpsError {
    // An identity that does not parse is a malformed request, not a missing object: the operator
    // typed something that could never name anything (§C.2, `usage`).
    fn from(err: IdError) -> Self {
        Self::usage(err.to_string())
    }
}

/// Which mapped jobs an operator means.
///
/// It lives beside the operation rather than in the argument parser because the TUI's search box
/// has to narrow the same rows by the same rules: a filter written in `clap` would be a filter one
/// of the two faces of the product could not apply.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobFilter {
    /// One DAG id. This one narrows the *read*: the other DAGs are never asked about.
    pub dag: Option<String>,
    /// The rolled-up job state, e.g. `failed` or `running`.
    pub state: Option<String>,
    /// The issue reference the job answers.
    pub issue: Option<String>,
    /// Only jobs that need a person: failed, or waiting on a gate.
    pub attention: bool,
    /// At most this many rows. A listing the limit cut says so (rule 3).
    pub limit: Option<usize>,
}

impl JobFilter {
    /// True when this row survives every field that was set.
    pub fn matches(&self, job: &JobRow, waiting: &[JobId]) -> bool {
        if self.attention
            && !swf_domain::states::is_failed(&job.state)
            && !waiting.contains(&job.id())
        {
            return false;
        }
        if let Some(state) = &self.state {
            if !job.state.eq_ignore_ascii_case(state.trim()) {
                return false;
            }
        }
        if let Some(issue) = &self.issue {
            if !job.issue.trim().eq_ignore_ascii_case(issue.trim()) {
                return false;
            }
        }
        true
    }
}

/// The jobs one pass found, whether that is all of them, and what could not be read.
#[derive(Debug, Clone, Default)]
pub struct JobList {
    /// The rows themselves.
    pub jobs: Vec<JobRow>,
    /// True when a page bound or the limit shortened the listing.
    pub truncated: bool,
    /// Per-source failures, so a short table is never mistaken for a quiet factory (rule 4).
    pub errors: Vec<SourceError>,
}

/// The runs one listing found, and whether it is the whole listing.
#[derive(Debug, Clone, Default)]
pub struct RunList {
    /// The rows themselves.
    pub runs: Vec<Run>,
    /// True when the read stopped at a page bound or at `--limit`.
    pub truncated: bool,
}

/// Everything both interfaces can do, over one environment.
pub struct Ops {
    context: Context,
    backend: Option<Arc<FactoryApi>>,
    runs: Option<Arc<dyn Runs>>,
    deliveries: Option<Arc<dyn Deliveries>>,
    sandboxes: Option<Arc<dyn Sandboxes>>,
    metrics: Option<Arc<dyn MetricsStore>>,
    commands: Arc<dyn CommandRunner>,
    /// Shared rather than owned: a bulk answer runs its writes as tasks, and every one of them has
    /// to count sightings into the same table the selection that preceded it wrote to.
    sightings: Arc<Sightings>,
    collect: CollectOpts,
    stack: StackOpts,
}

/// Assemble an [`Ops`] from whichever adapters a caller has.
///
/// Tests use it to drive the whole product with four small structs and no network; the TUI uses it
/// to rebuild the world when the operator switches context.
pub struct OpsBuilder {
    ops: Ops,
}

impl OpsBuilder {
    /// Use this Airflow.
    pub fn runs(mut self, runs: Arc<dyn Runs>) -> Self {
        self.ops.runs = Some(runs);
        self
    }

    /// Use this GitHub.
    pub fn deliveries(mut self, deliveries: Arc<dyn Deliveries>) -> Self {
        self.ops.deliveries = Some(deliveries);
        self
    }

    /// Use this sandbox provider.
    pub fn sandboxes(mut self, sandboxes: Arc<dyn Sandboxes>) -> Self {
        self.ops.sandboxes = Some(sandboxes);
        self
    }

    /// Use this metrics store.
    pub fn metrics(mut self, metrics: Arc<dyn MetricsStore>) -> Self {
        self.ops.metrics = Some(metrics);
        self
    }

    /// Run subprocesses through this runner instead of forking real ones.
    pub fn commands(mut self, commands: Arc<dyn CommandRunner>) -> Self {
        self.ops.commands = commands;
        self
    }

    /// Override the collection policy (`runs_per_dag`, `jobs_per_dag`, the PR label).
    pub fn collect_opts(mut self, opts: CollectOpts) -> Self {
        self.ops.collect = opts;
        self
    }

    /// Override where the local stack lives.
    pub fn stack_opts(mut self, opts: StackOpts) -> Self {
        self.ops.stack = opts;
        self
    }

    /// Finish.
    pub fn build(self) -> Ops {
        self.ops
    }
}

impl Ops {
    /// Start from a context and no adapters at all.
    pub fn builder(context: Context) -> OpsBuilder {
        let collect = CollectOpts::default()
            .for_dags(&context.dag_ids)
            .tagged(&context.dag_tag);
        OpsBuilder {
            ops: Self {
                context,
                backend: None,
                runs: None,
                deliveries: None,
                sandboxes: None,
                metrics: None,
                commands: Arc::new(SystemRunner),
                sightings: Arc::new(Sightings::default()),
                collect,
                stack: StackOpts::default(),
            },
        }
    }

    /// Build the adapters a context implies and connect them.
    ///
    /// The credential is read here, once, and a missing environment variable fails now rather than
    /// on the third pane's refresh. `repo` and `owner` are optional and their absence is silent:
    /// there is nothing wrong with a context that only watches Airflow.
    pub fn connect(context: Context) -> Result<Self> {
        Self::connect_with_timeout(context, DEFAULT_HTTP_TIMEOUT)
    }

    /// The same, with the HTTP deadline `--timeout` sets. It never bounds a subprocess (§C.6).
    pub fn connect_with_timeout(mut context: Context, timeout: Duration) -> Result<Self> {
        let backend_url =
            std::env::var("SWF_BACKEND_URL").unwrap_or_else(|_| context.backend_url.clone());
        context.backend_url.clone_from(&backend_url);
        if !backend_url.is_empty() {
            let token = std::env::var("SWF_BACKEND_TOKEN").unwrap_or_default();
            let backend = Arc::new(FactoryApi::new(&backend_url, token, timeout)?);
            let airflow = backend.runs(&context.airflow_url)?;
            let mut ops = Self::builder(context)
                .runs(Arc::new(airflow))
                .deliveries(backend.clone())
                .sandboxes(backend.clone())
                .metrics(backend.clone())
                .build();
            ops.backend = Some(backend);
            return Ok(ops);
        }
        let auth = context.airflow_auth()?;
        let airflow = AirflowApi::new(&context.airflow_url, auth, timeout)?;
        let commands: Arc<dyn CommandRunner> = Arc::new(SystemRunner);
        let mut builder = Self::builder(context.clone())
            .runs(Arc::new(airflow))
            .commands(Arc::clone(&commands))
            .metrics(Arc::new(FsMetrics::new(context.metrics_path())));
        if let Some(repo) = &context.repo {
            builder = builder.deliveries(Arc::new(GhCli::new(repo, Arc::clone(&commands))));
        }
        if let Some(owner) = &context.owner {
            builder = builder.sandboxes(Arc::new(IsloCli::new(owner, Arc::clone(&commands))));
        }
        Ok(builder.build())
    }

    /// The environment these operations run against.
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// The collection policy in force.
    pub fn collect_opts(&self) -> &CollectOpts {
        &self.collect
    }

    /// Airflow, or an error naming the setting that would provide it.
    pub fn runs(&self) -> Result<&dyn Runs> {
        self.runs
            .as_deref()
            .ok_or_else(|| OpsError::operational("no Airflow is configured for this context"))
    }

    /// The same adapter, shareable — what a bulk answer hands to each of its tasks.
    pub fn runs_shared(&self) -> Result<Arc<dyn Runs>> {
        self.runs
            .clone()
            .ok_or_else(|| OpsError::operational("no Airflow is configured for this context"))
    }

    /// GitHub, or an error naming the setting that would provide it.
    pub fn deliveries(&self) -> Result<&dyn Deliveries> {
        self.deliveries.as_deref().ok_or_else(|| {
            OpsError::operational(format!(
                "context {:?} has no repo, so there is no GitHub to ask",
                self.context.name
            ))
            .with_hint("swf context add … --repo owner/name")
        })
    }

    /// The sandbox provider, or an error naming the setting that would provide it.
    pub fn sandboxes(&self) -> Result<&dyn Sandboxes> {
        self.sandboxes.as_deref().ok_or_else(|| {
            OpsError::operational(format!(
                "context {:?} has no sandbox owner, so sandboxes cannot be listed or removed",
                self.context.name
            ))
            .with_hint("swf context add … --owner you@example.com")
        })
    }

    /// The committed metrics history.
    pub fn metrics_store(&self) -> Result<&dyn MetricsStore> {
        self.metrics
            .as_deref()
            .ok_or_else(|| OpsError::operational("no metrics root is configured"))
    }

    /// One pass over every source. Never fails: per-source errors ride in the snapshot.
    pub async fn snapshot(&self, opts: &CollectOpts, cancel: &CancellationToken) -> Snapshot {
        let sources = Sources {
            airflow: self.runs.as_deref(),
            github: self.deliveries.as_deref(),
            islo: self.sandboxes.as_deref(),
            metrics: self.metrics.as_deref(),
        };
        crate::snapshot::collect(sources, opts, Utc::now(), cancel).await
    }

    /// A pass with this context's own policy.
    pub async fn snapshot_default(&self, cancel: &CancellationToken) -> Snapshot {
        self.snapshot(&self.collect.clone(), cancel).await
    }

    /// What needs a human right now.
    pub async fn attention(&self, cancel: &CancellationToken) -> Attention {
        let snap = self.snapshot_default(cancel).await;
        Attention::from_snapshot(&snap, Utc::now())
    }

    /// The readiness report for this machine. Never fails — a failing check *is* the answer.
    pub async fn doctor(&self, cancel: &CancellationToken) -> Vec<Check> {
        if let Some(backend) = &self.backend {
            return match backend.call("/doctor", serde_json::json!({}), cancel).await {
                Ok(checks) => checks,
                Err(error) => vec![Check::fail(
                    "factory backend",
                    error.to_string(),
                    "start swfactory backend and check SWF_BACKEND_TOKEN",
                )],
            };
        }
        crate::doctor::checks(
            &self.context,
            self.runs.as_deref(),
            self.commands.as_ref(),
            cancel,
        )
        .await
    }

    /// Send governed work to Airflow.
    pub async fn submit(
        &self,
        request: &SubmitRequest,
        cancel: &CancellationToken,
    ) -> Result<Submission> {
        if let Some(backend) = &self.backend {
            let mut submitted: Submission = backend
                .call(
                    "/work-orders",
                    serde_json::json!({
                        "line": request.blueprint,
                        "issues": request.issues,
                        "targets": request.targets,
                    }),
                    cancel,
                )
                .await?;
            // The backend may see an internal Airflow hostname. Browser links use the context.
            submitted.url = self.runs()?.run_url(&submitted.run());
            return Ok(submitted);
        }
        crate::submit::submit(self.runs()?, request, cancel).await
    }

    /// Every gate awaiting an answer, readiness established.
    pub async fn gates(&self, cancel: &CancellationToken) -> Result<GateList> {
        self.gates_matching(&GateFilter::default(), cancel).await
    }

    /// The gates one filter selects, readiness established.
    pub async fn gates_matching(
        &self,
        filter: &GateFilter,
        cancel: &CancellationToken,
    ) -> Result<GateList> {
        Ok(self.gates_selection(filter, cancel).await?.into_list())
    }

    /// The same selection, keeping the facts a batch needs to explain what it skipped.
    pub async fn gates_selection(
        &self,
        filter: &GateFilter,
        cancel: &CancellationToken,
    ) -> Result<Selection> {
        let selection = crate::gates::select(self.runs()?, filter, cancel).await?;
        for row in selection.rows.iter().filter(|row| row.gate.ready) {
            // Sighting a ready gate on a plain listing counts: it is what lets an operator who has
            // been looking at the screen answer without waiting for a confirming poll — and what
            // keeps a batch from paying a confirmation delay per gate for a set it just read.
            self.sightings.record(&row.gate.id());
        }
        Ok(selection)
    }

    /// The evidence for one gate, and the revision to hand back to [`Ops::gate_answer`].
    pub async fn gate_review(&self, id: &GateId, cancel: &CancellationToken) -> Result<GateReview> {
        let review = crate::gates::review(self.runs()?, id, cancel).await?;
        if review.ready {
            self.sightings.record(id);
        }
        Ok(review)
    }

    /// Answer one gate, re-validating immediately before the write.
    pub async fn gate_answer(
        &self,
        id: &GateId,
        decision: Decision,
        opts: &AnswerOpts,
        cancel: &CancellationToken,
    ) -> Result<GateAnswer> {
        crate::gates::answer(self.runs()?, &self.sightings, id, decision, opts, cancel).await
    }

    /// Answer every gate in a selection, one outcome per gate.
    ///
    /// The selection is taken as an argument rather than re-read here because the caller has to be
    /// able to show it — and be told to confirm it — between the read and the writes. A dry run is
    /// simply this method never being called.
    pub async fn gate_answer_all(
        &self,
        selection: &Selection,
        decision: Decision,
        filter: &GateFilter,
        cancel: &CancellationToken,
    ) -> Result<BatchReport> {
        let runs = self.runs_shared()?;
        Ok(crate::gates::answer_all(
            runs,
            Arc::clone(&self.sightings),
            selection,
            decision,
            filter,
            cancel,
        )
        .await)
    }

    /// One poll of one task attempt's log.
    pub async fn logs(
        &self,
        id: &JobId,
        opts: &LogOpts,
        cancel: &CancellationToken,
    ) -> Result<LogStream> {
        crate::logs::fetch(self.runs()?, id, opts, cancel).await
    }

    /// Follow a log, emitting only what is new. Returns how many lines were emitted.
    pub async fn follow_logs(
        &self,
        id: &JobId,
        opts: &LogOpts,
        sink: &mut dyn FnMut(&str),
        cancel: &CancellationToken,
    ) -> Result<usize> {
        crate::logs::follow(self.runs()?, id, opts, sink, cancel).await
    }

    /// What the factory has published.
    pub async fn deliveries_list(&self, cancel: &CancellationToken) -> Result<Vec<Delivery>> {
        crate::delivery::list(
            self.deliveries()?,
            &self.collect.pr_label,
            self.collect.pr_limit,
            cancel,
        )
        .await
    }

    /// Verify one delivery. The three claims stay three claims (non-negotiable 10).
    pub async fn verify_delivery(
        &self,
        id: &DeliveryId,
        opts: &VerifyOpts,
        cancel: &CancellationToken,
    ) -> Result<DeliveryReport> {
        crate::delivery::verify(
            self.deliveries.as_deref(),
            self.metrics.as_deref(),
            self.commands.as_ref(),
            &self.context,
            id,
            opts,
            cancel,
        )
        .await
    }

    /// The sandboxes the configured owner created.
    pub async fn sandboxes_list(&self, cancel: &CancellationToken) -> Result<Vec<SandboxRef>> {
        Ok(self.sandboxes()?.list(cancel).await?)
    }

    /// Remove one sandbox, after the adapter re-reads the provider's listing to confirm it may.
    pub async fn remove_sandbox(
        &self,
        name: &str,
        cancel: &CancellationToken,
    ) -> Result<Vec<String>> {
        Ok(self.sandboxes()?.remove(name, cancel).await?)
    }

    /// Drive the local development stack, and report what is actually running.
    pub async fn stack(
        &self,
        action: StackAction,
        cancel: &CancellationToken,
    ) -> Result<StackStatus> {
        crate::stack::run(
            self.commands.as_ref(),
            self.runs.as_deref(),
            action,
            &self.stack,
            cancel,
        )
        .await
    }

    /// The newest runs of one DAG.
    pub async fn runs_list(
        &self,
        dag_id: &str,
        limit: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<Run>> {
        Ok(self.runs()?.list_runs(dag_id, limit, cancel).await?.rows)
    }

    /// The newest runs of one DAG, narrowed by state, saying whether that is all of them.
    ///
    /// `limit` bounds the *read* — it is how many runs of this DAG are asked for — and `state`
    /// then narrows what was read. A read that came back full has to be reported as shortened:
    /// Airflow's cursor mode returns no total, so "there are exactly this many" is a claim this
    /// client cannot make, and quietly implying it is the bug this whole binary replaces.
    pub async fn runs_matching(
        &self,
        dag_id: &str,
        limit: usize,
        state: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<RunList> {
        let page = self.runs()?.list_runs(dag_id, limit, cancel).await?;
        let read = page.rows.len();
        let runs = match state {
            None => page.rows,
            Some(want) => page
                .rows
                .into_iter()
                .filter(|run| run.state.eq_ignore_ascii_case(want.trim()))
                .collect(),
        };
        Ok(RunList {
            runs,
            truncated: page.truncated || read >= limit,
        })
    }

    /// Every mapped job the filter selects, out of one pass over the sources.
    ///
    /// Never fails, for the same reason [`Ops::snapshot`] does not: a dead `gh` must not turn a
    /// jobs listing into an error, and the sources that did answer are still the answer.
    pub async fn jobs_matching(&self, filter: &JobFilter, cancel: &CancellationToken) -> JobList {
        let mut opts = self.collect.clone();
        if let Some(dag) = &filter.dag {
            // The one filter this API can push down: a DAG the operator excluded is a DAG whose
            // runs — and whose two calls per run — are never fetched at all.
            opts.dag_ids = Some(vec![dag.trim().to_string()]);
        }
        let snap = self.snapshot(&opts, cancel).await;
        let waiting: Vec<JobId> = snap.gates.iter().map(Gate::job).collect();
        let mut jobs: Vec<JobRow> = snap
            .jobs()
            .filter(|job| filter.matches(job, &waiting))
            .cloned()
            .collect();
        let mut truncated = snap
            .health
            .get(SOURCE_AIRFLOW)
            .is_some_and(|health| health.truncated);
        if let Some(limit) = filter.limit {
            if jobs.len() > limit {
                jobs.truncate(limit);
                truncated = true;
            }
        }
        JobList {
            jobs,
            truncated,
            errors: snap.errors,
        }
    }

    /// The job rows of one run.
    pub async fn job_rows(&self, run: &RunRef, cancel: &CancellationToken) -> Result<Vec<JobRow>> {
        Ok(self.runs()?.job_rows(run, &[], cancel).await?.rows)
    }

    /// Mark one Airflow run failed.
    ///
    /// Named for what Airflow actually does. Nothing is killed, no sandbox is cleaned up, and any
    /// task already running keeps running — non-negotiable 9, and the caller must say so too.
    pub async fn stop_run(&self, run: &RunRef, cancel: &CancellationToken) -> Result<()> {
        Ok(self.runs()?.stop_run(run, cancel).await?)
    }

    /// The UI deep link for a run.
    pub fn run_url(&self, run: &RunRef) -> Result<String> {
        Ok(self.runs()?.run_url(run))
    }

    /// The committed metrics history, oldest first.
    pub async fn metrics_runs(&self, cancel: &CancellationToken) -> Result<Vec<RunMetrics>> {
        Ok(self.metrics_store()?.runs(cancel).await?)
    }

    /// The aggregate over the committed history.
    pub async fn metrics_summary(&self, cancel: &CancellationToken) -> Result<MetricsSummary> {
        Ok(self.metrics_store()?.summary(cancel).await?)
    }

    /// One gate by identity, out of the pending list.
    pub async fn gate(&self, id: &GateId, cancel: &CancellationToken) -> Result<Gate> {
        let listing = self.gates(cancel).await?;
        listing
            .gates
            .into_iter()
            .find(|gate| gate.id() == *id)
            .ok_or_else(|| {
                OpsError::not_found(format!("no gate {id} is waiting for an answer"))
                    .with_hint("swf gates list")
            })
    }

    /// Where the local stack is expected to live.
    pub fn stack_opts(&self) -> &StackOpts {
        &self.stack
    }

    /// The scratch directory verification clones into, for a caller that wants to name it.
    pub fn scratch_root(&self) -> PathBuf {
        std::env::temp_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_kind_vocabulary_is_the_exit_table() {
        let cases = [
            (ErrorKind::Operational, "operational", 1),
            (ErrorKind::Usage, "usage", 2),
            (ErrorKind::NotFound, "not_found", 3),
            (ErrorKind::Auth, "auth", 4),
            (ErrorKind::Unreachable, "unreachable", 5),
            (ErrorKind::Conflict, "conflict", 6),
        ];
        for (kind, word, code) in cases {
            assert_eq!(kind.as_str(), word);
            assert_eq!(kind.exit_code(), code);
        }
        assert_eq!(ErrorKind::Cancelled.exit_code(), 1);
    }

    #[test]
    fn adapter_errors_keep_their_classification_across_the_boundary() {
        let cases: Vec<(AdapterError, &str, i32)> = vec![
            (
                AdapterError::from_status(401, "GET /dags", "Not authenticated"),
                "auth",
                4,
            ),
            (
                AdapterError::from_status(403, "PATCH gate", "Forbidden"),
                "operational",
                1,
            ),
            (
                AdapterError::from_status(404, "run", "gone"),
                "not_found",
                3,
            ),
            (
                AdapterError::from_status(409, "gate", "answered"),
                "conflict",
                6,
            ),
            (
                AdapterError::Unreachable {
                    what: "gh".into(),
                    detail: "no such file".into(),
                },
                "unreachable",
                5,
            ),
            (
                AdapterError::Timeout {
                    what: "gh".into(),
                    after: Duration::from_secs(120),
                },
                "unreachable",
                5,
            ),
            (AdapterError::refused("not yours"), "operational", 1),
        ];
        for (err, kind, code) in cases {
            let mapped = OpsError::from(err);
            assert_eq!(mapped.kind(), kind, "{}", mapped.message);
            assert_eq!(mapped.exit_code(), code, "{}", mapped.message);
        }
        assert!(OpsError::from(AdapterError::Cancelled).is_cancelled());
    }

    #[test]
    fn the_json_envelope_carries_its_own_exit_code() {
        let err = OpsError::conflict("someone answered it first").with_hint("swf gates list");
        let doc = err.envelope();
        assert_eq!(doc["error"]["kind"], "conflict");
        assert_eq!(doc["error"]["exit_code"], 6);
        assert_eq!(doc["error"]["hint"], "swf gates list");
        assert!(OpsError::operational("x").envelope()["error"]
            .get("hint")
            .is_none());
    }

    #[test]
    fn a_message_from_a_service_cannot_move_the_cursor() {
        let err = OpsError::operational("boom\u{1b}[2Jgone\u{7}");
        assert_eq!(err.message, "boomgone");
    }

    #[test]
    fn an_unconfigured_source_names_the_setting_rather_than_failing_like_an_outage() {
        let ops = Ops::builder(Context::builtin()).build();
        let Err(err) = ops.deliveries() else {
            panic!("a context with no repo must not answer a GitHub adapter");
        };
        assert_eq!(err.exit_code(), 1, "not configured is not unreachable");
        assert!(err.message.contains("repo"), "{}", err.message);
        assert!(err.hint.is_some());
        assert!(ops.sandboxes().is_err());
        assert!(ops.runs().is_err());
    }

    #[test]
    fn a_context_carries_its_dag_policy_into_collection() {
        let mut ctx = Context::builtin();
        ctx.dag_ids = vec!["factory".into(), "hotfix".into()];
        ctx.dag_tag = "custom".into();
        let ops = Ops::builder(ctx).build();
        assert_eq!(
            ops.collect_opts().dag_ids.as_deref(),
            Some(&["factory".to_string(), "hotfix".to_string()][..])
        );
        assert_eq!(ops.collect_opts().dag_tag, "custom");
    }
}
