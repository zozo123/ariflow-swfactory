//! Every verb, wired to the operations layer and to nothing else.
//!
//! This module is deliberately thin, and that thinness is the boundary it defends. Not one rule
//! lives here: readiness, re-validation, blueprint resolution, the removal guard and the three
//! delivery verdicts are all decided in `swf-app`, where the TUI takes exactly the same path
//! (`00-architecture.md` §0). A check written in this file would be a check a keystroke could
//! skip, and the two faces of the product would start to drift on the day it mattered most.
//!
//! What does live here is the shape of an answer: which document a command prints, which words a
//! person reads, and what the shell is told. Confirmation is asked *before* the call and never
//! after, so a mutation that was refused at the prompt never reached a service at all.

use std::sync::Arc;

use chrono::Utc;
use swf_app::context::{Auth, Context, ContextStore};
use swf_app::delivery::VerifyOpts;
use swf_app::gates::{AnswerOpts, BatchOutcome, BatchReport, Decision, GateFilter, Selection};
use swf_app::logs::LogOpts;
use swf_app::ops::{JobFilter, Ops, OpsError, Result};
use swf_app::stack::StackAction;
use swf_app::submit::SubmitRequest;
use swf_domain::doctor;
use swf_domain::evidence::DeliveryReport;
use swf_domain::ids::{DeliveryId, GateId, JobId, RunRef};
use swf_domain::model::{Gate, JobRow, Run};
use tokio_util::sync::CancellationToken;

use crate::cli::{
    AnswerArgs, Cli, Command, ContextAddArgs, ContextCmd, DeliveriesCmd, GateFilterArgs, GatesCmd,
    JobListArgs, JobsCmd, LogsArgs, MetricsArgs, RunsCmd, SandboxesCmd, StackCmd, SubmitArgs,
    VerifyArgs,
};
use crate::exit::Outcome;
use crate::json;
use crate::render;
use crate::term::{confirm, Term};

/// How many runs of a DAG are searched when the operator named one by id.
///
/// A run is addressed by identity, not by position, so the read has to be wide enough that
/// "inspect the run I submitted an hour ago" works after a busy afternoon. It is still bounded:
/// an unbounded scan for a typo'd id would hammer the API to prove a negative.
const RUN_LOOKUP_LIMIT: usize = 200;

/// How often `--wait` re-reads a run's state.
const WAIT_POLL_S: u64 = 3;

/// Everything a command needs that is not in its own arguments.
pub struct Ctx {
    /// The parsed command line.
    pub cli: Cli,
    /// What the terminal is and what the operator allowed.
    pub term: Term,
    /// Cancelled on the first Ctrl-C, so an in-flight request is abandoned rather than raced.
    pub cancel: CancellationToken,
}

impl Ctx {
    /// Say something on stderr that only a `-v` operator asked for.
    fn note(&self, message: &str) {
        if self.cli.verbose > 0 {
            eprintln!("{message}");
        }
    }

    /// Say something on stderr that everyone needs, whatever `--json` is doing to stdout.
    fn warn(&self, message: &str) {
        eprintln!("{}", self.term.warn(message));
    }

    /// The config file for this machine.
    fn store(&self) -> Result<ContextStore> {
        Ok(ContextStore::open()?)
    }

    /// The environment this invocation acts against.
    fn context(&self) -> Result<Context> {
        Ok(self.store()?.resolve(self.cli.context.as_deref())?)
    }

    /// The operations layer, connected.
    fn ops(&self) -> Result<Ops> {
        self.ops_for(self.context()?)
    }

    /// The same, for a context a command has already adjusted.
    fn ops_for(&self, context: Context) -> Result<Ops> {
        self.note(&format!(
            "context {} -> {}",
            context.name, context.airflow_url
        ));
        match self.cli.timeout {
            Some(seconds) if seconds > 0.0 => {
                Ops::connect_with_timeout(context, std::time::Duration::from_secs_f64(seconds))
            }
            Some(_) => Err(OpsError::usage("--timeout must be greater than zero")),
            None => Ops::connect(context),
        }
    }
}

/// Who Airflow will record as the respondent, as far as this client can tell.
///
/// A basic-auth context knows its username; a token context does not, because the token names the
/// user and this process never decodes it. Saying "Airflow token owner" is the honest answer, and
/// it is the same wording `herd` uses so the two control rooms echo one event the same way.
pub fn actor(context: &Context) -> String {
    if !context.backend_url.is_empty() {
        return "factory backend Airflow identity".to_string();
    }
    match &context.auth {
        Auth::Basic { user, .. } => user.clone(),
        _ => "Airflow token owner".to_string(),
    }
}

/// Run one command.
pub async fn run(ctx: &Ctx) -> Result<Outcome> {
    match &ctx.cli.command {
        Command::Version => Ok(Outcome::new(
            format!("swf {}", env!("CARGO_PKG_VERSION")),
            json::version(),
        )),
        Command::Completions(_) => Ok(Outcome::text(String::new())),
        Command::Context(cmd) => context_cmd(ctx, cmd),
        Command::Doctor => doctor_cmd(ctx).await,
        Command::Submit(args) => submit_cmd(ctx, args).await,
        Command::Attention => attention_cmd(ctx).await,
        Command::Runs(cmd) => runs_cmd(ctx, cmd).await,
        Command::Jobs(cmd) => jobs_cmd(ctx, cmd).await,
        Command::Logs(args) => logs_cmd(ctx, args).await,
        Command::Gates(cmd) => gates_cmd(ctx, cmd).await,
        Command::Deliveries(cmd) => deliveries_cmd(ctx, cmd).await,
        Command::Sandboxes(cmd) => sandboxes_cmd(ctx, cmd).await,
        Command::Metrics(args) => metrics_cmd(ctx, args).await,
        Command::Snapshot => snapshot_cmd(ctx).await,
        Command::Stack(cmd) => stack_cmd(ctx, cmd).await,
        Command::Tui => tui_cmd(ctx).await,
    }
}

// ---------------------------------------------------------------------------- context

fn context_cmd(ctx: &Ctx, cmd: &ContextCmd) -> Result<Outcome> {
    match cmd {
        ContextCmd::List => {
            let store = ctx.store()?;
            let active = store.resolve(ctx.cli.context.as_deref())?;
            let all = store.list();
            let docs: Vec<_> = all
                .iter()
                .map(|c| json::context(c, c.name == active.name))
                .collect();
            Ok(Outcome::new(
                render::contexts(&all, &active.name, &ctx.term),
                serde_json::Value::Array(docs),
            ))
        }
        ContextCmd::Show { name } => {
            let store = ctx.store()?;
            let context = match name {
                Some(name) => store
                    .get(name)
                    .ok_or_else(|| OpsError::not_found(format!("no context named {name:?}")))?,
                None => store.resolve(ctx.cli.context.as_deref())?,
            };
            Ok(Outcome::new(
                swf_app::context::show(&context),
                json::context(&context, true),
            ))
        }
        ContextCmd::Use { name } => {
            let mut store = ctx.store()?;
            store.use_context(name)?;
            let context = store
                .get(name)
                .ok_or_else(|| OpsError::not_found(format!("no context named {name:?}")))?;
            Ok(Outcome::new(
                format!("{name} is now the default ({})", store.path().display()),
                json::context(&context, true),
            ))
        }
        ContextCmd::Add(args) => context_add(ctx, args),
        ContextCmd::Remove { name } => {
            let mut store = ctx.store()?;
            confirm(&ctx.term, &format!("forget context {name}?"))?;
            store.remove(name)?;
            Ok(Outcome::new(
                format!("forgot {name} ({})", store.path().display()),
                serde_json::json!({ "name": name, "removed": true }),
            ))
        }
    }
}

fn context_add(ctx: &Ctx, args: &ContextAddArgs) -> Result<Outcome> {
    let mut store = ctx.store()?;
    let mut context = Context::new(&args.name, &args.airflow_url);
    context.backend_url = if args.direct {
        String::new()
    } else {
        args.backend_url.clone()
    };
    context.repo.clone_from(&args.repo);
    context.owner.clone_from(&args.owner);
    if let Some(root) = &args.metrics_root {
        context.metrics_root.clone_from(root);
    }
    context.dag_ids.clone_from(&args.dags);
    if let Some(tag) = &args.dag_tag {
        context.dag_tag.clone_from(tag);
    }
    context.auth = auth_from(args)?;
    store.add(context.clone(), args.force)?;
    if args.use_it {
        store.use_context(&args.name)?;
    }
    let mut text = format!("added {} ({})", args.name, store.path().display());
    if args.use_it {
        text.push_str(&format!("\n{} is now the default", args.name));
    }
    Ok(Outcome::new(text, json::context(&context, args.use_it)))
}

/// Turn the credential flags into an [`Auth`], refusing the combinations that cannot work.
///
/// Every arm names an environment variable and stops there: the type cannot hold a secret, so a
/// future edit cannot accidentally write one into the config file (non-negotiable 6).
fn auth_from(args: &ContextAddArgs) -> Result<Auth> {
    match (&args.token_env, &args.user, &args.password_env) {
        (Some(var), None, None) => Ok(Auth::TokenEnv { var: var.clone() }),
        (Some(_), _, _) => Err(OpsError::usage(
            "--token-env cannot be combined with --user/--password-env",
        )),
        (None, Some(user), Some(password_env)) => Ok(Auth::Basic {
            user: user.clone(),
            password_env: password_env.clone(),
        }),
        (None, Some(_), None) => Err(OpsError::usage(
            "--user needs --password-env: swf stores the NAME of the variable, never the password",
        )
        .with_hint("swf context add … --user admin --password-env AIRFLOW_PASSWORD")),
        (None, None, Some(_)) => Err(OpsError::usage("--password-env needs --user")),
        (None, None, None) => Ok(Auth::None),
    }
}

// ---------------------------------------------------------------------------- doctor

async fn doctor_cmd(ctx: &Ctx) -> Result<Outcome> {
    // A doctor that refuses to report because it could not connect is a doctor that cannot report
    // the one thing it was run to report, so a failed connection becomes a red row, not an `Err`.
    let context = ctx.context()?;
    let checks = match ctx.ops_for(context.clone()) {
        Ok(ops) => ops.doctor(&ctx.cancel).await,
        Err(err) => vec![doctor::Check::fail(
            "airflow api",
            err.message.clone(),
            err.hint.clone().unwrap_or_else(|| {
                "swf context add <name> --airflow-url http://localhost:8080".to_string()
            }),
        )],
    };
    let code = doctor::exit_code(&checks);
    Ok(Outcome::new(doctor::table(&checks), doctor::to_json_value(&checks)).with_code(code))
}

// ---------------------------------------------------------------------------- submit

async fn submit_cmd(ctx: &Ctx, args: &SubmitArgs) -> Result<Outcome> {
    let ops = ctx.ops()?;
    let request = SubmitRequest {
        issues: args.issues.clone(),
        blueprint: args.blueprint.clone(),
        targets: args.targets.clone(),
    };
    let submission = ops.submit(&request, &ctx.cancel).await?;
    let run = submission.run();
    let mut text = render::submission(&submission);
    let mut doc = serde_json::to_value(&submission).unwrap_or(serde_json::Value::Null);

    if args.wait {
        let final_run = wait_for_run(ctx, &ops, &run).await?;
        text.push_str(&format!("\nstate         {}", final_run.state));
        if let Some(map) = doc.as_object_mut() {
            map.insert("state".into(), final_run.state.clone().into());
        }
        if final_run.state != "success" {
            return Ok(Outcome::new(text, doc).with_code(1));
        }
    }
    Ok(Outcome::new(text, doc))
}

/// Poll one run until Airflow stops calling it active.
///
/// The loop is the client's, not the server's: Airflow has no "tell me when this finishes" call,
/// and inventing a deadline here would turn a slow factory into a reported failure.
async fn wait_for_run(ctx: &Ctx, ops: &Ops, run: &RunRef) -> Result<Run> {
    loop {
        let found = find_run(ops, run, &ctx.cancel).await?;
        if !found.active() {
            return Ok(found);
        }
        ctx.note(&format!("{run} is {}", found.state));
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => return Err(OpsError::cancelled()),
            () = tokio::time::sleep(std::time::Duration::from_secs(WAIT_POLL_S)) => {}
        }
    }
}

// ---------------------------------------------------------------------------- attention

async fn attention_cmd(ctx: &Ctx) -> Result<Outcome> {
    let ops = ctx.ops()?;
    let attention = ops.attention(&ctx.cancel).await;
    let doc = serde_json::to_value(&attention).unwrap_or(serde_json::Value::Null);
    // A report always exits 0, including when it has bad news: `swf attention` under `set -e` is
    // how an operator opens their morning, and a shell that dies because a gate is waiting is
    // useless. Non-negotiable 4 is met by *saying* which source could not be read — a quiet screen
    // never silently means "we could not tell" — not by refusing to print the rest.
    Ok(Outcome::new(render::attention(&attention, &ctx.term), doc))
}

// ---------------------------------------------------------------------------- runs

async fn runs_cmd(ctx: &Ctx, cmd: &RunsCmd) -> Result<Outcome> {
    let ops = ctx.ops()?;
    match cmd {
        RunsCmd::List { dag, state, limit } => {
            let dags = match dag {
                Some(dag) => vec![dag.clone()],
                None => dag_ids(ctx, &ops).await?,
            };
            let mut rows: Vec<Run> = Vec::new();
            let mut truncated = false;
            for dag_id in &dags {
                let listing = ops
                    .runs_matching(dag_id, *limit, state.as_deref(), &ctx.cancel)
                    .await?;
                truncated = truncated || listing.truncated;
                rows.extend(listing.runs);
            }
            if truncated {
                // On stderr so the document stays a bare array (§C.4): a listing that was
                // shortened must say so without changing shape under a `jq` that already works.
                ctx.warn(&format!(
                    "the run list stopped at its bound (--limit {limit} per DAG); \
                     some runs are not shown"
                ));
            }
            let docs: Vec<_> = rows.iter().map(json::run_row).collect();
            Ok(Outcome::new(
                render::runs(&rows, Utc::now(), &ctx.term),
                serde_json::Value::Array(docs),
            ))
        }
        RunsCmd::Inspect { run } => {
            let id = RunRef::parse(run)?;
            let mut found = find_run(&ops, &id, &ctx.cancel).await?;
            found.jobs = job_rows(&ops, &id, &found.issues(), &ctx.cancel).await?;
            let url = ops.run_url(&id)?;
            Ok(Outcome::new(
                render::run_detail(&found, &url, Utc::now(), &ctx.term),
                json::run_detail(&found, &url),
            ))
        }
        RunsCmd::Stop { run } => {
            let id = RunRef::parse(run)?;
            confirm(
                &ctx.term,
                &format!(
                    "mark the Airflow run {id} failed? nothing is stopped and no sandbox is removed"
                ),
            )?;
            ops.stop_run(&id, &ctx.cancel).await?;
            let text = format!(
                "marked the Airflow run {id} failed\nnote  nothing was stopped: tasks already \
                 running keep running, the agent keeps working, and no sandbox was removed\nnote  \
                 remove a sandbox with: swf sandboxes rm <name>"
            );
            Ok(Outcome::new(text, json::stopped(&id.dag_id, &id.run_id)))
        }
        RunsCmd::Unpause { dag } => {
            ops.runs()?.unpause_dag(dag, &ctx.cancel).await?;
            Ok(Outcome::new(
                format!("unpaused {dag}"),
                serde_json::json!({ "dag_id": dag, "paused": false }),
            ))
        }
    }
}

/// The DAGs this context reads: the ones it names, or whatever carries its tag.
async fn dag_ids(ctx: &Ctx, ops: &Ops) -> Result<Vec<String>> {
    let context = ops.context();
    if !context.dag_ids.is_empty() {
        return Ok(context.dag_ids.clone());
    }
    let page = ops.runs()?.list_dags(&context.dag_tag, &ctx.cancel).await?;
    if page.truncated {
        ctx.warn("the DAG list was truncated; some runs are not shown");
    }
    Ok(page.rows)
}

/// One run by identity, out of a bounded window of the newest runs.
async fn find_run(ops: &Ops, id: &RunRef, cancel: &CancellationToken) -> Result<Run> {
    let rows = ops.runs_list(&id.dag_id, RUN_LOOKUP_LIMIT, cancel).await?;
    rows.into_iter()
        .find(|run| run.run_id == id.run_id)
        .ok_or_else(|| {
            OpsError::not_found(format!("no run {id} in the newest {RUN_LOOKUP_LIMIT}"))
                .with_hint(format!("swf runs list --dag {}", id.dag_id))
        })
}

/// The job rows of one run, with the run's own issues as the fallback the XCom may not supply.
async fn job_rows(
    ops: &Ops,
    id: &RunRef,
    fallback: &[String],
    cancel: &CancellationToken,
) -> Result<Vec<JobRow>> {
    let page = ops.runs()?.job_rows(id, fallback, cancel).await?;
    Ok(page.rows)
}

// ---------------------------------------------------------------------------- jobs

async fn jobs_cmd(ctx: &Ctx, cmd: &JobsCmd) -> Result<Outcome> {
    let ops = ctx.ops()?;
    match cmd {
        JobsCmd::List(args) => {
            let listing = ops.jobs_matching(&job_filter(args), &ctx.cancel).await;
            for error in &listing.errors {
                ctx.warn(&format!(
                    "source {} is unavailable: {}",
                    error.source, error.message
                ));
            }
            if listing.truncated {
                ctx.warn("the job list was truncated; some jobs are not shown");
            }
            let rows: Vec<JobRow> = listing.jobs;
            let docs: Vec<_> = rows.iter().map(json::job_row).collect();
            Ok(Outcome::new(
                render::jobs(&rows, &ctx.term),
                serde_json::Value::Array(docs),
            ))
        }
        JobsCmd::Inspect { job } => {
            let id = JobId::parse(job)?;
            let run = id.run();
            let found = find_run(&ops, &run, &ctx.cancel).await?;
            let rows = job_rows(&ops, &run, &found.issues(), &ctx.cancel).await?;
            let row = rows
                .into_iter()
                .find(|row| row.map_index == id.map_index)
                .ok_or_else(|| {
                    OpsError::not_found(format!("no job {id}"))
                        .with_hint(format!("swf runs inspect {run}"))
                })?;
            let gates: Vec<Gate> = ops
                .gates(&ctx.cancel)
                .await
                .map(|list| list.gates)
                .unwrap_or_default()
                .into_iter()
                .filter(|gate| gate.job() == id)
                .collect();
            let url = ops.run_url(&run)?;
            Ok(Outcome::new(
                render::job_detail(&row, &gates, &url, &ctx.term),
                json::job_detail(&row, &gates, &url),
            ))
        }
    }
}

/// The command line's job filters, as the operations layer's own type.
fn job_filter(args: &JobListArgs) -> JobFilter {
    JobFilter {
        dag: args.dag.clone(),
        state: args.state.clone(),
        issue: args.issue.clone(),
        attention: args.attention,
        limit: args.limit,
    }
}

// ---------------------------------------------------------------------------- logs

async fn logs_cmd(ctx: &Ctx, args: &LogsArgs) -> Result<Outcome> {
    let ops = ctx.ops()?;
    let id = JobId::parse(&args.job)?;
    let mut opts = LogOpts {
        attempt: args.attempt,
        ..LogOpts::default()
    };
    if let Some(task) = &args.task {
        opts.task = Some(task_argument(&id, task));
    }

    if args.follow {
        // Airflow has no streaming log endpoint, so `--follow` is a poll loop whose only state is
        // the continuation token. Lines are printed as they arrive unless one document was asked
        // for, in which case they are held until the attempt is complete.
        let mut lines: Vec<String> = Vec::new();
        let mut sink = |line: &str| {
            if !ctx.term.json {
                println!("{line}");
            }
            lines.push(line.to_string());
        };
        let task = swf_app::logs::resolve_task(ops.runs()?, &id, &ctx.cancel).await?;
        let named = LogOpts {
            task: Some(task.clone()),
            ..opts.clone()
        };
        ops.follow_logs(&id, &named, &mut sink, &ctx.cancel).await?;
        let stream = swf_app::logs::LogStream {
            task,
            attempt: args.attempt,
            lines: lines.clone(),
            continuation_token: None,
        };
        // The lines are already on stdout; repeating them in `text` would print each twice.
        return Ok(Outcome::new(
            String::new(),
            json::logs(&id.to_string(), &stream),
        ));
    }

    let stream = ops.logs(&id, &opts, &ctx.cancel).await?;
    if !stream.complete() {
        ctx.warn(&format!(
            "the log for {} is not complete; re-run with --follow",
            stream.task
        ));
    }
    Ok(Outcome::new(
        stream.lines.join("\n"),
        json::logs(&id.to_string(), &stream),
    ))
}

/// Read the task name an operator typed the way they meant it.
///
/// Every stage of a mapped job lives in Airflow's `job` task group, so its real id is `job.setup`
/// while the thing on the operator's screen — and in `swf jobs inspect` — is `setup`. `fan_out` is
/// the one task outside the group, and it is also the only one that is unmapped, so the job's own
/// `map_index` settles which of the two spellings was meant without asking the server.
///
/// This is argument interpretation, the same as `GateId::parse` accepting a bare gate name; it
/// belongs beside `swf_app::logs::resolve_task` the moment the TUI needs it too.
fn task_argument(job: &JobId, task: &str) -> String {
    let name = task.trim();
    if name.is_empty() || name.contains('.') || !job.mapped() {
        return name.to_string();
    }
    format!("job.{name}")
}

// ---------------------------------------------------------------------------- gates

async fn gates_cmd(ctx: &Ctx, cmd: &GatesCmd) -> Result<Outcome> {
    let ops = ctx.ops()?;
    match cmd {
        GatesCmd::List(args) => {
            let listing = ops
                .gates_matching(&gate_filter(&args.filter), &ctx.cancel)
                .await?;
            if listing.truncated {
                // Said on stderr so the document stays an array: hiding gates is the failure mode
                // this product replaces, and a listing that lies by omission is worse than none.
                ctx.warn("the gate list was truncated; some gates are not shown");
            }
            let docs: Vec<_> = listing.gates.iter().map(json::gate_row).collect();
            Ok(Outcome::new(
                render::gates(&listing.gates, Utc::now(), &ctx.term),
                serde_json::Value::Array(docs),
            ))
        }
        GatesCmd::Review { gate } => {
            let id = GateId::parse(gate)?;
            let review = ops.gate_review(&id, &ctx.cancel).await?;
            Ok(Outcome::new(
                render::gate_review(&review, &ctx.term),
                json::gate_review(&review),
            ))
        }
        GatesCmd::Approve(args) => answer(ctx, &ops, args, Decision::Approve).await,
        GatesCmd::Reject(args) => answer(ctx, &ops, args, Decision::Reject).await,
    }
}

/// The command line's gate filters, as the operations layer's own type.
fn gate_filter(args: &GateFilterArgs) -> GateFilter {
    GateFilter {
        dag: args.dag.clone(),
        blueprint: args.blueprint.clone(),
        issue: args.issue.clone(),
        gate: args.gate_name.clone(),
        ready: args.ready,
        limit: args.limit,
    }
}

/// One gate by identity, or a whole filtered set with `--all`.
async fn answer(ctx: &Ctx, ops: &Ops, args: &AnswerArgs, decision: Decision) -> Result<Outcome> {
    let filter = gate_filter(&args.filter);
    if args.all {
        return answer_all(ctx, ops, args, decision, &filter).await;
    }

    // Every refusal below is written out rather than left to clap, because each one is a rule with
    // a reason an operator deserves to read: a flag that was silently ignored here would be a flag
    // whose absence changed which gates got answered.
    if args.dry_run {
        return Err(OpsError::usage(
            "--dry-run describes a set; pass --all with the filters you mean",
        )
        .with_hint("swf gates approve --all --dag <id> --dry-run"));
    }
    if !filter.is_empty() {
        return Err(OpsError::usage(
            "--dag/--blueprint/--issue/--gate/--ready/--limit narrow --all; \
             one gate is already named by its identity",
        )
        .with_hint("swf gates approve --all --dag <id> --dry-run"));
    }
    let Some(gate) = args.gate.as_deref() else {
        return Err(OpsError::usage(
            "name a gate to answer, or pass --all to answer a filtered set",
        )
        .with_hint("swf gates list"));
    };

    let id = GateId::parse(gate)?;
    let who = actor(ops.context());
    let verb = render::verb(decision);
    confirm(
        &ctx.term,
        &format!(
            "{verb} {} of {}[{}] as {who}?",
            id.short_name(),
            id.job.run(),
            id.job.map_index
        ),
    )?;
    let opts = AnswerOpts {
        expect: args.expect.clone(),
        force: args.force,
        confirm_delay: None,
    };
    let answered = ops.gate_answer(&id, decision, &opts, &ctx.cancel).await?;
    Ok(Outcome::new(
        render::gate_answer(&answered, &who, &ctx.term),
        json::gate_answer(&answered, &who),
    ))
}

/// Answer every gate a filter selects: select, show, confirm once, then write.
///
/// The order is the whole safety story. Selecting is a read, so it happens before the question and
/// the question can therefore name the count and the filter that produced it; the writes happen
/// only after an answer, and `--dry-run` reaches the report without this function ever calling the
/// one method that writes.
async fn answer_all(
    ctx: &Ctx,
    ops: &Ops,
    args: &AnswerArgs,
    decision: Decision,
    filter: &GateFilter,
) -> Result<Outcome> {
    if args.gate.is_some() {
        return Err(OpsError::usage("name one gate or pass --all, not both")
            .with_hint("swf gates approve --all --dag <id> --dry-run"));
    }
    if args.force {
        // The rule this refusal defends is in `swf-app`: forcing is a decision about one gate an
        // operator has read, and it does not generalise to a set nobody has read. A batch that
        // could force would answer gates inside the window that makes the scheduler fail them.
        return Err(OpsError::usage(
            "--force cannot be combined with --all: forcing is a decision about one gate you \
             have read, and a batch answers only gates that are ready",
        )
        .with_hint("swf gates approve <gate> --force"));
    }
    if args.expect.is_some() {
        return Err(OpsError::usage(
            "--expect names one piece of evidence, so it cannot be combined with --all",
        )
        .with_hint("swf gates review <gate>"));
    }

    let who = actor(ops.context());
    let verb = render::verb(decision);
    let selection = ops.gates_selection(filter, &ctx.cancel).await?;
    if selection.truncated {
        // A batch over a shortened selection has answered *a* set, not *the* set, and the rest is
        // invisible rather than merely unanswered. It is a warning and not a refusal because the
        // operator may well mean exactly the set they bounded with --limit.
        ctx.warn(
            "the gate selection was truncated; some matching gates are not in it — \
             narrow it with a filter before answering",
        );
    }

    let report = if args.dry_run {
        BatchReport::dry(&selection, decision, filter)
    } else {
        let ready = selection.ready_count();
        if ready > 0 {
            // Nothing has been written yet; a refusal here reaches no service at all.
            confirm(&ctx.term, &batch_question(verb, &who, &selection, filter))?;
        }
        ops.gate_answer_all(&selection, decision, filter, &ctx.cancel)
            .await?
    };

    let code = report.exit_code();
    Ok(Outcome::new(
        render::batch(&report, &who, &ctx.term),
        batch_doc(&report, &who),
    )
    .with_code(code))
}

/// The one question a batch asks: how many, and what selected them.
///
/// Both halves are load-bearing. A count with no filter cannot be checked by the person answering
/// it, and a filter with no count hides the blast radius.
fn batch_question(verb: &str, who: &str, selection: &Selection, filter: &GateFilter) -> String {
    let ready = selection.ready_count();
    let arming = selection.len() - ready;
    let tail = if arming > 0 {
        format!(
            " ({} matched, {arming} still arming and will be skipped)",
            selection.len()
        )
    } else {
        String::new()
    };
    format!(
        "{verb} {} as {who}{tail}? filter: {}",
        render::count_of(ready, "gate"),
        filter.describe()
    )
}

/// What a script reads back from a batch: one result per gate, and one summary.
///
/// It is built here rather than in `json.rs` because none of it is a domain type: every field is
/// an outcome this command produced, and the array alone could not carry the summary a batch has
/// to report — how many matched, how many were skipped, and whether the set was shortened.
fn batch_doc(report: &BatchReport, actor: &str) -> serde_json::Value {
    let results: Vec<serde_json::Value> = report
        .items
        .iter()
        .map(|item| {
            serde_json::json!({
                "id": item.id.to_string(),
                "job": item.id.job.to_string(),
                "gate": item.gate,
                "issue": item.issue,
                "outcome": item.outcome.as_str(),
                "answered": item.outcome == BatchOutcome::Answered,
                "ready": item.ready,
                "revision": item.revision,
                "sightings": item.sightings,
                "detail": item.detail,
            })
        })
        .collect();
    serde_json::json!({
        "results": results,
        "summary": {
            "decision": render::verb(report.decision),
            "chosen_option": report.decision.word(),
            "actor": actor,
            "filter": report.filter,
            "dry_run": report.dry_run,
            "matched": report.matched(),
            "planned": report.count(BatchOutcome::Planned),
            "answered": report.count(BatchOutcome::Answered),
            "skipped": report.count(BatchOutcome::Skipped),
            "conflict": report.count(BatchOutcome::Conflict),
            "failed": report.count(BatchOutcome::Failed),
            "truncated": report.truncated,
            "exit_code": report.exit_code(),
        }
    })
}

// ---------------------------------------------------------------------------- deliveries

async fn deliveries_cmd(ctx: &Ctx, cmd: &DeliveriesCmd) -> Result<Outcome> {
    let ops = ctx.ops()?;
    match cmd {
        DeliveriesCmd::List => {
            let rows = ops.deliveries_list(&ctx.cancel).await?;
            let docs = serde_json::to_value(&rows).unwrap_or(serde_json::Value::Null);
            Ok(Outcome::new(render::deliveries(&rows, &ctx.term), docs))
        }
        DeliveriesCmd::Verify(args) => verify_cmd(ctx, &ops, args).await,
    }
}

async fn verify_cmd(ctx: &Ctx, ops: &Ops, args: &VerifyArgs) -> Result<Outcome> {
    let opts = VerifyOpts {
        clone: args.clone_it,
        repo: args.repo.clone(),
        base_branch: args.base_branch.clone(),
        target_dir: args.target_dir.clone(),
        keep_checkout: args.keep,
        origin: args.origin.clone(),
        branch: args.branch.clone(),
        ..VerifyOpts::default()
    };

    let ids: Vec<DeliveryId> =
        match (&args.delivery, args.all) {
            (Some(one), _) => vec![DeliveryId::parse(one)?],
            (None, true) => ops
                .deliveries_list(&ctx.cancel)
                .await?
                .iter()
                .map(|delivery| {
                    DeliveryId::parse(&delivery.branch)
                        .unwrap_or(DeliveryId::Pr(delivery.number.max(0) as u64))
                })
                .collect(),
            (None, false) => return Err(OpsError::usage(
                "name a delivery to verify, or pass --all to verify every one this context lists",
            )
            .with_hint("swf deliveries list")),
        };

    let mut reports: Vec<DeliveryReport> = Vec::new();
    for id in &ids {
        ctx.note(&format!("verifying {id}"));
        reports.push(ops.verify_delivery(id, &opts, &ctx.cancel).await?);
    }
    if !args.clone_it {
        ctx.warn("--clone was not given, so nothing above `published` was attempted");
    }

    // "Not proven" is not "proven false", but both are non-zero: a script that treats an
    // inconclusive verification as a pass will ship work nobody checked (non-negotiable 10).
    let code = i32::from(!reports.iter().all(|r| r.verdict.is_verified()));
    let docs: Vec<_> = reports.iter().map(json::verification).collect();
    let doc = if args.all {
        serde_json::Value::Array(docs)
    } else {
        docs.into_iter().next().unwrap_or(serde_json::Value::Null)
    };
    Ok(Outcome::new(render::verifications(&reports, &ctx.term), doc).with_code(code))
}

// ---------------------------------------------------------------------------- sandboxes

async fn sandboxes_cmd(ctx: &Ctx, cmd: &SandboxesCmd) -> Result<Outcome> {
    let ops = ctx.ops()?;
    match cmd {
        SandboxesCmd::List => {
            let rows = ops.sandboxes_list(&ctx.cancel).await?;
            let docs = serde_json::to_value(&rows).unwrap_or(serde_json::Value::Null);
            Ok(Outcome::new(
                render::sandboxes(&rows, Utc::now(), &ctx.term),
                docs,
            ))
        }
        SandboxesCmd::Inspect { name } => {
            let rows = ops.sandboxes_list(&ctx.cancel).await?;
            let found = rows
                .into_iter()
                .find(|row| row.name == *name)
                .ok_or_else(|| {
                    OpsError::not_found(format!("no sandbox named {name}"))
                        .with_hint("swf sandboxes list")
                })?;
            let doc = serde_json::to_value(&found).unwrap_or(serde_json::Value::Null);
            Ok(Outcome::new(
                render::sandboxes(std::slice::from_ref(&found), Utc::now(), &ctx.term),
                doc,
            ))
        }
        SandboxesCmd::Rm { name } => {
            confirm(&ctx.term, &format!("remove sandbox {name}?"))?;
            let argv = ops.remove_sandbox(name, &ctx.cancel).await?;
            Ok(Outcome::new(
                format!(
                    "removed {name}\n{}",
                    ctx.term.dim(&format!("$ {}", argv.join(" ")))
                ),
                serde_json::json!({ "name": name, "removed": true, "command": argv }),
            ))
        }
    }
}

// ---------------------------------------------------------------------------- metrics

async fn metrics_cmd(ctx: &Ctx, args: &MetricsArgs) -> Result<Outcome> {
    let mut context = ctx.context()?;
    if let Some(root) = &args.root {
        context.metrics_root.clone_from(root);
    }
    let ops = ctx.ops_for(context)?;
    let summary = ops.metrics_summary(&ctx.cancel).await?;
    let runs = ops.metrics_runs(&ctx.cancel).await?;
    let doc = serde_json::json!({
        "summary": serde_json::to_value(&summary).unwrap_or(serde_json::Value::Null),
        "runs": serde_json::to_value(&runs).unwrap_or(serde_json::Value::Null),
    });
    Ok(Outcome::new(swf_domain::metrics::table(&summary), doc))
}

// ---------------------------------------------------------------------------- snapshot

async fn snapshot_cmd(ctx: &Ctx) -> Result<Outcome> {
    let ops = ctx.ops()?;
    let snap = ops.snapshot_default(&ctx.cancel).await;
    for error in &snap.errors {
        ctx.warn(&format!(
            "source {} is unavailable: {}",
            error.source, error.message
        ));
    }
    let now = Utc::now();
    Ok(Outcome::raw(
        swf_domain::snapshot::snapshot_text(&snap),
        swf_domain::snapshot::snapshot_json_string(&snap, now),
    ))
}

// ---------------------------------------------------------------------------- stack

async fn stack_cmd(ctx: &Ctx, cmd: &StackCmd) -> Result<Outcome> {
    let ops = ctx.ops()?;
    let action = match cmd {
        StackCmd::Up { build } => StackAction::Up { build: *build },
        StackCmd::Down { volumes } => {
            let question = if *volumes {
                "take the local stack down and drop its volumes (metadata DB included)?"
            } else {
                "take the local stack down?"
            };
            confirm(&ctx.term, question)?;
            StackAction::Down { volumes: *volumes }
        }
        StackCmd::Status => StackAction::Status,
    };
    let status = ops.stack(action, &ctx.cancel).await?;
    let doc = serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
    let code = i32::from(!status.ok);
    Ok(Outcome::new(render::stack(&status, &ctx.term), doc).with_code(code))
}

// ---------------------------------------------------------------------------- tui

async fn tui_cmd(ctx: &Ctx) -> Result<Outcome> {
    if ctx.cli.json {
        return Err(OpsError::usage(
            "swf tui is an interface, not a document; drop --json",
        ));
    }
    if !ctx.term.stdout_tty {
        return Err(OpsError::usage("swf tui needs a terminal on stdout"));
    }
    let ops = ctx.ops()?;
    swf_tui::run(Arc::new(ops))
        .await
        .map_err(|err| OpsError::operational(err.to_string()))?;
    Ok(Outcome::text(String::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn ctx(argv: &[&str], term: Term) -> Ctx {
        Ctx {
            cli: Cli::try_parse_from(argv).expect("parse"),
            term,
            cancel: CancellationToken::new(),
        }
    }

    #[test]
    fn a_password_flag_without_a_user_is_refused_before_anything_is_written() {
        let args = ContextAddArgs {
            name: "x".into(),
            airflow_url: "http://x".into(),
            backend_url: "http://localhost:8082".into(),
            direct: false,
            repo: None,
            owner: None,
            metrics_root: None,
            dags: vec![],
            dag_tag: None,
            user: Some("admin".into()),
            password_env: None,
            token_env: None,
            use_it: false,
            force: false,
        };
        let err = auth_from(&args).expect_err("must refuse");
        assert_eq!(err.exit_code(), 2);
        assert!(
            err.message.contains("NAME of the variable"),
            "{}",
            err.message
        );
    }

    #[test]
    fn the_credential_that_is_stored_is_only_ever_a_variable_name() {
        let args = ContextAddArgs {
            name: "x".into(),
            airflow_url: "http://x".into(),
            backend_url: "http://localhost:8082".into(),
            direct: false,
            repo: None,
            owner: None,
            metrics_root: None,
            dags: vec![],
            dag_tag: None,
            user: Some("admin".into()),
            password_env: Some("AIRFLOW_PASSWORD".into()),
            token_env: None,
            use_it: false,
            force: false,
        };
        match auth_from(&args).expect("basic auth") {
            Auth::Basic { user, password_env } => {
                assert_eq!(user, "admin");
                assert_eq!(password_env, "AIRFLOW_PASSWORD");
            }
            other => panic!("expected basic auth, got {other:?}"),
        }
    }

    #[test]
    fn token_and_basic_credentials_cannot_be_given_at_once() {
        let args = ContextAddArgs {
            name: "x".into(),
            airflow_url: "http://x".into(),
            backend_url: "http://localhost:8082".into(),
            direct: false,
            repo: None,
            owner: None,
            metrics_root: None,
            dags: vec![],
            dag_tag: None,
            user: Some("admin".into()),
            password_env: Some("P".into()),
            token_env: Some("T".into()),
            use_it: false,
            force: false,
        };
        assert_eq!(auth_from(&args).expect_err("refused").exit_code(), 2);
    }

    #[test]
    fn the_actor_is_named_only_when_this_client_actually_knows_it() {
        let mut context = Context::builtin();
        context.backend_url.clear();
        assert_eq!(actor(&context), "Airflow token owner");
        context.auth = Auth::TokenEnv { var: "T".into() };
        assert_eq!(actor(&context), "Airflow token owner");
        context.auth = Auth::Basic {
            user: "admin".into(),
            password_env: "P".into(),
        };
        assert_eq!(actor(&context), "admin");
    }

    #[test]
    fn a_stage_name_is_read_as_the_task_the_operator_can_see() {
        let mapped = JobId::new("factory", "r1", 0);
        assert_eq!(task_argument(&mapped, "setup"), "job.setup");
        assert_eq!(
            task_argument(&mapped, " build_and_test "),
            "job.build_and_test"
        );
        // A full id is already an answer, and `fan_out` lives outside the mapped group.
        assert_eq!(task_argument(&mapped, "job.setup"), "job.setup");
        let unmapped = JobId::new("factory", "r1", -1);
        assert_eq!(task_argument(&unmapped, "fan_out"), "fan_out");
    }

    /// The arguments of one `swf gates approve` invocation, defaulted to "one named gate".
    fn answer_args(gate: Option<&str>) -> AnswerArgs {
        AnswerArgs {
            gate: gate.map(str::to_string),
            expect: None,
            force: false,
            all: false,
            dry_run: false,
            filter: GateFilterArgs {
                dag: None,
                blueprint: None,
                issue: None,
                gate_name: None,
                ready: false,
                limit: None,
            },
        }
    }

    /// An operations layer with no adapters: every refusal below must be reached before one is
    /// needed, which is exactly the property being asserted.
    fn unconnected() -> Ops {
        Ops::builder(Context::builtin()).build()
    }

    #[tokio::test]
    async fn a_batch_refuses_to_be_forced_and_says_why_forcing_does_not_generalise() {
        let ops = unconnected();
        let args = AnswerArgs {
            all: true,
            force: true,
            ..answer_args(None)
        };
        let err = answer(
            &ctx(
                &["swf", "gates", "approve", "--all", "--force"],
                Term::plain(),
            ),
            &ops,
            &args,
            Decision::Approve,
        )
        .await
        .expect_err("a set nobody has read must not be forced");
        assert_eq!(err.exit_code(), 2);
        assert!(err.message.contains("--force"), "{}", err.message);
        assert!(
            err.message.contains("one gate you have read"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn a_batch_refuses_the_flags_that_only_name_one_gate() {
        let ops = unconnected();
        let term = Term::plain();
        let argv = ["swf", "gates", "approve", "--all"];

        let with_gate = AnswerArgs {
            all: true,
            ..answer_args(Some("factory/r1#0:plan"))
        };
        let err = answer(&ctx(&argv, term), &ops, &with_gate, Decision::Approve)
            .await
            .expect_err("one gate or a set, never both");
        assert_eq!(err.exit_code(), 2);

        let with_expect = AnswerArgs {
            all: true,
            expect: Some("abc123".into()),
            ..answer_args(None)
        };
        let err = answer(&ctx(&argv, term), &ops, &with_expect, Decision::Approve)
            .await
            .expect_err("a revision names one piece of evidence");
        assert_eq!(err.exit_code(), 2);
        assert!(err.message.contains("--expect"), "{}", err.message);
    }

    #[tokio::test]
    async fn a_filter_without_all_is_refused_instead_of_quietly_ignored() {
        // A flag that narrowed nothing would be a flag whose absence changed which gates were
        // answered, and that is the difference between a filter and a footgun.
        let ops = unconnected();
        let term = Term::plain();
        let filtered = AnswerArgs {
            filter: GateFilterArgs {
                dag: Some("factory".into()),
                blueprint: None,
                issue: None,
                gate_name: None,
                ready: false,
                limit: None,
            },
            ..answer_args(Some("factory/r1#0:plan"))
        };
        let err = answer(
            &ctx(&["swf", "gates", "approve", "x"], term),
            &ops,
            &filtered,
            Decision::Approve,
        )
        .await
        .expect_err("filters narrow --all");
        assert_eq!(err.exit_code(), 2);

        let dry = AnswerArgs {
            dry_run: true,
            ..answer_args(Some("factory/r1#0:plan"))
        };
        let err = answer(
            &ctx(&["swf", "gates", "approve", "x"], term),
            &ops,
            &dry,
            Decision::Approve,
        )
        .await
        .expect_err("--dry-run describes a set");
        assert_eq!(err.exit_code(), 2);

        let nothing = answer_args(None);
        let err = answer(
            &ctx(&["swf", "gates", "approve"], term),
            &ops,
            &nothing,
            Decision::Approve,
        )
        .await
        .expect_err("name a gate or a set");
        assert_eq!(err.exit_code(), 2);
        assert!(err.message.contains("--all"), "{}", err.message);
    }

    #[tokio::test]
    async fn the_tui_refuses_to_be_asked_for_a_document() {
        let term = Term {
            json: true,
            ..Term::plain()
        };
        let err = tui_cmd(&ctx(&["swf", "tui", "--json"], term))
            .await
            .expect_err("must refuse");
        assert_eq!(err.exit_code(), 2);
    }

    #[tokio::test]
    async fn the_tui_refuses_a_pipe() {
        let err = tui_cmd(&ctx(&["swf", "tui"], Term::plain()))
            .await
            .expect_err("must refuse");
        assert_eq!(err.exit_code(), 2);
        assert!(err.message.contains("terminal"), "{}", err.message);
    }
}
