//! Transitional thin command surface for the new backend operator views.
//!
//! This module owns argument spelling and rendering only. All context resolution, validation and
//! backend access flows through `swf_app::OperatorOps` and the existing `FactoryApi` transport.
//! It can therefore be folded into the main clap enum later without moving any business logic.

use std::io::Write;
use std::time::Duration;

use clap::{Parser, Subcommand};
use swf_adapters::traits::DEFAULT_HTTP_TIMEOUT;
use swf_app::context::ContextStore;
use swf_app::ops::{OpsError, Result};
use swf_app::OperatorOps;
use swf_domain::operator::{
    BackendCapabilities, FleetSummary, OperationDebt, QueueEntry, QueueSnapshot,
};
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::exit::{self, Outcome};

#[derive(Debug, Parser)]
#[command(name = "swf", disable_help_subcommand = true)]
struct OperatorCli {
    #[arg(long, global = true)]
    context: Option<String>,
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true)]
    timeout: Option<f64>,
    #[arg(long = "no-color", global = true)]
    _no_color: bool,
    #[arg(short = 'v', long, global = true, action = clap::ArgAction::Count)]
    _verbose: u8,
    #[command(subcommand)]
    command: OperatorCommand,
}

#[derive(Debug, Subcommand)]
enum OperatorCommand {
    /// Durable admission queue and pressure.
    #[command(subcommand)]
    Queue(QueueCmd),
    /// In-doubt/exhausted external mutation repair debt.
    #[command(subcommand)]
    Operations(OperationCmd),
    /// Fleet summary across cells, queue and repair debt.
    Fleet,
    /// Backend contract versions, features and mutation readiness.
    Compatibility,
}

#[derive(Debug, Subcommand)]
enum QueueCmd {
    List {
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    Inspect {
        work_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum OperationCmd {
    List {
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    Inspect {
        operation_key: String,
    },
}

pub fn recognizes(argv: &[String]) -> bool {
    argv.iter().skip(1).any(|arg| {
        matches!(
            arg.as_str(),
            "queue" | "operations" | "fleet" | "compatibility"
        )
    })
}

pub fn start(argv: &[String]) -> i32 {
    let cli = match OperatorCli::try_parse_from(argv) {
        Ok(cli) => cli,
        Err(err) => {
            let _ = err.print();
            return if err.use_stderr() { 2 } else { 0 };
        }
    };
    let json = cli.json;
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            let failure = OpsError::operational(format!("cannot start the async runtime: {err}"));
            return exit::report(
                &failure,
                json,
                &mut std::io::stdout(),
                &mut std::io::stderr(),
            );
        }
    };
    let cancel = CancellationToken::new();
    runtime.block_on(async move {
        let result = run(&cli, &cancel).await;
        match result {
            Ok(outcome) => outcome
                .emit(json, &mut std::io::stdout())
                .unwrap_or_else(|err| {
                    let _ = writeln!(std::io::stderr(), "error: cannot write the answer: {err}");
                    1
                }),
            Err(failure) => exit::report(
                &failure,
                json,
                &mut std::io::stdout(),
                &mut std::io::stderr(),
            ),
        }
    })
}

async fn run(cli: &OperatorCli, cancel: &CancellationToken) -> Result<Outcome> {
    let store = ContextStore::open()?;
    let context = store.resolve(cli.context.as_deref())?;
    let timeout = match cli.timeout {
        Some(seconds) if seconds > 0.0 => Duration::from_secs_f64(seconds),
        Some(_) => return Err(OpsError::usage("--timeout must be greater than zero")),
        None => DEFAULT_HTTP_TIMEOUT,
    };
    let ops = OperatorOps::connect(&context, timeout)?;
    match &cli.command {
        OperatorCommand::Queue(QueueCmd::List { limit }) => {
            let row = ops.queue(*limit, cancel).await?;
            Ok(outcome(render_queue(&row), &row))
        }
        OperatorCommand::Queue(QueueCmd::Inspect { work_id }) => {
            let row = ops.queue_item(work_id, cancel).await?;
            Ok(outcome(render_queue_item(&row), &row))
        }
        OperatorCommand::Operations(OperationCmd::List { limit }) => {
            let rows = ops.operations(*limit, cancel).await?;
            Ok(outcome(render_operations(&rows), &rows))
        }
        OperatorCommand::Operations(OperationCmd::Inspect { operation_key }) => {
            let row = ops.operation(operation_key, cancel).await?;
            Ok(outcome(render_operation(&row), &row))
        }
        OperatorCommand::Fleet => {
            let row = ops.fleet(cancel).await?;
            Ok(outcome(render_fleet(&row), &row))
        }
        OperatorCommand::Compatibility => {
            let row = ops.capabilities(cancel).await?;
            Ok(outcome(render_compatibility(&row), &row))
        }
    }
}

fn outcome<T: serde::Serialize>(human: String, value: &T) -> Outcome {
    Outcome::new(
        human,
        serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
    )
}

fn render_queue(row: &QueueSnapshot) -> String {
    let mut out = format!(
        "queue active={} queued={} oldest={:.1}s p95={:.1}s throttles={}\n",
        row.pressure.active,
        row.pressure.queued,
        row.pressure.oldest_wait_s,
        row.pressure.p95_wait_s,
        row.pressure.throttles,
    );
    if row.queued.is_empty() {
        out.push_str("no queued work");
    } else {
        out.push_str("POS PRIORITY   WAIT STATE      WORK / LIMIT\n");
        for item in &row.queued {
            let limit = item
                .limiting
                .as_ref()
                .map(|v| format!("{} {}/{}", v.dimension, v.current, v.limit))
                .unwrap_or_else(|| "-".to_string());
            out.push_str(&format!(
                "{:>3} {:<10} {:>5.0}s {:<10} {} / {}\n",
                item.position.unwrap_or(0),
                sanitize_line(&item.priority),
                item.wait_s,
                sanitize_line(&item.state),
                sanitize_line(&item.work_id),
                sanitize_line(&limit),
            ));
        }
    }
    out.trim_end().to_string()
}

fn render_queue_item(row: &QueueEntry) -> String {
    let limit = row
        .limiting
        .as_ref()
        .map(|v| format!("{} {}/{} {}", v.dimension, v.current, v.limit, v.key))
        .unwrap_or_else(|| "-".to_string());
    format!(
        "work       {}\nstate      {}\npriority   {}\nwait       {:.1}s\nrepo       {}\nactor      {}\nblueprint  {}\nlimiting   {}\ncell       {}\nepoch      {}",
        sanitize_line(&row.work_id),
        sanitize_line(&row.state),
        sanitize_line(&row.priority),
        row.wait_s,
        sanitize_line(&row.repo),
        sanitize_line(&row.actor),
        sanitize_line(&row.blueprint),
        sanitize_line(&limit),
        sanitize_line(row.cell_id.as_deref().unwrap_or("-")),
        row.cell_epoch.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
    )
}

fn render_operations(rows: &[OperationDebt]) -> String {
    if rows.is_empty() {
        return "no unresolved mutation debt".to_string();
    }
    let mut out = String::from("STATE        ATTEMPTS KIND              CELL / OPERATION\n");
    for row in rows {
        out.push_str(&format!(
            "{:<12} {:>8} {:<17} {} / {}\n",
            sanitize_line(&row.state),
            row.attempts,
            sanitize_line(&row.kind),
            sanitize_line(&row.cell_id),
            sanitize_line(&row.operation_key),
        ));
    }
    out.trim_end().to_string()
}

fn render_operation(row: &OperationDebt) -> String {
    format!(
        "operation  {}\ncell       {}\nepoch      {}\nkind       {}\nstate      {}\nattempts   {}\nerror      {}\nupdated    {:.3}",
        sanitize_line(&row.operation_key),
        sanitize_line(&row.cell_id),
        row.epoch,
        sanitize_line(&row.kind),
        sanitize_line(&row.state),
        row.attempts,
        sanitize_line(row.last_error.as_deref().unwrap_or("-")),
        row.updated_at,
    )
}

fn render_fleet(row: &FleetSummary) -> String {
    format!(
        "active={} queued={} failed={} stale={} orphaned={} repair_debt={} cleanup_debt={}\nbottlenecks: {}",
        row.cells_active,
        row.cells_queued,
        row.cells_failed,
        row.cells_stale,
        row.cells_orphaned,
        row.unresolved_operations,
        row.cleanup_debt,
        if row.bottlenecks.is_empty() {
            "none".to_string()
        } else {
            row.bottlenecks
                .iter()
                .map(|v| sanitize_line(v))
                .collect::<Vec<_>>()
                .join(", ")
        }
    )
}

fn render_compatibility(row: &BackendCapabilities) -> String {
    format!(
        "read_ready={} mutation_ready={} generation={}\ncontracts api={} cell={} evidence={} mutation={} generation={}\nfeatures: {}",
        row.read_ready,
        row.mutation_ready,
        sanitize_line(row.serving_generation.as_deref().unwrap_or("-")),
        row.contracts.api,
        row.contracts.cell,
        row.contracts.evidence,
        row.contracts.mutation,
        row.contracts.generation,
        row.features
            .iter()
            .map(|v| sanitize_line(v))
            .collect::<Vec<_>>()
            .join(", ")
    )
}
