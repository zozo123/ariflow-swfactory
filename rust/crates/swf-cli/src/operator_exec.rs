//! Thin CLI rendering for the backend's operator views.
//!
//! Queue pressure, repair debt, the fleet summary and the compatibility report are chosen and
//! validated in `swf-app::operator`, where `swf tui` reads exactly the same answers; this module
//! only picks the human and JSON shapes. The context, the credential and the deadline come from
//! [`Ctx::backend`], so these verbs connect the same way, honour the same `--timeout`, narrate the
//! same `-v` line and are cancelled by the same Ctrl-C as every other verb the binary answers.
//!
//! This file used to carry a clap parser and a tokio runtime of its own. Both are gone: a second
//! parser is a second `--help`, a second completion surface and a second usage envelope, which is
//! the drift #197 exists to collapse.

use swf_app::ops::Result;
use swf_domain::operator::{
    BackendCapabilities, FleetSummary, OperationDebt, QueueEntry, QueueSnapshot,
};
use swf_domain::sanitize::sanitize_line;

use crate::cli::{OperationsCmd, QueueCmd};
use crate::exec::Ctx;
use crate::exit::Outcome;

/// `swf queue …`
pub async fn queue(ctx: &Ctx, cmd: &QueueCmd) -> Result<Outcome> {
    let ops = ctx.backend("the admission queue")?.operator()?;
    match cmd {
        QueueCmd::List { limit } => {
            let row = ops.queue(*limit, &ctx.cancel).await?;
            Ok(outcome(render_queue(&row), &row))
        }
        QueueCmd::Inspect { work_id } => {
            let row = ops.queue_item(work_id, &ctx.cancel).await?;
            Ok(outcome(render_queue_item(&row), &row))
        }
    }
}

/// `swf operations …`
pub async fn operations(ctx: &Ctx, cmd: &OperationsCmd) -> Result<Outcome> {
    let ops = ctx.backend("external mutation repair debt")?.operator()?;
    match cmd {
        OperationsCmd::List { limit } => {
            let rows = ops.operations(*limit, &ctx.cancel).await?;
            Ok(outcome(render_operations(&rows), &rows))
        }
        OperationsCmd::Inspect { operation_key } => {
            let row = ops.operation(operation_key, &ctx.cancel).await?;
            Ok(outcome(render_operation(&row), &row))
        }
    }
}

/// `swf fleet`
pub async fn fleet(ctx: &Ctx) -> Result<Outcome> {
    let ops = ctx.backend("the fleet summary")?.operator()?;
    let row = ops.fleet(&ctx.cancel).await?;
    Ok(outcome(render_fleet(&row), &row))
}

/// `swf compatibility`
pub async fn compatibility(ctx: &Ctx) -> Result<Outcome> {
    let ops = ctx
        .backend("the backend compatibility report")?
        .operator()?;
    let row = ops.capabilities(&ctx.cancel).await?;
    Ok(outcome(render_compatibility(&row), &row))
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
