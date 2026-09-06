//! Thin CLI rendering for durable Factory Cells.
//!
//! Validation and transport live in `swf-app::cells`; this module only chooses human/JSON shapes.

use std::time::Duration;

use swf_adapters::traits::DEFAULT_HTTP_TIMEOUT;
use swf_app::cells::CellOps;
use swf_app::context::ContextStore;
use swf_app::ops::{OpsError, Result};
use swf_domain::cell::{CellEvent, CellRecord};
use swf_domain::sanitize::sanitize_line;

use crate::cli::CellsCmd;
use crate::exec::Ctx;
use crate::exit::Outcome;

pub async fn run(ctx: &Ctx, cmd: &CellsCmd) -> Result<Outcome> {
    let store = ContextStore::open()?;
    let context = store.resolve(ctx.cli.context.as_deref())?;
    let timeout = match ctx.cli.timeout {
        Some(seconds) if seconds > 0.0 => Duration::from_secs_f64(seconds),
        Some(_) => return Err(OpsError::usage("--timeout must be greater than zero")),
        None => DEFAULT_HTTP_TIMEOUT,
    };
    let cells = CellOps::connect(&context, timeout)?;

    match cmd {
        CellsCmd::List { limit } => {
            let rows = cells.list(*limit, &ctx.cancel).await?;
            let doc = serde_json::to_value(&rows).unwrap_or(serde_json::Value::Null);
            Ok(Outcome::new(render_list(&rows), doc))
        }
        CellsCmd::Inspect { cell_id } => {
            let row = cells.inspect(cell_id, &ctx.cancel).await?;
            let doc = serde_json::to_value(&row).unwrap_or(serde_json::Value::Null);
            Ok(Outcome::new(render_cell(&row), doc))
        }
        CellsCmd::History { cell_id } => {
            let rows = cells.history(cell_id, &ctx.cancel).await?;
            let doc = serde_json::to_value(&rows).unwrap_or(serde_json::Value::Null);
            Ok(Outcome::new(render_history(cell_id, &rows), doc))
        }
    }
}

fn render_list(rows: &[CellRecord]) -> String {
    if rows.is_empty() {
        return "no Factory Cells".to_string();
    }
    let mut out =
        String::from("CELL                         EPOCH STATE        ISSUE REPOSITORY / TARGET\n");
    for row in rows {
        out.push_str(&format!(
            "{:<28} {:>5} {:<12} {:<5} {} / {}\n",
            sanitize_line(&row.cell_id),
            row.epoch,
            sanitize_line(&row.state),
            sanitize_line(&row.issue),
            sanitize_line(&row.repo),
            sanitize_line(&row.target),
        ));
    }
    out.trim_end().to_string()
}

fn render_cell(row: &CellRecord) -> String {
    let airflow = row
        .airflow_identity()
        .unwrap_or_else(|| "unbound".to_string());
    let generation = row.factory_generation.as_deref().unwrap_or("-");
    let policy = row.policy_digest.as_deref().unwrap_or("-");
    let base = row.base_sha.as_deref().unwrap_or("-");
    let observed = row.observed_target_sha.as_deref().unwrap_or("-");
    let compute = compact(&row.compute);
    let cleanup = compact(&row.cleanup);
    format!(
        "cell           {}\nschema         {}\nepoch          {}\nstate          {}\nissue          {}\nrepository     {}\ntarget         {}\nairflow        {}\ngeneration     {}\npolicy         {}\nbase           {}\nobserved       {}\ncompute        {}\ncleanup        {}\nupdated_at     {:.3}",
        sanitize_line(&row.cell_id),
        row.schema_version,
        row.epoch,
        sanitize_line(&row.state),
        sanitize_line(&row.issue),
        sanitize_line(&row.repo),
        sanitize_line(&row.target),
        sanitize_line(&airflow),
        sanitize_line(generation),
        sanitize_line(policy),
        sanitize_line(base),
        sanitize_line(observed),
        sanitize_line(&compute),
        sanitize_line(&cleanup),
        row.updated_at,
    )
}

fn render_history(cell_id: &str, rows: &[CellEvent]) -> String {
    if rows.is_empty() {
        return format!("{} has no recorded events", sanitize_line(cell_id));
    }
    let mut out = format!(
        "history {}\nSEQ EPOCH KIND                 OPERATION\n",
        sanitize_line(cell_id)
    );
    for row in rows {
        out.push_str(&format!(
            "{:>3} {:>5} {:<20} {}\n",
            row.seq,
            row.epoch,
            sanitize_line(&row.kind),
            sanitize_line(&row.operation_key),
        ));
    }
    out.trim_end().to_string()
}

fn compact(value: &Option<serde_json::Value>) -> String {
    match value {
        Some(value) => serde_json::to_string(value).unwrap_or_else(|_| "<invalid>".to_string()),
        None => "-".to_string(),
    }
}
