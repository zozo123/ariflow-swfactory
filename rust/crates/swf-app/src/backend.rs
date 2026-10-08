//! Read-only operator views the factory backend serves: durable Factory Cells, queue pressure,
//! repair debt, fleet and compatibility.
//!
//! Each view is one POST through the shared `FactoryApi`, so there is no second HTTP client, auth
//! path or timeout policy. Airflow remains the scheduler and the Python backend remains the
//! persistence/fencing authority.

use std::sync::Arc;

use serde_json::json;
use swf_adapters::factory::FactoryApi;
use swf_domain::cell::{CellEvent, CellRecord};
use swf_domain::operator::{
    BackendCapabilities, FleetSummary, OperationDebt, QueueEntry, QueueSnapshot,
};
use tokio_util::sync::CancellationToken;

use crate::backend_context::BackendContext;
use crate::ops::{OpsError, Result};

pub struct BackendOps {
    api: Arc<FactoryApi>,
}

impl BackendOps {
    /// Reuse one resolved backend client across application operations.
    pub fn from_backend(backend: &BackendContext) -> Self {
        Self { api: backend.api() }
    }

    /// Newest durable Factory Cells, newest mutation first.
    pub async fn cells(&self, limit: usize, cancel: &CancellationToken) -> Result<Vec<CellRecord>> {
        let limit = bounded(limit, "cell list limit")?;
        Ok(self
            .api
            .call("/cells", json!({"limit": limit}), cancel)
            .await?)
    }

    /// One durable Factory Cell projection.
    pub async fn cell(&self, cell_id: &str, cancel: &CancellationToken) -> Result<CellRecord> {
        validate_cell_id(cell_id)?;
        Ok(self
            .api
            .call("/cells/inspect", json!({"cell_id": cell_id}), cancel)
            .await?)
    }

    /// The append-only history of one durable Factory Cell.
    pub async fn cell_history(
        &self,
        cell_id: &str,
        cancel: &CancellationToken,
    ) -> Result<Vec<CellEvent>> {
        validate_cell_id(cell_id)?;
        Ok(self
            .api
            .call("/cells/history", json!({"cell_id": cell_id}), cancel)
            .await?)
    }

    pub async fn queue(&self, limit: usize, cancel: &CancellationToken) -> Result<QueueSnapshot> {
        let limit = bounded(limit, "limit")?;
        Ok(self
            .api
            .call("/queue", json!({"limit": limit}), cancel)
            .await?)
    }

    pub async fn queue_item(
        &self,
        work_id: &str,
        cancel: &CancellationToken,
    ) -> Result<QueueEntry> {
        validate_bounded_id(work_id, "work id")?;
        Ok(self
            .api
            .call("/queue/inspect", json!({"work_id": work_id}), cancel)
            .await?)
    }

    pub async fn operations(
        &self,
        limit: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<OperationDebt>> {
        let limit = bounded(limit, "limit")?;
        Ok(self
            .api
            .call("/operations", json!({"limit": limit}), cancel)
            .await?)
    }

    pub async fn operation(
        &self,
        operation_key: &str,
        cancel: &CancellationToken,
    ) -> Result<OperationDebt> {
        validate_bounded_id(operation_key, "operation key")?;
        Ok(self
            .api
            .call(
                "/operations/inspect",
                json!({"operation_key": operation_key}),
                cancel,
            )
            .await?)
    }

    pub async fn fleet(&self, cancel: &CancellationToken) -> Result<FleetSummary> {
        Ok(self.api.call("/fleet", json!({}), cancel).await?)
    }

    pub async fn capabilities(&self, cancel: &CancellationToken) -> Result<BackendCapabilities> {
        Ok(self.api.call("/compatibility", json!({}), cancel).await?)
    }
}

fn bounded(limit: usize, what: &str) -> Result<usize> {
    if (1..=1000).contains(&limit) {
        Ok(limit)
    } else {
        Err(OpsError::usage(format!(
            "{what} must be between 1 and 1000"
        )))
    }
}

fn validate_cell_id(cell_id: &str) -> Result<()> {
    // Delegates rather than restating the rule: this surface already required the full shape while
    // the domain types accepted a bare prefix, which is how the two drifted apart.
    if swf_domain::cell::is_cell_id(cell_id) {
        Ok(())
    } else {
        Err(OpsError::usage(
            "Factory Cell id must be cell_ followed by 24 hexadecimal characters",
        ))
    }
}

fn validate_bounded_id(value: &str, name: &str) -> Result<()> {
    if !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_whitespace) {
        Ok(())
    } else {
        Err(OpsError::usage(format!(
            "{name} must be nonempty, whitespace-free and at most 512 characters"
        )))
    }
}
