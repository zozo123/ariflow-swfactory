//! Operator operations over durable Factory Cells.
//!
//! This is intentionally a read-only control-plane surface.  Airflow remains the scheduler and
//! the Python backend remains the persistence/fencing authority.

use std::sync::Arc;
use std::time::Duration;

use swf_adapters::factory::FactoryApi;
use swf_domain::cell::{CellEvent, CellRecord};
use tokio_util::sync::CancellationToken;

use crate::backend_context::BackendContext;
use crate::context::Context;
use crate::ops::{OpsError, Result};

pub struct CellOps {
    api: Arc<FactoryApi>,
}

impl CellOps {
    /// Connect through the shared backend URL/token/direct-mode policy.
    pub fn connect(context: &Context, timeout: Duration) -> Result<Self> {
        let backend = BackendContext::connect(context, timeout, "Factory Cell operations")?;
        Ok(Self::from_backend(&backend))
    }

    /// Reuse one resolved backend client across CLI/TUI application operations.
    pub fn from_backend(backend: &BackendContext) -> Self {
        Self { api: backend.api() }
    }

    pub async fn list(&self, limit: usize, cancel: &CancellationToken) -> Result<Vec<CellRecord>> {
        if !(1..=1000).contains(&limit) {
            return Err(OpsError::usage(
                "cell list limit must be between 1 and 1000",
            ));
        }
        Ok(self.api.cells(limit as u32, cancel).await?)
    }

    pub async fn inspect(&self, cell_id: &str, cancel: &CancellationToken) -> Result<CellRecord> {
        validate_cell_id(cell_id)?;
        Ok(self.api.cell(cell_id, cancel).await?)
    }

    pub async fn history(
        &self,
        cell_id: &str,
        cancel: &CancellationToken,
    ) -> Result<Vec<CellEvent>> {
        validate_cell_id(cell_id)?;
        Ok(self.api.cell_history(cell_id, cancel).await?)
    }
}

pub fn validate_cell_id(cell_id: &str) -> Result<()> {
    let valid = cell_id.len() == 29
        && cell_id.starts_with("cell_")
        && cell_id[5..].bytes().all(|b| b.is_ascii_hexdigit());
    if valid {
        Ok(())
    } else {
        Err(OpsError::usage(
            "Factory Cell id must be cell_ followed by 24 hexadecimal characters",
        ))
    }
}
