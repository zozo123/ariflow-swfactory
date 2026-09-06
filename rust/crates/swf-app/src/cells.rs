//! Operator operations over durable Factory Cells.
//!
//! This is intentionally a read-only control-plane surface.  Airflow remains the scheduler and
//! the Python backend remains the persistence/fencing authority.

use std::time::Duration;

use swf_adapters::factory::FactoryApi;
use swf_domain::cell::{CellEvent, CellRecord};
use tokio_util::sync::CancellationToken;

use crate::context::Context;
use crate::ops::{OpsError, Result};

pub struct CellOps {
    api: FactoryApi,
}

impl CellOps {
    /// Connect to the configured Python backend using the same credential contract as `Ops`.
    pub fn connect(context: &Context, timeout: Duration) -> Result<Self> {
        let backend_url =
            std::env::var("SWF_BACKEND_URL").unwrap_or_else(|_| context.backend_url.clone());
        if backend_url.is_empty() {
            return Err(OpsError::operational(
                "Factory Cells require the Python backend; this context is in direct mode",
            )
            .with_hint("configure --backend-url and SWF_BACKEND_TOKEN"));
        }
        let token = std::env::var("SWF_BACKEND_TOKEN").unwrap_or_default();
        Ok(Self {
            api: FactoryApi::new(&backend_url, token, timeout)?,
        })
    }

    pub async fn list(&self, limit: usize, cancel: &CancellationToken) -> Result<Vec<CellRecord>> {
        if !(1..=1000).contains(&limit) {
            return Err(OpsError::usage("cell list limit must be between 1 and 1000"));
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
