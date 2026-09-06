//! Read-only fleet/control-plane operations shared by CLI and TUI.

use std::time::Duration;

use swf_adapters::factory::FactoryApi;
use swf_domain::operator::{BackendCapabilities, FleetSummary, OperationDebt, QueueEntry, QueueSnapshot};
use tokio_util::sync::CancellationToken;

use crate::context::Context;
use crate::ops::{OpsError, Result};

pub struct OperatorOps {
    api: FactoryApi,
}

impl OperatorOps {
    pub fn connect(context: &Context, timeout: Duration) -> Result<Self> {
        let backend_url =
            std::env::var("SWF_BACKEND_URL").unwrap_or_else(|_| context.backend_url.clone());
        if backend_url.is_empty() {
            return Err(OpsError::operational(
                "fleet operations require the Python backend; this context is in direct mode",
            )
            .with_hint("configure --backend-url and SWF_BACKEND_TOKEN"));
        }
        let token = std::env::var("SWF_BACKEND_TOKEN").unwrap_or_default();
        Ok(Self {
            api: FactoryApi::new(&backend_url, token, timeout)?,
        })
    }

    pub async fn queue(&self, limit: usize, cancel: &CancellationToken) -> Result<QueueSnapshot> {
        validate_limit(limit)?;
        Ok(self.api.queue(limit as u32, cancel).await?)
    }

    pub async fn queue_item(&self, work_id: &str, cancel: &CancellationToken) -> Result<QueueEntry> {
        validate_bounded_id(work_id, "work id")?;
        Ok(self.api.queue_item(work_id, cancel).await?)
    }

    pub async fn operations(
        &self,
        limit: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<OperationDebt>> {
        validate_limit(limit)?;
        Ok(self.api.operations(limit as u32, cancel).await?)
    }

    pub async fn operation(
        &self,
        operation_key: &str,
        cancel: &CancellationToken,
    ) -> Result<OperationDebt> {
        validate_bounded_id(operation_key, "operation key")?;
        Ok(self.api.operation_debt(operation_key, cancel).await?)
    }

    pub async fn fleet(&self, cancel: &CancellationToken) -> Result<FleetSummary> {
        Ok(self.api.fleet(cancel).await?)
    }

    pub async fn capabilities(&self, cancel: &CancellationToken) -> Result<BackendCapabilities> {
        Ok(self.api.capabilities(cancel).await?)
    }

    pub async fn require_mutation_feature(
        &self,
        feature: &str,
        cancel: &CancellationToken,
    ) -> Result<BackendCapabilities> {
        let capabilities = self.capabilities(cancel).await?;
        capabilities
            .require_mutation(feature)
            .map_err(OpsError::operational)?;
        Ok(capabilities)
    }
}

fn validate_limit(limit: usize) -> Result<()> {
    if (1..=1000).contains(&limit) {
        Ok(())
    } else {
        Err(OpsError::usage("limit must be between 1 and 1000"))
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
