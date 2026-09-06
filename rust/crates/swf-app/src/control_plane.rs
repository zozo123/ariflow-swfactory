//! Shared operator operations for durable queue, mutation debt and fleet state.
//!
//! This is read-only. Mutations continue to flow through the established Ops/backend paths.

use std::time::Duration;

use serde_json::json;
use swf_adapters::factory::FactoryApi;
use swf_domain::control_plane::{CompatibilityDocument, FleetSummary, OperationRecord, QueueRecord};
use tokio_util::sync::CancellationToken;

use crate::context::Context;
use crate::ops::{OpsError, Result};

pub struct ControlPlaneOps {
    api: FactoryApi,
}

impl ControlPlaneOps {
    pub fn connect(context: &Context, timeout: Duration) -> Result<Self> {
        let backend_url =
            std::env::var("SWF_BACKEND_URL").unwrap_or_else(|_| context.backend_url.clone());
        if backend_url.is_empty() {
            return Err(OpsError::operational(
                "durable control-plane views require the Python backend; this context is direct",
            )
            .with_hint("configure --backend-url and SWF_BACKEND_TOKEN"));
        }
        let token = std::env::var("SWF_BACKEND_TOKEN").unwrap_or_default();
        Ok(Self {
            api: FactoryApi::new(&backend_url, token, timeout)?,
        })
    }

    pub async fn compatibility(
        &self,
        cancel: &CancellationToken,
    ) -> Result<CompatibilityDocument> {
        Ok(self.api.call("/compatibility", json!({}), cancel).await?)
    }

    pub async fn queue(
        &self,
        state: Option<&str>,
        limit: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<QueueRecord>> {
        bounded(limit, "queue")?;
        Ok(self
            .api
            .call(
                "/queue",
                json!({"state": state, "limit": limit}),
                cancel,
            )
            .await?)
    }

    pub async fn queue_item(
        &self,
        work_id: &str,
        cancel: &CancellationToken,
    ) -> Result<QueueRecord> {
        if work_id.trim().is_empty() || work_id.len() > 256 {
            return Err(OpsError::usage("work id must be nonempty and at most 256 characters"));
        }
        Ok(self
            .api
            .call("/queue/inspect", json!({"work_id": work_id}), cancel)
            .await?)
    }

    pub async fn operations(
        &self,
        limit: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<OperationRecord>> {
        bounded(limit, "operation")?;
        Ok(self
            .api
            .call("/operations", json!({"limit": limit}), cancel)
            .await?)
    }

    pub async fn operation(
        &self,
        operation_key: &str,
        cancel: &CancellationToken,
    ) -> Result<OperationRecord> {
        if operation_key.trim().is_empty() || operation_key.len() > 256 {
            return Err(OpsError::usage(
                "operation key must be nonempty and at most 256 characters",
            ));
        }
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
}

fn bounded(limit: usize, noun: &str) -> Result<()> {
    if (1..=1000).contains(&limit) {
        Ok(())
    } else {
        Err(OpsError::usage(format!(
            "{noun} list limit must be between 1 and 1000"
        )))
    }
}
