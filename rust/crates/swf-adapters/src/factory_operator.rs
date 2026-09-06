//! Typed read surfaces for queue pressure, repair debt, fleet and compatibility.
//!
//! These methods deliberately extend the existing `FactoryApi`; they do not create a second HTTP
//! client, auth path or timeout policy.

use serde_json::json;
use swf_domain::operator::{
    BackendCapabilities, FleetSummary, OperationDebt, QueueEntry, QueueSnapshot,
};
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::factory::FactoryApi;

impl FactoryApi {
    pub async fn queue(&self, limit: u32, cancel: &CancellationToken) -> Result<QueueSnapshot> {
        self.call("/queue", json!({"limit": limit}), cancel).await
    }

    pub async fn queue_item(
        &self,
        work_id: &str,
        cancel: &CancellationToken,
    ) -> Result<QueueEntry> {
        self.call("/queue/inspect", json!({"work_id": work_id}), cancel)
            .await
    }

    pub async fn operations(
        &self,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<OperationDebt>> {
        self.call("/operations", json!({"limit": limit}), cancel)
            .await
    }

    pub async fn operation_debt(
        &self,
        operation_key: &str,
        cancel: &CancellationToken,
    ) -> Result<OperationDebt> {
        self.call(
            "/operations/inspect",
            json!({"operation_key": operation_key}),
            cancel,
        )
        .await
    }

    pub async fn fleet(&self, cancel: &CancellationToken) -> Result<FleetSummary> {
        self.call("/fleet", json!({}), cancel).await
    }

    pub async fn capabilities(&self, cancel: &CancellationToken) -> Result<BackendCapabilities> {
        self.call("/compatibility", json!({}), cancel).await
    }
}
