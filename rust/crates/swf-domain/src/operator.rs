//! Read-only operator contracts shared by CLI and TUI.
//!
//! The Python backend owns persistence and mutation authority. These types mirror its versioned
//! JSON so Rust can render queue pressure, repair debt, fleet state and compatibility without
//! inventing a second interpretation.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitingDimension {
    pub dimension: String,
    pub current: i64,
    pub limit: i64,
    #[serde(default)]
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueEntry {
    pub work_id: String,
    pub repo: String,
    pub actor: String,
    pub blueprint: String,
    pub priority: String,
    pub state: String,
    pub reason: String,
    #[serde(default)]
    pub position: Option<u64>,
    #[serde(default)]
    pub wait_s: f64,
    #[serde(default)]
    pub limiting: Option<LimitingDimension>,
    #[serde(default)]
    pub cell_id: Option<String>,
    #[serde(default)]
    pub cell_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuePressure {
    pub active: u64,
    pub queued: u64,
    pub oldest_wait_s: f64,
    pub p50_wait_s: f64,
    pub p95_wait_s: f64,
    pub throttles: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueSnapshot {
    pub active: Vec<QueueEntry>,
    pub queued: Vec<QueueEntry>,
    #[serde(default)]
    pub limits: Value,
    pub pressure: QueuePressure,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationDebt {
    pub operation_key: String,
    pub cell_id: String,
    pub epoch: u64,
    pub kind: String,
    pub state: String,
    #[serde(default)]
    pub attempts: u64,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub observation: Option<Value>,
    #[serde(default)]
    pub updated_at: f64,
    #[serde(default)]
    pub next_attempt_at: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FleetSummary {
    pub cells_active: u64,
    pub cells_queued: u64,
    pub cells_failed: u64,
    pub cells_stale: u64,
    pub cells_orphaned: u64,
    pub queue_depth: u64,
    pub unresolved_operations: u64,
    pub cleanup_debt: u64,
    #[serde(default)]
    pub generation_counts: Value,
    #[serde(default)]
    pub bottlenecks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContractVersions {
    pub api: u32,
    pub cell: u32,
    pub evidence: u32,
    pub mutation: u32,
    pub generation: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub schema_version: u32,
    pub contracts: ContractVersions,
    #[serde(default)]
    pub features: Vec<String>,
    pub read_ready: bool,
    pub mutation_ready: bool,
    #[serde(default)]
    pub serving_generation: Option<String>,
    #[serde(default)]
    pub draining_generation: Option<String>,
}

impl BackendCapabilities {
    pub fn supports(&self, feature: &str) -> bool {
        self.features.iter().any(|candidate| candidate == feature)
    }

    pub fn require_mutation(&self, feature: &str) -> Result<(), String> {
        if !self.mutation_ready {
            return Err("backend is not mutation-ready".to_string());
        }
        if !self.supports(feature) {
            return Err(format!(
                "backend does not advertise required feature {feature}"
            ));
        }
        Ok(())
    }
}
