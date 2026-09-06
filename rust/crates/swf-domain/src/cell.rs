//! Durable Factory Cell contracts shared by the operator surfaces.
//!
//! The Python backend is authoritative for persistence/fencing.  These types deliberately mirror
//! its versioned JSON documents so Rust can inspect the same identity without reinterpreting it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One durable issue×target lifecycle projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellRecord {
    pub cell_id: String,
    pub schema_version: u32,
    pub repo: String,
    pub target: String,
    pub issue: String,
    pub epoch: u64,
    pub state: String,
    pub airflow_dag_id: Option<String>,
    pub airflow_run_id: Option<String>,
    pub map_index: Option<i64>,
    pub factory_generation: Option<String>,
    pub policy_digest: Option<String>,
    pub base_sha: Option<String>,
    pub observed_target_sha: Option<String>,
    pub compute: Option<Value>,
    pub cleanup: Option<Value>,
    pub created_at: f64,
    pub updated_at: f64,
}

impl CellRecord {
    /// Whether this projection is in a lifecycle-terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state.as_str(),
            "success" | "failed" | "cancelled" | "rejected" | "cleaned"
        )
    }

    /// Stable `dag/run#index` identity when Airflow has accepted the cell.
    pub fn airflow_identity(&self) -> Option<String> {
        Some(format!(
            "{}/{}#{}",
            self.airflow_dag_id.as_ref()?,
            self.airflow_run_id.as_ref()?,
            self.map_index?
        ))
    }
}

/// One append-only mutation/evidence event from a Factory Cell history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellEvent {
    pub seq: u64,
    pub epoch: u64,
    pub operation_key: String,
    pub kind: String,
    pub payload: Value,
    pub created_at: f64,
}
