//! Read-only contracts for the bounded seven-worker fan-out/fan-in surface.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_WORKERS: usize = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRole {
    Authority,
    Airflow,
    Workgraph,
    Recovery,
    Security,
    Evidence,
    Operator,
}

impl WorkerRole {
    pub const ALL: [Self; MAX_WORKERS] = [
        Self::Authority,
        Self::Airflow,
        Self::Workgraph,
        Self::Recovery,
        Self::Security,
        Self::Evidence,
        Self::Operator,
    ];
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerReceipt {
    pub task_id: String,
    pub role: WorkerRole,
    pub state: String,
    pub started_at: f64,
    pub finished_at: f64,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerBatch {
    pub batch_id: String,
    pub cell_id: String,
    pub epoch: u64,
    pub receipts: Vec<WorkerReceipt>,
    #[serde(default)]
    pub cancelled: bool,
}

impl WorkerBatch {
    pub fn active_roles(&self) -> usize {
        WorkerRole::ALL
            .iter()
            .filter(|role| self.receipts.iter().any(|receipt| &receipt.role == *role))
            .count()
    }

    pub fn is_ok(&self) -> bool {
        !self.cancelled
            && self
                .receipts
                .iter()
                .all(|receipt| receipt.state == "success")
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.cell_id.starts_with("cell_") {
            return Err("worker batch must reference a Factory Cell".to_string());
        }
        if self.epoch == 0 {
            return Err("worker batch epoch must be positive".to_string());
        }
        if self.active_roles() > MAX_WORKERS {
            return Err("worker batch exceeds seven role lanes".to_string());
        }
        let mut ids: Vec<&str> = self
            .receipts
            .iter()
            .map(|receipt| receipt.task_id.as_str())
            .collect();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("worker batch contains duplicate task ids".to_string());
        }
        Ok(())
    }
}
