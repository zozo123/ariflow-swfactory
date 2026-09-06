//! Durable backend control-plane documents rendered by Rust operator surfaces.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueRecord {
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
    pub limiting_dimension: Option<String>,
    #[serde(default)]
    pub limiting_current: Option<i64>,
    #[serde(default)]
    pub limiting_limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub operation_key: String,
    pub cell_id: String,
    pub epoch: i64,
    pub kind: String,
    pub state: String,
    #[serde(default)]
    pub attempts: i64,
    #[serde(default)]
    pub updated_at: f64,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub observation: Option<Value>,
    #[serde(default)]
    pub next_attempt_at: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FleetSummary {
    #[serde(default)]
    pub cells: BTreeMap<String, u64>,
    #[serde(default)]
    pub queue: BTreeMap<String, u64>,
    #[serde(default)]
    pub repair_debt: BTreeMap<String, u64>,
    #[serde(default)]
    pub generations: BTreeMap<String, u64>,
    #[serde(default)]
    pub bottlenecks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractVersion {
    pub major: u32,
    pub minor: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompatibilityDocument {
    pub api: ContractVersion,
    pub cells: ContractVersion,
    pub evidence: ContractVersion,
    pub mutations: ContractVersion,
    pub generation: ContractVersion,
    #[serde(default)]
    pub features: BTreeMap<String, bool>,
    #[serde(default)]
    pub read_ready: bool,
    #[serde(default)]
    pub mutation_ready: bool,
    #[serde(default)]
    pub drain_state: String,
}

impl CompatibilityDocument {
    pub fn require_feature(&self, name: &str) -> Result<(), String> {
        if self.features.get(name).copied().unwrap_or(false) {
            Ok(())
        } else {
            Err(format!("factory backend does not advertise required feature {name:?}"))
        }
    }

    pub fn require_mutation_ready(&self) -> Result<(), String> {
        if self.mutation_ready {
            Ok(())
        } else {
            Err(format!(
                "factory backend is read-only for mutations (drain_state={})",
                self.drain_state
            ))
        }
    }
}
