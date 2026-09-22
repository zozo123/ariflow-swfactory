//! Opaque credential-lease identity shared with the Python control plane.
//!
//! This module contains identity only. Raw credentials and redeem I/O belong to the trusted
//! adapter/control plane, never to the domain contract or an Airflow worker.

use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

pub const CREDENTIAL_LEASE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CredentialLeaseContractError {
    #[error("credential lease field {0} must be non-empty")]
    Empty(&'static str),
    #[error("credential lease attempt_number and epoch must be positive")]
    InvalidGeneration,
    #[error("credential lease cell_id is invalid")]
    InvalidCell,
    #[error("credential lease policy_digest is invalid")]
    InvalidPolicy,
    #[error("credential lease serialization failed: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CredentialLeaseBinding {
    pub factory_run_id: String,
    pub dag_run_id: String,
    pub task_instance_id: String,
    pub stage_id: String,
    pub sandbox_id: String,
    pub attempt_number: u64,
    pub cell_id: String,
    pub epoch: u64,
    pub operation_key: String,
    pub policy_digest: String,
}

impl CredentialLeaseBinding {
    pub fn validate(&self) -> Result<(), CredentialLeaseContractError> {
        for (name, value) in [
            ("factory_run_id", self.factory_run_id.as_str()),
            ("dag_run_id", self.dag_run_id.as_str()),
            ("task_instance_id", self.task_instance_id.as_str()),
            ("stage_id", self.stage_id.as_str()),
            ("sandbox_id", self.sandbox_id.as_str()),
            ("operation_key", self.operation_key.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CredentialLeaseContractError::Empty(name));
            }
        }
        if self.attempt_number == 0 || self.epoch == 0 {
            return Err(CredentialLeaseContractError::InvalidGeneration);
        }
        if !self.cell_id.starts_with("cell_") || self.cell_id.len() != 29 {
            return Err(CredentialLeaseContractError::InvalidCell);
        }
        if !self.policy_digest.starts_with("policy:") {
            return Err(CredentialLeaseContractError::InvalidPolicy);
        }
        Ok(())
    }

    /// Python uses json.dumps(sort_keys=True,separators=(",",":")); reproduce that exact byte
    /// contract rather than relying on struct declaration order.
    pub fn canonical_json(&self) -> Result<String, CredentialLeaseContractError> {
        self.validate()?;
        let raw = serde_json::to_value(self)
            .map_err(|error| CredentialLeaseContractError::Serialization(error.to_string()))?;
        let Value::Object(object) = raw else {
            return Err(CredentialLeaseContractError::Serialization(
                "binding was not an object".into(),
            ));
        };
        let mut keys: Vec<_> = object.keys().cloned().collect();
        keys.sort();
        let mut ordered = Map::new();
        for key in keys {
            if let Some(value) = object.get(&key) {
                ordered.insert(key, value.clone());
            }
        }
        serde_json::to_string(&Value::Object(ordered))
            .map_err(|error| CredentialLeaseContractError::Serialization(error.to_string()))
    }

    pub fn digest(&self) -> Result<String, CredentialLeaseContractError> {
        let canonical = self.canonical_json()?;
        let hash = digest(&SHA256, canonical.as_bytes());
        Ok(format!("lease-binding:{}", hex(hash.as_ref())))
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}
