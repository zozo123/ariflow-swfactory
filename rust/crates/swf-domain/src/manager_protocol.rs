//! Versioned protocol between Airflow lifecycle tasks and the Rust factory manager.
//!
//! Airflow carries scheduling identity and invokes one manager use case. It never carries factory
//! business logic or provider credentials. The same payload can travel over HTTP or a Unix socket.

use serde::{Deserialize, Serialize};

use crate::factory::{FactoryName, FactoryRunId};

pub const MANAGER_API_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagerEnvelope<T> {
    pub api_version: u32,
    pub request_id: String,
    pub payload: T,
}

impl<T> ManagerEnvelope<T> {
    pub fn new(request_id: impl Into<String>, payload: T) -> Result<Self, ProtocolError> {
        let request_id = request_id.into();
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Err(ProtocolError::InvalidRequestId);
        }
        Ok(Self {
            api_version: MANAGER_API_VERSION,
            request_id,
            payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageInvocation {
    pub factory: FactoryName,
    pub run_id: FactoryRunId,
    pub cell_id: String,
    pub epoch: u64,
    pub stage: String,
    pub attempt: u32,
    pub airflow: AirflowInvocation,
}

impl StageInvocation {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if !self.cell_id.starts_with("cell_") {
            return Err(ProtocolError::InvalidCellId);
        }
        if self.epoch == 0 {
            return Err(ProtocolError::InvalidEpoch);
        }
        if self.stage.trim().is_empty() {
            return Err(ProtocolError::InvalidStage);
        }
        if self.attempt == 0 {
            return Err(ProtocolError::InvalidAttempt);
        }
        if self.airflow.dag_id.trim().is_empty()
            || self.airflow.dag_run_id.trim().is_empty()
            || self.airflow.task_id.trim().is_empty()
        {
            return Err(ProtocolError::InvalidAirflowIdentity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AirflowInvocation {
    pub dag_id: String,
    pub dag_run_id: String,
    pub task_id: String,
    #[serde(default)]
    pub map_index: Option<i64>,
    pub try_number: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageDisposition {
    Completed,
    WaitingApproval,
    Retryable,
    Failed,
    InDoubt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageReceipt {
    pub run_id: FactoryRunId,
    pub cell_id: String,
    pub epoch: u64,
    pub stage: String,
    pub attempt: u32,
    pub disposition: StageDisposition,
    #[serde(default)]
    pub evidence_digest: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    #[error("manager request id must be nonempty and at most 128 characters")]
    InvalidRequestId,
    #[error("stage invocation requires a Factory Cell id")]
    InvalidCellId,
    #[error("stage invocation epoch must be positive")]
    InvalidEpoch,
    #[error("stage name must be nonempty")]
    InvalidStage,
    #[error("stage attempt must be positive")]
    InvalidAttempt,
    #[error("Airflow dag/run/task identity must be complete")]
    InvalidAirflowIdentity,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::{FactoryName, FactoryRunId};

    fn invocation() -> StageInvocation {
        StageInvocation {
            factory: FactoryName::parse("research").unwrap(),
            run_id: FactoryRunId::parse("frun_0123456789abcdef").unwrap(),
            cell_id: "cell_0123456789abcdef01234567".into(),
            epoch: 3,
            stage: "build_and_test".into(),
            attempt: 2,
            airflow: AirflowInvocation {
                dag_id: "swf__research".into(),
                dag_run_id: "manual__abc".into(),
                task_id: "job.build_and_test".into(),
                map_index: Some(4),
                try_number: 2,
            },
        }
    }

    #[test]
    fn airflow_identity_is_transport_context_not_run_identity() {
        let item = invocation();
        item.validate().unwrap();
        assert_eq!(item.run_id.as_str(), "frun_0123456789abcdef");
        assert_ne!(item.run_id.as_str(), item.airflow.dag_run_id);
    }

    #[test]
    fn invocation_requires_current_cell_epoch_and_attempt() {
        let mut item = invocation();
        item.epoch = 0;
        assert_eq!(item.validate(), Err(ProtocolError::InvalidEpoch));

        let mut item = invocation();
        item.attempt = 0;
        assert_eq!(item.validate(), Err(ProtocolError::InvalidAttempt));
    }

    #[test]
    fn envelope_is_versioned_for_http_or_unix_socket_transport() {
        let envelope = ManagerEnvelope::new("airflow-task-1", invocation()).unwrap();
        assert_eq!(envelope.api_version, MANAGER_API_VERSION);
    }
}
