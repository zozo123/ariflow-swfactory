//! Language-neutral identity for one Airflow-scheduled Rust task.
//!
//! Airflow owns *when* this task runs. Rust owns *what* the task means.
//! The identifiers here are intentionally small enough to cross either the phase-1 subprocess
//! boundary or the phase-2 Airflow Task SDK coordinator protocol without smuggling domain state
//! through argv/environment variables.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInvocation {
    pub dag_id: String,
    pub run_id: String,
    pub task_id: String,
    pub map_index: i64,
    pub stage: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TaskInvocationError {
    #[error("{0} must be nonempty")]
    Empty(&'static str),
    #[error("stage contains invalid characters")]
    InvalidStage,
    #[error("cell_id and epoch must be supplied together")]
    IncompleteCellIdentity,
    #[error("cell_id has invalid shape")]
    InvalidCellId,
}

impl TaskInvocation {
    pub fn validate(&self) -> Result<(), TaskInvocationError> {
        for (name, value) in [
            ("dag_id", self.dag_id.as_str()),
            ("run_id", self.run_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("stage", self.stage.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(TaskInvocationError::Empty(name));
            }
        }

        if !self
            .stage
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
        {
            return Err(TaskInvocationError::InvalidStage);
        }

        match (&self.cell_id, self.epoch) {
            (None, None) => {}
            (Some(cell), Some(_)) => {
                let suffix = cell
                    .strip_prefix("cell_")
                    .ok_or(TaskInvocationError::InvalidCellId)?;
                if suffix.len() != 24 || !suffix.chars().all(|ch| ch.is_ascii_hexdigit()) {
                    return Err(TaskInvocationError::InvalidCellId);
                }
            }
            _ => return Err(TaskInvocationError::IncompleteCellIdentity),
        }

        Ok(())
    }

    /// Stable identity for evidence, replay and task-runtime dedupe.
    pub fn identity(&self) -> String {
        format!(
            "{}/{}/{}#{}:{}",
            self.dag_id, self.run_id, self.task_id, self.map_index, self.stage
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invocation() -> TaskInvocation {
        TaskInvocation {
            dag_id: "factory".into(),
            run_id: "manual__2026-09-18T10:00:00+00:00".into(),
            task_id: "job.build_and_test".into(),
            map_index: 7,
            stage: "build_and_test".into(),
            cell_id: Some("cell_0123456789abcdef01234567".into()),
            epoch: Some(3),
        }
    }

    #[test]
    fn managed_task_identity_is_explicit_and_stable() {
        let task = invocation();
        task.validate().expect("valid");
        assert_eq!(
            task.identity(),
            "factory/manual__2026-09-18T10:00:00+00:00/job.build_and_test#7:build_and_test"
        );
    }

    #[test]
    fn cell_identity_is_all_or_nothing() {
        let mut task = invocation();
        task.epoch = None;
        assert_eq!(
            task.validate().expect_err("incomplete"),
            TaskInvocationError::IncompleteCellIdentity
        );
    }

    #[test]
    fn stage_is_a_contract_name_not_an_argv_fragment() {
        let mut task = invocation();
        task.stage = "build; rm -rf /".into();
        assert_eq!(
            task.validate().expect_err("unsafe"),
            TaskInvocationError::InvalidStage
        );
    }
}
