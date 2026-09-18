//! One Rust dispatch point for Airflow-scheduled factory tasks.
//!
//! The runtime does not schedule, retry or promote. Airflow hands it a validated TaskInvocation;
//! the registry resolves exactly one Rust handler for that stage. Missing stages are refused
//! explicitly so migration cannot silently fall back to a second Python implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use swf_domain::task_runtime::{TaskInvocation, TaskInvocationError};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResult {
    pub stage: String,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum TaskRuntimeError {
    #[error(transparent)]
    InvalidInvocation(#[from] TaskInvocationError),
    #[error("Rust task stage {0:?} has not migrated yet")]
    UnmigratedStage(String),
    #[error("task stage {stage:?} failed: {detail}")]
    Failed { stage: String, detail: String },
}

#[async_trait]
pub trait TaskHandler: Send + Sync {
    async fn execute(
        &self,
        invocation: &TaskInvocation,
        cancel: &CancellationToken,
    ) -> Result<TaskResult, TaskRuntimeError>;
}

#[derive(Default)]
pub struct TaskRuntime {
    handlers: BTreeMap<String, Arc<dyn TaskHandler>>,
}

impl TaskRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<H>(mut self, stage: impl Into<String>, handler: H) -> Self
    where
        H: TaskHandler + 'static,
    {
        self.handlers.insert(stage.into(), Arc::new(handler));
        self
    }

    pub fn migrated_stages(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    pub async fn execute(
        &self,
        invocation: &TaskInvocation,
        cancel: &CancellationToken,
    ) -> Result<TaskResult, TaskRuntimeError> {
        invocation.validate()?;
        let handler = self
            .handlers
            .get(&invocation.stage)
            .ok_or_else(|| TaskRuntimeError::UnmigratedStage(invocation.stage.clone()))?;
        handler.execute(invocation, cancel).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo;

    #[async_trait]
    impl TaskHandler for Echo {
        async fn execute(
            &self,
            invocation: &TaskInvocation,
            _cancel: &CancellationToken,
        ) -> Result<TaskResult, TaskRuntimeError> {
            Ok(TaskResult {
                stage: invocation.stage.clone(),
                detail: invocation.identity(),
            })
        }
    }

    fn invocation(stage: &str) -> TaskInvocation {
        TaskInvocation {
            dag_id: "factory".into(),
            run_id: "manual__1".into(),
            task_id: format!("job.{stage}"),
            map_index: 0,
            stage: stage.into(),
            cell_id: None,
            epoch: None,
        }
    }

    #[tokio::test]
    async fn migrated_stage_uses_exactly_one_rust_handler() {
        let runtime = TaskRuntime::new().register("intent", Echo);
        let result = runtime
            .execute(&invocation("intent"), &CancellationToken::new())
            .await
            .expect("execute");

        assert_eq!(result.stage, "intent");
        assert!(result.detail.ends_with(":intent"));
    }

    #[tokio::test]
    async fn unmigrated_stage_is_refused_instead_of_falling_back_to_python() {
        let error = TaskRuntime::new()
            .execute(&invocation("deliver"), &CancellationToken::new())
            .await
            .expect_err("not migrated");

        assert!(matches!(error, TaskRuntimeError::UnmigratedStage(stage) if stage == "deliver"));
    }

    #[test]
    fn registry_exposes_migration_progress_deterministically() {
        let runtime = TaskRuntime::new().register("plan", Echo).register("intent", Echo);
        assert_eq!(runtime.migrated_stages(), vec!["intent", "plan"]);
    }
}
