//! One Rust dispatch point for Airflow-scheduled factory stages.
//!
//! Airflow decides when to run. The typed manager protocol identifies what to run. This registry
//! resolves exactly one Rust handler. An unmigrated stage is refused explicitly; it never falls
//! through to a second Python implementation behind the same stage name.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use swf_domain::{ProtocolError, StageInvocation};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageResult {
    pub stage: String,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum StageRuntimeError {
    #[error(transparent)]
    InvalidInvocation(#[from] ProtocolError),
    #[error("Rust stage {0:?} has not migrated yet")]
    UnmigratedStage(String),
    #[error("stage {stage:?} failed: {detail}")]
    Failed { stage: String, detail: String },
}

#[async_trait]
pub trait StageHandler: Send + Sync {
    async fn execute(
        &self,
        invocation: &StageInvocation,
        cancel: &CancellationToken,
    ) -> Result<StageResult, StageRuntimeError>;
}

#[derive(Default)]
pub struct StageRuntime {
    handlers: BTreeMap<String, Arc<dyn StageHandler>>,
}

impl StageRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<H>(mut self, stage: impl Into<String>, handler: H) -> Self
    where
        H: StageHandler + 'static,
    {
        self.handlers.insert(stage.into(), Arc::new(handler));
        self
    }

    pub fn migrated_stages(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    pub async fn execute(
        &self,
        invocation: &StageInvocation,
        cancel: &CancellationToken,
    ) -> Result<StageResult, StageRuntimeError> {
        invocation.validate()?;
        let handler = self
            .handlers
            .get(&invocation.stage)
            .ok_or_else(|| StageRuntimeError::UnmigratedStage(invocation.stage.clone()))?;
        handler.execute(invocation, cancel).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swf_domain::{AirflowInvocation, FactoryName, FactoryRunId};

    struct Echo;

    #[async_trait]
    impl StageHandler for Echo {
        async fn execute(
            &self,
            invocation: &StageInvocation,
            _cancel: &CancellationToken,
        ) -> Result<StageResult, StageRuntimeError> {
            Ok(StageResult {
                stage: invocation.stage.clone(),
                detail: format!(
                    "{}/{}/{}#{}",
                    invocation.run_id.as_str(),
                    invocation.cell_id,
                    invocation.stage,
                    invocation.attempt
                ),
            })
        }
    }

    fn invocation(stage: &str) -> StageInvocation {
        StageInvocation {
            factory: FactoryName::parse("research").unwrap(),
            run_id: FactoryRunId::parse("frun_0123456789abcdef").unwrap(),
            cell_id: "cell_0123456789abcdef01234567".into(),
            epoch: 3,
            stage: stage.into(),
            attempt: 1,
            airflow: AirflowInvocation {
                dag_id: "swf__research".into(),
                dag_run_id: "manual__1".into(),
                task_id: format!("job.{stage}"),
                map_index: Some(0),
                try_number: 1,
            },
        }
    }

    #[tokio::test]
    async fn migrated_stage_uses_exactly_one_rust_handler() {
        let runtime = StageRuntime::new().register("intent", Echo);
        let result = runtime
            .execute(&invocation("intent"), &CancellationToken::new())
            .await
            .expect("execute");
        assert_eq!(result.stage, "intent");
    }

    #[tokio::test]
    async fn unmigrated_stage_is_refused_instead_of_falling_back_to_python() {
        let error = StageRuntime::new()
            .execute(&invocation("deliver"), &CancellationToken::new())
            .await
            .expect_err("not migrated");
        assert!(matches!(error, StageRuntimeError::UnmigratedStage(stage) if stage == "deliver"));
    }

    #[test]
    fn registry_exposes_migration_progress_deterministically() {
        let runtime = StageRuntime::new()
            .register("plan", Echo)
            .register("intent", Echo);
        assert_eq!(runtime.migrated_stages(), vec!["intent", "plan"]);
    }
}
