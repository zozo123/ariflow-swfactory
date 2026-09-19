//! Rust-first factory manager bridge.
//!
//! Airflow remains the lifecycle scheduler. This use case owns logical factory identity and
//! returns the scheduler binding as data rather than treating dag_run_id as the product identity.

use serde::{Deserialize, Serialize};
use swf_adapters::traits::Runs;
use swf_domain::factory::{
    FactoryRunId, FactoryRunRequest, FactoryRunState, FactoryRunStatus, SchedulerBinding,
};
use swf_domain::ids::RunRef;
use tokio_util::sync::CancellationToken;

use crate::ops::{OpsError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartedFactoryRun {
    pub status: FactoryRunStatus,
    pub scheduler_url: String,
    pub harness: String,
    pub factory_session: String,
}

pub async fn start(
    runs: &dyn Runs,
    request: &FactoryRunRequest,
    cancel: &CancellationToken,
) -> Result<StartedFactoryRun> {
    request
        .validate()
        .map_err(|error| OpsError::usage(error.to_string()))?;

    if !request.targets.is_empty() {
        return Err(OpsError::usage(
            "Rust FactoryManager target overrides are not wired to Airflow conf yet",
        )
        .with_hint("use a factory/blueprint whose targets are already declared"));
    }

    let logical_id =
        FactoryRunId::for_request(request).map_err(|error| OpsError::usage(error.to_string()))?;
    let harness = request.harness.clone().ok_or_else(|| {
        OpsError::usage("Rust FactoryManager requires --harness paired with --factory-id")
    })?;
    let factory_session = request.factory_session.clone().ok_or_else(|| {
        OpsError::usage("Rust FactoryManager requires --factory-id paired with --harness")
    })?;

    let dag_id = request.factory.as_str().to_string();
    let dag_run_id = runs.trigger(&dag_id, &request.issues, cancel).await?;
    let run_ref = RunRef::new(dag_id.clone(), dag_run_id.clone());

    Ok(StartedFactoryRun {
        status: FactoryRunStatus {
            run_id: logical_id,
            factory: request.factory.clone(),
            state: FactoryRunState::Scheduled,
            scheduler: Some(SchedulerBinding {
                scheduler: "airflow".into(),
                dag_id,
                dag_run_id,
            }),
        },
        scheduler_url: runs.run_url(&run_ref),
        harness,
        factory_session,
    })
}
