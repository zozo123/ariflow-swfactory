//! Rust-native factory manager use case.
//!
//! The harness-facing CLI speaks domain contracts to this module. This module decides how those
//! contracts bind to the configured scheduler transport through Ops; the CLI never constructs
//! Airflow requests or legacy submit arguments itself.

use swf_domain::FactoryRunRequest;
use tokio_util::sync::CancellationToken;

use crate::ops::{Ops, OpsError, Result};
use crate::submit::{Submission, SubmitRequest};

/// Convert one logical factory request into the scheduler-facing submission contract.
pub fn scheduler_request(request: &FactoryRunRequest) -> Result<SubmitRequest> {
    request
        .validate()
        .map_err(|error| OpsError::usage(error.to_string()))?;
    Ok(SubmitRequest {
        issues: request.issues.clone(),
        blueprint: request.factory.as_str().to_string(),
        targets: request.targets.clone(),
        harness: request.harness.clone(),
        factory_id: request.factory_session.clone(),
    })
}

/// Execute one logical factory run through the configured Rust scheduler binding.
pub async fn run(
    ops: &Ops,
    request: &FactoryRunRequest,
    cancel: &CancellationToken,
) -> Result<Submission> {
    let scheduler = scheduler_request(request)?;
    ops.submit(&scheduler, cancel).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use swf_domain::FactoryName;

    #[test]
    fn scheduler_binding_preserves_logical_factory_and_harness_identity() {
        let request = FactoryRunRequest {
            factory: FactoryName::parse("research").unwrap(),
            issues: vec!["42".into(), "demo/issue.md".into()],
            targets: vec!["owner/repo".into()],
            harness: Some("codex".into()),
            factory_session: Some("session-7".into()),
        };

        let bound = scheduler_request(&request).unwrap();
        assert_eq!(bound.blueprint, "research");
        assert_eq!(bound.issues, request.issues);
        assert_eq!(bound.targets, request.targets);
        assert_eq!(bound.harness.as_deref(), Some("codex"));
        assert_eq!(bound.factory_id.as_deref(), Some("session-7"));
    }

    #[test]
    fn invalid_logical_request_is_refused_before_scheduler_binding() {
        let request = FactoryRunRequest {
            factory: FactoryName::parse("research").unwrap(),
            issues: vec!["42".into()],
            targets: vec![],
            harness: Some("codex".into()),
            factory_session: None,
        };

        assert!(scheduler_request(&request).is_err());
    }
}
