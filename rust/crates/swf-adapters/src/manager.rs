//! Typed transport from Airflow lifecycle tasks to the Rust factory manager.
//!
//! Airflow schedules; the manager executes factory use cases. The wire payload is the
//! versioned `swf-domain::manager_protocol`, so Python DAG glue never re-implements
//! stage semantics.

use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
use swf_domain::{ManagerEnvelope, StageInvocation, StageReceipt};
use tokio_util::sync::CancellationToken;

use crate::error::{truncate, AdapterError, Result};
use crate::traits::DEFAULT_HTTP_TIMEOUT;

pub const STAGE_EXECUTE_PATH: &str = "/v1/manager/stages/execute";

#[derive(Debug, Clone)]
pub struct ManagerApi {
    base: String,
    token: Option<String>,
    http: Client,
    timeout: Duration,
}

impl ManagerApi {
    pub fn new(base_url: &str, token: Option<String>, timeout: Duration) -> Result<Self> {
        let base = base_url.trim().trim_end_matches('/');
        if base.is_empty() {
            return Err(AdapterError::refused("manager base URL must be nonempty"));
        }
        let http = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .user_agent(concat!("swf/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| AdapterError::Unreachable {
                what: "manager HTTP client".into(),
                detail: truncate(&error.to_string()),
            })?;
        Ok(Self {
            base: base.to_string(),
            token,
            http,
            timeout,
        })
    }

    pub fn with_defaults(base_url: &str, token: Option<String>) -> Result<Self> {
        Self::new(base_url, token, DEFAULT_HTTP_TIMEOUT)
    }

    pub fn endpoint(&self) -> String {
        format!("{}{STAGE_EXECUTE_PATH}", self.base)
    }

    pub async fn execute_stage(
        &self,
        request_id: impl Into<String>,
        invocation: StageInvocation,
        cancel: &CancellationToken,
    ) -> Result<StageReceipt> {
        invocation
            .validate()
            .map_err(|error| AdapterError::refused(error.to_string()))?;
        let envelope = ManagerEnvelope::new(request_id, invocation.clone())
            .map_err(|error| AdapterError::refused(error.to_string()))?;
        let url = self.endpoint();

        let mut request = self
            .http
            .post(&url)
            .header(reqwest::header::ACCEPT, "application/json")
            .json(&envelope);
        if let Some(token) = self.token.as_deref() {
            request = request.bearer_auth(token);
        }

        let send = async {
            let response = request.send().await.map_err(|error| match AdapterError::from_reqwest(&url, &error) {
                AdapterError::Timeout { what, .. } => AdapterError::Timeout {
                    what,
                    after: self.timeout,
                },
                other => other,
            })?;
            let status = response.status();
            let text = response.text().await.map_err(|error| AdapterError::Decode {
                what: url.clone(),
                detail: truncate(&error.to_string()),
            })?;
            if !status.is_success() {
                return Err(AdapterError::from_status(
                    status.as_u16(),
                    &format!("POST {url}"),
                    &service_detail(&text),
                ));
            }
            let receipt: StageReceipt =
                serde_json::from_str(&text).map_err(|error| AdapterError::Decode {
                    what: url.clone(),
                    detail: truncate(&format!("{error}: {}", excerpt(&text))),
                })?;
            verify_receipt(&invocation, &receipt)?;
            Ok(receipt)
        };

        tokio::select! {
            _ = cancel.cancelled() => Err(AdapterError::Cancelled),
            result = send => result,
        }
    }
}

fn verify_receipt(invocation: &StageInvocation, receipt: &StageReceipt) -> Result<()> {
    if receipt.run_id != invocation.run_id
        || receipt.cell_id != invocation.cell_id
        || receipt.epoch != invocation.epoch
        || receipt.stage != invocation.stage
        || receipt.attempt != invocation.attempt
    {
        return Err(AdapterError::Conflict {
            detail: "manager receipt identity does not match the stage invocation".into(),
        });
    }
    Ok(())
}

fn service_detail(text: &str) -> String {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|value| value.get("detail").cloned())
        .map(|detail| match detail {
            Value::String(text) => text,
            other => other.to_string(),
        })
        .unwrap_or_else(|| excerpt(text))
}

fn excerpt(text: &str) -> String {
    truncate(if text.trim().is_empty() {
        "<empty response>"
    } else {
        text
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use swf_domain::{
        AirflowInvocation, FactoryName, FactoryRunId, StageDisposition,
    };

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
    fn manager_endpoint_is_stable_and_has_no_airflow_database_path() {
        let api = ManagerApi::with_defaults("http://manager:8083/", None).unwrap();
        assert_eq!(
            api.endpoint(),
            "http://manager:8083/v1/manager/stages/execute"
        );
    }

    #[test]
    fn receipt_must_bind_back_to_the_exact_invocation() {
        let request = invocation();
        let good = StageReceipt {
            run_id: request.run_id.clone(),
            cell_id: request.cell_id.clone(),
            epoch: request.epoch,
            stage: request.stage.clone(),
            attempt: request.attempt,
            disposition: StageDisposition::Completed,
            evidence_digest: Some("sha256:abc".into()),
            detail: None,
        };
        verify_receipt(&request, &good).unwrap();

        let mut stale = good;
        stale.epoch += 1;
        assert!(matches!(
            verify_receipt(&request, &stale),
            Err(AdapterError::Conflict { .. })
        ));
    }
}
