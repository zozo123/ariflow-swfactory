//! HTTP client for the Python factory backend. Service credentials stay on the server.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use swf_domain::cell::{CellEvent, CellRecord};
use swf_domain::metrics::{MetricsSummary, RunMetrics};
use swf_domain::model::{IssueRef, PullRequest, SandboxRef};
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::airflow::{AirflowApi, Auth};
use crate::error::{AdapterError, Result};
use crate::traits::{CommandRunner, Deliveries, MetricsStore, PrHead, Sandboxes, SystemRunner};

pub struct FactoryApi {
    base: String,
    token: String,
    http: Client,
    timeout: Duration,
}

impl FactoryApi {
    pub fn new(base: &str, token: String, timeout: Duration) -> Result<Self> {
        let url =
            url::Url::parse(base).map_err(|_| AdapterError::refused("invalid backend URL"))?;
        let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(AdapterError::refused(
                "backend URL requires HTTPS or loopback HTTP, without credentials, query or fragment",
            ));
        }
        if token.len() < 32 || token.chars().any(char::is_whitespace) {
            return Err(AdapterError::Auth {
                detail:
                    "set SWF_BACKEND_TOKEN to the backend's operator token (at least 32 characters)"
                        .into(),
            });
        }
        let http = Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| AdapterError::refused("could not create backend client"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            token,
            http,
            timeout,
        })
    }

    pub fn runs(&self, ui_url: &str) -> Result<AirflowApi> {
        Ok(AirflowApi::new(
            &format!("{}/v1/airflow", self.base),
            Auth::Token(self.token.clone()),
            self.timeout,
        )?
        .with_ui_base(ui_url))
    }

    pub async fn call<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Value,
        cancel: &CancellationToken,
    ) -> Result<T> {
        let request = async {
            let response = self
                .http
                .post(format!("{}/v1{}", self.base, path))
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await
                .map_err(|_| AdapterError::Unreachable {
                    what: "factory backend".into(),
                    detail:
                        "request failed; a mutation may have committed, inspect before retrying"
                            .into(),
                })?;
            let status = response.status().as_u16();
            let value: Value = response.json().await.map_err(|_| AdapterError::Decode {
                what: "factory backend".into(),
                detail: "invalid JSON response; inspect mutation outcome before retrying".into(),
            })?;
            if status >= 300 {
                let detail = sanitize_line(
                    value
                        .get("detail")
                        .and_then(Value::as_str)
                        .unwrap_or("backend operation failed"),
                );
                return Err(match status {
                    401 => AdapterError::Auth { detail },
                    404 => AdapterError::NotFound { what: detail },
                    409 => AdapterError::Conflict { detail },
                    _ => AdapterError::Status {
                        code: Some(status),
                        detail,
                    },
                });
            }
            serde_json::from_value(value).map_err(|_| AdapterError::Decode {
                what: "factory backend".into(),
                detail: "response does not match API v1".into(),
            })
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(AdapterError::Cancelled),
            result = request => result,
        }
    }

    /// Newest durable Factory Cells, newest mutation first.
    pub async fn cells(
        &self,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<CellRecord>> {
        self.call("/cells", json!({"limit": limit}), cancel).await
    }

    /// One durable Factory Cell projection.
    pub async fn cell(&self, cell_id: &str, cancel: &CancellationToken) -> Result<CellRecord> {
        self.call("/cells/inspect", json!({"cell_id": cell_id}), cancel)
            .await
    }

    /// The append-only history of one durable Factory Cell.
    pub async fn cell_history(
        &self,
        cell_id: &str,
        cancel: &CancellationToken,
    ) -> Result<Vec<CellEvent>> {
        self.call("/cells/history", json!({"cell_id": cell_id}), cancel)
            .await
    }
}

#[async_trait]
impl Deliveries for FactoryApi {
    async fn prs(
        &self,
        label: &str,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<PullRequest>> {
        let mut rows: Vec<PullRequest> = self
            .call(
                "/deliveries/prs",
                json!({"label": label, "limit": limit}),
                cancel,
            )
            .await?;
        for row in &mut rows {
            row.title = sanitize_line(&row.title);
            row.head = sanitize_line(&row.head);
            row.state = sanitize_line(&row.state);
            row.checks = sanitize_line(&row.checks);
            row.url = sanitize_line(&row.url);
            row.labels = row.labels.iter().map(|s| sanitize_line(s)).collect();
        }
        Ok(rows)
    }

    async fn issues(
        &self,
        label: &str,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<IssueRef>> {
        let mut rows: Vec<IssueRef> = self
            .call(
                "/deliveries/issues",
                json!({"label": label, "limit": limit}),
                cancel,
            )
            .await?;
        for row in &mut rows {
            row.title = sanitize_line(&row.title);
            row.url = sanitize_line(&row.url);
            row.labels = row.labels.iter().map(|s| sanitize_line(s)).collect();
        }
        Ok(rows)
    }

    async fn pr_for_branch(
        &self,
        branch: &str,
        cancel: &CancellationToken,
    ) -> Result<Option<PrHead>> {
        let mut row: Option<PrHead> = self
            .call("/deliveries/head", json!({"branch": branch}), cancel)
            .await?;
        if let Some(row) = &mut row {
            row.title = sanitize_line(&row.title);
            row.url = sanitize_line(&row.url);
            row.state = sanitize_line(&row.state);
            row.head_sha = sanitize_line(&row.head_sha);
            row.base_ref = sanitize_line(&row.base_ref);
            row.labels = row.labels.iter().map(|s| sanitize_line(s)).collect();
        }
        Ok(row)
    }

    async fn checks(&self, number: i64, cancel: &CancellationToken) -> Result<String> {
        let value: String = self
            .call("/deliveries/checks", json!({"number": number}), cancel)
            .await?;
        Ok(sanitize_line(&value))
    }

    async fn pr_view(&self, number: i64, cancel: &CancellationToken) -> Result<Vec<String>> {
        let target: String = self
            .call("/deliveries/url", json!({"number": number}), cancel)
            .await?;
        let url = url::Url::parse(&target).map_err(|_| AdapterError::refused("invalid PR URL"))?;
        if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
            return Err(AdapterError::refused("PR browser links must use HTTPS"));
        }
        let program = if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        };
        let argv = vec![program.to_string(), target];
        let result = SystemRunner
            .run(&argv, Duration::from_secs(15), cancel)
            .await?;
        if result.code != 0 {
            return Err(AdapterError::refused("local browser opener failed"));
        }
        Ok(argv)
    }
}

#[async_trait]
impl Sandboxes for FactoryApi {
    async fn list(&self, cancel: &CancellationToken) -> Result<Vec<SandboxRef>> {
        let mut rows: Vec<SandboxRef> = self.call("/workers", json!({}), cancel).await?;
        for row in &mut rows {
            row.name = sanitize_line(&row.name);
            row.status = sanitize_line(&row.status);
            row.created_by = sanitize_line(&row.created_by);
        }
        Ok(rows)
    }

    async fn remove(&self, name: &str, cancel: &CancellationToken) -> Result<Vec<String>> {
        self.call("/workers/remove", json!({"name": name}), cancel)
            .await
    }
}

#[async_trait]
impl MetricsStore for FactoryApi {
    async fn runs(&self, cancel: &CancellationToken) -> Result<Vec<RunMetrics>> {
        self.call("/metrics/runs", json!({}), cancel).await
    }

    async fn summary(&self, cancel: &CancellationToken) -> Result<MetricsSummary> {
        self.call("/metrics/summary", json!({}), cancel).await
    }
}
