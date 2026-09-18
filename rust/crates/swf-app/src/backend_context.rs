//! One backend connection policy for every application operation.
//!
//! URL override, direct-mode refusal, credential lookup and timeout live here so CLI/TUI
//! operations cannot silently drift. Callers may share one `BackendContext` to share the same
//! `FactoryApi`; convenience `connect` constructors still use exactly this policy.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use swf_adapters::factory::FactoryApi;

use crate::context::Context;
use crate::ops::{OpsError, Result};

#[derive(Clone)]
pub struct BackendContext {
    api: Arc<FactoryApi>,
    base_url: String,
}

impl BackendContext {
    /// Resolve the one backend endpoint policy shared by every application caller.
    pub fn endpoint(context: &Context) -> Option<String> {
        let backend_url =
            env::var("SWF_BACKEND_URL").unwrap_or_else(|_| context.backend_url.clone());
        let backend_url = backend_url.trim().trim_end_matches('/').to_string();
        (!backend_url.is_empty()).then_some(backend_url)
    }

    pub fn connect(context: &Context, timeout: Duration, feature: &str) -> Result<Self> {
        let Some(backend_url) = Self::endpoint(context) else {
            return Err(OpsError::operational(format!(
                "{feature} are served only by the factory backend, and context {:?} is explicitly direct; \
                 swf never widens direct mode to local credentials to answer them",
                context.name
            ))
            .with_hint(format!(
                "give it a backend: swf context add {} --airflow-url {} --backend-url URL --force, \
                 then export SWF_BACKEND_TOKEN",
                context.name, context.airflow_url
            )));
        };
        let token = env::var("SWF_BACKEND_TOKEN").unwrap_or_default();
        let api = FactoryApi::new(&backend_url, token, timeout)?;
        Ok(Self {
            api: Arc::new(api),
            base_url: backend_url,
        })
    }

    pub fn api(&self) -> Arc<FactoryApi> {
        Arc::clone(&self.api)
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}
