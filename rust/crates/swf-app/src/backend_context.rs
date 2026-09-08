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
    pub fn connect(context: &Context, timeout: Duration, feature: &str) -> Result<Self> {
        let backend_url =
            env::var("SWF_BACKEND_URL").unwrap_or_else(|_| context.backend_url.clone());
        if backend_url.is_empty() {
            return Err(OpsError::operational(format!(
                "{feature} require the Python backend; this context is in direct mode"
            ))
            .with_hint("configure --backend-url and SWF_BACKEND_TOKEN"));
        }
        let token = env::var("SWF_BACKEND_TOKEN").unwrap_or_default();
        let api = FactoryApi::new(&backend_url, token, timeout)?;
        Ok(Self {
            api: Arc::new(api),
            base_url: backend_url.trim_end_matches('/').to_string(),
        })
    }

    pub fn api(&self) -> Arc<FactoryApi> {
        Arc::clone(&self.api)
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}
