//! Pure factory/run contracts for the Rust-first manager.
//!
//! These types intentionally know nothing about Airflow transport. A logical factory run exists
//! before it is scheduled and keeps the same identity if the scheduler binding is recreated.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FactoryName(String);

impl FactoryName {
    pub fn parse(value: impl Into<String>) -> Result<Self, FactoryError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(FactoryError::InvalidName(
                "factory name must be nonempty".into(),
            ));
        }
        if trimmed.len() > 128 {
            return Err(FactoryError::InvalidName(
                "factory name must be at most 128 characters".into(),
            ));
        }
        if !trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        {
            return Err(FactoryError::InvalidName(
                "factory name may contain only letters, digits, '-', '_' and '.'".into(),
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FactoryRunId(String);

impl FactoryRunId {
    pub fn parse(value: impl Into<String>) -> Result<Self, FactoryError> {
        let value = value.into();
        let trimmed = value.trim();
        if !trimmed.starts_with("frun_") || trimmed.len() < 13 {
            return Err(FactoryError::InvalidRunId(
                "factory run id must start with 'frun_' and include a stable suffix".into(),
            ));
        }
        if !trimmed[5..].chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(FactoryError::InvalidRunId(
                "factory run suffix must be hexadecimal".into(),
            ));
        }
        Ok(Self(trimmed.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Stable logical identity for one governed outer-harness request.
    ///
    /// Airflow run identity is intentionally absent: retries may create a new scheduler binding
    /// while the logical factory run remains the same.
    pub fn for_request(request: &FactoryRunRequest) -> Result<Self, FactoryError> {
        request.validate()?;
        let harness = request.harness.as_deref().ok_or_else(|| {
            FactoryError::InvalidRequest(
                "stable FactoryRunId derivation requires harness + factory_session".into(),
            )
        })?;
        let session = request.factory_session.as_deref().ok_or_else(|| {
            FactoryError::InvalidRequest(
                "stable FactoryRunId derivation requires harness + factory_session".into(),
            )
        })?;

        let mut issues = request.issues.clone();
        issues.sort();
        let mut targets = request.targets.clone();
        targets.sort();

        let mut hasher = Sha256::new();
        for value in [
            request.factory.as_str(),
            harness,
            session,
            &issues.join("\u{0}"),
            &targets.join("\u{0}"),
        ] {
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        let digest = format!("{:x}", hasher.finalize());
        Self::parse(format!("frun_{}", &digest[..32]))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorySpec {
    pub name: FactoryName,
    pub line: String,
    #[serde(default)]
    pub targets: Vec<String>,
}

impl FactorySpec {
    pub fn validate(&self) -> Result<(), FactoryError> {
        if self.line.trim().is_empty() {
            return Err(FactoryError::InvalidSpec(
                "factory line must be nonempty".into(),
            ));
        }
        if self.targets.iter().any(|target| target.trim().is_empty()) {
            return Err(FactoryError::InvalidSpec(
                "factory targets must be nonempty repository names".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactoryRunRequest {
    pub factory: FactoryName,
    pub issues: Vec<String>,
    #[serde(default)]
    pub targets: Vec<String>,
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub factory_session: Option<String>,
}

impl FactoryRunRequest {
    pub fn validate(&self) -> Result<(), FactoryError> {
        if self.issues.is_empty() {
            return Err(FactoryError::InvalidRequest(
                "factory run needs at least one issue".into(),
            ));
        }
        if self
            .issues
            .iter()
            .any(|issue| issue.trim().is_empty() || issue.len() > 128)
        {
            return Err(FactoryError::InvalidRequest(
                "issue references must be nonempty and at most 128 characters".into(),
            ));
        }
        if self
            .issues
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != self.issues.len()
        {
            return Err(FactoryError::InvalidRequest(
                "duplicate issue references are not allowed".into(),
            ));
        }
        match (&self.harness, &self.factory_session) {
            (Some(harness), Some(session))
                if !harness.trim().is_empty() && !session.trim().is_empty() => {}
            (None, None) => {}
            _ => {
                return Err(FactoryError::InvalidRequest(
                    "harness and factory_session must be supplied together".into(),
                ))
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactoryRunState {
    Admitted,
    Scheduled,
    Running,
    WaitingApproval,
    Verifying,
    Delivered,
    Failed,
    Cancelled,
    InDoubt,
}

impl FactoryRunState {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Delivered | Self::Failed | Self::Cancelled | Self::InDoubt
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerBinding {
    pub scheduler: String,
    pub dag_id: String,
    pub dag_run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactoryRunStatus {
    pub run_id: FactoryRunId,
    pub factory: FactoryName,
    pub state: FactoryRunState,
    #[serde(default)]
    pub scheduler: Option<SchedulerBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FactoryError {
    #[error("{0}")]
    InvalidName(String),
    #[error("{0}")]
    InvalidRunId(String),
    #[error("{0}")]
    InvalidSpec(String),
    #[error("{0}")]
    InvalidRequest(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_name_is_strict_and_script_friendly() {
        assert_eq!(
            FactoryName::parse("research-prod").unwrap().as_str(),
            "research-prod"
        );
        assert!(FactoryName::parse("").is_err());
        assert!(FactoryName::parse("has spaces").is_err());
    }

    #[test]
    fn request_binds_harness_identity_as_a_pair() {
        let request = FactoryRunRequest {
            factory: FactoryName::parse("default").unwrap(),
            issues: vec!["42".into()],
            targets: vec![],
            harness: Some("codex".into()),
            factory_session: None,
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn run_identity_is_independent_of_airflow_binding() {
        let status = FactoryRunStatus {
            run_id: FactoryRunId::parse("frun_0123456789abcdef").unwrap(),
            factory: FactoryName::parse("default").unwrap(),
            state: FactoryRunState::Scheduled,
            scheduler: Some(SchedulerBinding {
                scheduler: "airflow".into(),
                dag_id: "swf__factory".into(),
                dag_run_id: "manual__abc".into(),
            }),
        };
        assert_eq!(status.run_id.as_str(), "frun_0123456789abcdef");
        assert!(!status.state.terminal());
    }

    #[test]
    fn governed_harness_request_has_scheduler_independent_identity() {
        let a = FactoryRunRequest {
            factory: FactoryName::parse("research").unwrap(),
            issues: vec!["42".into(), "17".into()],
            targets: vec!["owner/repo".into()],
            harness: Some("codex".into()),
            factory_session: Some("session-7".into()),
        };
        let mut b = a.clone();
        b.issues.reverse();

        let first = FactoryRunId::for_request(&a).unwrap();
        let second = FactoryRunId::for_request(&b).unwrap();
        assert_eq!(first, second);
        assert!(first.as_str().starts_with("frun_"));
    }

    #[test]
    fn in_doubt_is_terminal_for_automatic_progress() {
        assert!(FactoryRunState::InDoubt.terminal());
        assert!(!FactoryRunState::WaitingApproval.terminal());
    }
}
