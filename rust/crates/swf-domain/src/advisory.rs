//! Advisory backlog-triage contracts.
//!
//! Advisory results can help an operator navigate related work. They are not authority: no type in
//! this module can close an issue, suppress a proposal, enroll work, execute a stage, or promote a
//! candidate.

use serde::{Deserialize, Serialize};

pub const ADVISORY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvisoryRelationship {
    PotentialDuplicate,
    Extension,
    Dependency,
    Conflict,
    Related,
    Unrelated,
    InsufficientEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub title: String,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
}

impl WorkItem {
    pub fn has_observable_acceptance_criteria(&self) -> bool {
        self.acceptance_criteria
            .iter()
            .any(|criterion| !criterion.trim().is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdvisorySuggestion {
    pub schema_version: u32,
    pub existing_issue: u64,
    pub relationship: AdvisoryRelationship,
    pub confidence: f64,
    pub probability: f64,
    pub needs_review: bool,
    #[serde(default)]
    pub rationale: String,
}

impl AdvisorySuggestion {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != ADVISORY_SCHEMA_VERSION {
            return Err("unsupported advisory schema version");
        }
        if self.existing_issue == 0 {
            return Err("existing issue number must be positive");
        }
        for value in [self.confidence, self.probability] {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err("advisory probability/confidence must be finite and within [0,1]");
            }
        }
        Ok(())
    }
}

/// Apply deterministic safety semantics after an untrusted adviser returns.
///
/// Missing acceptance criteria always force operator review, regardless of confidence. The adviser
/// can suggest a relationship but cannot turn ambiguity into authority.
pub fn normalize_for_review(
    proposal: &WorkItem,
    existing: &WorkItem,
    mut suggestion: AdvisorySuggestion,
) -> Result<AdvisorySuggestion, &'static str> {
    suggestion.validate()?;

    if !proposal.has_observable_acceptance_criteria()
        || !existing.has_observable_acceptance_criteria()
        || suggestion.relationship == AdvisoryRelationship::InsufficientEvidence
    {
        suggestion.needs_review = true;
    }

    Ok(suggestion)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn suggestion(relationship: AdvisoryRelationship, confidence: f64) -> AdvisorySuggestion {
        AdvisorySuggestion {
            schema_version: ADVISORY_SCHEMA_VERSION,
            existing_issue: 2255,
            relationship,
            confidence,
            probability: confidence,
            needs_review: false,
            rationale: "model hint".into(),
        }
    }

    #[test]
    fn vague_upload_pair_forces_review_even_at_high_confidence() {
        let proposal = WorkItem {
            title: "Fix the flaky upload".into(),
            acceptance_criteria: vec![],
        };
        let existing = WorkItem {
            title: "Make upload reliable".into(),
            acceptance_criteria: vec![],
        };

        let result = normalize_for_review(
            &proposal,
            &existing,
            suggestion(AdvisoryRelationship::PotentialDuplicate, 0.93),
        )
        .unwrap();

        assert!(result.needs_review);
        assert_eq!(
            result.relationship,
            AdvisoryRelationship::PotentialDuplicate
        );
    }

    #[test]
    fn complete_acceptance_criteria_can_remain_an_advisory_without_forced_review() {
        let proposal = WorkItem {
            title: "Reject stale execution requests before a stage handler runs".into(),
            acceptance_criteria: vec!["stale epoch returns before handler invocation".into()],
        };
        let existing = WorkItem {
            title: "Require a valid fence before invoking a stage handler".into(),
            acceptance_criteria: vec!["handler call count remains zero for stale fences".into()],
        };

        let result = normalize_for_review(
            &proposal,
            &existing,
            suggestion(AdvisoryRelationship::PotentialDuplicate, 0.81),
        )
        .unwrap();

        assert!(!result.needs_review);
    }

    #[test]
    fn invalid_confidence_fails_closed() {
        let item = WorkItem {
            title: "x".into(),
            acceptance_criteria: vec!["observable".into()],
        };
        let err = normalize_for_review(
            &item,
            &item,
            suggestion(AdvisoryRelationship::Related, f64::NAN),
        )
        .unwrap_err();

        assert_eq!(
            err,
            "advisory probability/confidence must be finite and within [0,1]"
        );
    }
}
