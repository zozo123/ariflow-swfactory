//! Read-only advisory contracts for backlog relationship suggestions.
//!
//! The types in this module deliberately carry no mutation verbs. An adviser may suggest how a
//! proposal relates to existing work, but it cannot suppress a proposal, close an issue, enroll
//! work, authorize execution, or affect promotion. Deterministic post-processing owns the safety
//! rule: missing acceptance criteria always requires human review, regardless of model confidence.

use serde::{Deserialize, Serialize};

pub const ADVISORY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvisoryRelation {
    PotentialDuplicate,
    Extends,
    Conflicts,
    Related,
    Unrelated,
    InsufficientEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
}

impl WorkItem {
    pub fn has_acceptance_criteria(&self) -> bool {
        self.acceptance_criteria
            .iter()
            .any(|item| !item.trim().is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExistingIssue {
    pub number: u64,
    pub title: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
}

impl ExistingIssue {
    pub fn has_acceptance_criteria(&self) -> bool {
        self.acceptance_criteria
            .iter()
            .any(|item| !item.trim().is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdvisorySuggestion {
    pub existing_issue: u64,
    pub relationship: AdvisoryRelation,
    pub confidence: f64,
    #[serde(default)]
    pub needs_review: bool,
    #[serde(default)]
    pub rationale: String,
}

impl AdvisorySuggestion {
    pub fn validate(&self) -> Result<(), AdvisoryError> {
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(AdvisoryError::InvalidConfidence);
        }
        if self.existing_issue == 0 {
            return Err(AdvisoryError::InvalidIssueNumber);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisoryRequest {
    pub schema_version: u32,
    pub rubric_version: String,
    pub proposal: WorkItem,
    pub candidates: Vec<ExistingIssue>,
}

impl AdvisoryRequest {
    pub fn new(
        rubric_version: impl Into<String>,
        proposal: WorkItem,
        candidates: Vec<ExistingIssue>,
    ) -> Self {
        Self {
            schema_version: ADVISORY_SCHEMA_VERSION,
            rubric_version: rubric_version.into(),
            proposal,
            candidates,
        }
    }

    pub fn validate(&self) -> Result<(), AdvisoryError> {
        if self.schema_version != ADVISORY_SCHEMA_VERSION {
            return Err(AdvisoryError::UnsupportedSchema);
        }
        if self.rubric_version.trim().is_empty() {
            return Err(AdvisoryError::MissingRubricVersion);
        }
        if self.proposal.id.trim().is_empty() || self.proposal.title.trim().is_empty() {
            return Err(AdvisoryError::InvalidProposal);
        }
        if self.candidates.iter().any(|issue| issue.number == 0) {
            return Err(AdvisoryError::InvalidIssueNumber);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReviewedSuggestion {
    pub existing_issue: u64,
    pub relationship: AdvisoryRelation,
    pub confidence: f64,
    pub needs_review: bool,
    pub acceptance_criteria_missing: bool,
    pub rationale: String,
}

pub fn enforce_review_guard(
    proposal: &WorkItem,
    issue: &ExistingIssue,
    suggestion: &AdvisorySuggestion,
) -> Result<ReviewedSuggestion, AdvisoryError> {
    suggestion.validate()?;
    if suggestion.existing_issue != issue.number {
        return Err(AdvisoryError::IssueMismatch);
    }

    let acceptance_criteria_missing =
        !proposal.has_acceptance_criteria() || !issue.has_acceptance_criteria();
    let needs_review = suggestion.needs_review
        || acceptance_criteria_missing
        || matches!(
            suggestion.relationship,
            AdvisoryRelation::InsufficientEvidence
        );

    Ok(ReviewedSuggestion {
        existing_issue: suggestion.existing_issue,
        relationship: suggestion.relationship,
        confidence: suggestion.confidence,
        needs_review,
        acceptance_criteria_missing,
        rationale: suggestion.rationale.clone(),
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelatedIssue {
    pub issue: ExistingIssue,
    pub lexical_score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowIssueView {
    pub issue: ExistingIssue,
    pub lexical_score: f64,
    pub advisory: Option<ReviewedSuggestion>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowTriageView {
    pub proposal: WorkItem,
    pub related: Vec<ShadowIssueView>,
}

pub fn deterministic_related_issues(
    proposal: &WorkItem,
    candidates: &[ExistingIssue],
    limit: usize,
) -> Vec<RelatedIssue> {
    let proposal_tokens = tokens(&format!("{} {}", proposal.title, proposal.body));
    let mut scored: Vec<RelatedIssue> = candidates
        .iter()
        .cloned()
        .map(|issue| {
            let issue_tokens = tokens(&format!("{} {}", issue.title, issue.body));
            let lexical_score = jaccard(&proposal_tokens, &issue_tokens);
            RelatedIssue {
                issue,
                lexical_score,
            }
        })
        .collect();

    scored.sort_by(|left, right| {
        right
            .lexical_score
            .partial_cmp(&left.lexical_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.issue.number.cmp(&right.issue.number))
    });
    scored.truncate(limit.min(scored.len()));
    scored
}

pub fn build_shadow_view(
    proposal: &WorkItem,
    candidates: &[ExistingIssue],
    suggestions: &[AdvisorySuggestion],
    limit: usize,
) -> Result<ShadowTriageView, AdvisoryError> {
    let related = deterministic_related_issues(proposal, candidates, limit)
        .into_iter()
        .map(|ranked| {
            let advisory = suggestions
                .iter()
                .find(|suggestion| suggestion.existing_issue == ranked.issue.number)
                .map(|suggestion| enforce_review_guard(proposal, &ranked.issue, suggestion))
                .transpose()?;
            Ok(ShadowIssueView {
                issue: ranked.issue,
                lexical_score: ranked.lexical_score,
                advisory,
            })
        })
        .collect::<Result<Vec<_>, AdvisoryError>>()?;

    Ok(ShadowTriageView {
        proposal: proposal.clone(),
        related,
    })
}

fn tokens(text: &str) -> std::collections::BTreeSet<String> {
    const STOP: &[&str] = &[
        "a", "an", "and", "before", "by", "for", "in", "of", "on", "or", "the", "to",
    ];
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|word| {
            let normalized = word.trim().to_ascii_lowercase();
            if normalized.len() < 2 || STOP.contains(&normalized.as_str()) {
                None
            } else {
                Some(normalized)
            }
        })
        .collect()
}

fn jaccard(
    left: &std::collections::BTreeSet<String>,
    right: &std::collections::BTreeSet<String>,
) -> f64 {
    if left.is_empty() && right.is_empty() {
        return 0.0;
    }
    let intersection = left.intersection(right).count() as f64;
    let union = left.union(right).count() as f64;
    if union == 0.0 {
        0.0
    } else {
        intersection / union
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdvisoryError {
    #[error("unsupported advisory schema")]
    UnsupportedSchema,
    #[error("advisory rubric version is required")]
    MissingRubricVersion,
    #[error("proposal id and title are required")]
    InvalidProposal,
    #[error("issue number must be positive")]
    InvalidIssueNumber,
    #[error("advisory confidence must be finite and between 0 and 1")]
    InvalidConfidence,
    #[error("advisory suggestion targets a different issue")]
    IssueMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(criteria: &[&str]) -> WorkItem {
        WorkItem {
            id: "proposal-1".into(),
            title: "Reject stale execution requests before a stage handler runs.".into(),
            body: String::new(),
            acceptance_criteria: criteria.iter().map(|item| (*item).into()).collect(),
        }
    }

    fn issue(criteria: &[&str]) -> ExistingIssue {
        ExistingIssue {
            number: 2255,
            title: "Require a valid fence before invoking a stage handler".into(),
            body: String::new(),
            acceptance_criteria: criteria.iter().map(|item| (*item).into()).collect(),
        }
    }

    #[test]
    fn missing_acceptance_criteria_forces_review_even_at_high_confidence() {
        let suggestion = AdvisorySuggestion {
            existing_issue: 2255,
            relationship: AdvisoryRelation::PotentialDuplicate,
            confidence: 0.93,
            needs_review: false,
            rationale: "similar wording".into(),
        };

        let reviewed = enforce_review_guard(&proposal(&[]), &issue(&[]), &suggestion).unwrap();

        assert!(reviewed.needs_review);
        assert!(reviewed.acceptance_criteria_missing);
        assert_eq!(reviewed.relationship, AdvisoryRelation::PotentialDuplicate);
        assert_eq!(reviewed.confidence, 0.93);
    }

    #[test]
    fn explicit_review_request_is_preserved() {
        let suggestion = AdvisorySuggestion {
            existing_issue: 2255,
            relationship: AdvisoryRelation::Related,
            confidence: 0.4,
            needs_review: true,
            rationale: String::new(),
        };

        let reviewed = enforce_review_guard(
            &proposal(&["stale requests fail before handler invocation"]),
            &issue(&["handler checks epoch fence"]),
            &suggestion,
        )
        .unwrap();

        assert!(reviewed.needs_review);
        assert!(!reviewed.acceptance_criteria_missing);
    }

    #[test]
    fn complete_criteria_can_remain_advisory_without_forcing_review() {
        let suggestion = AdvisorySuggestion {
            existing_issue: 2255,
            relationship: AdvisoryRelation::Extends,
            confidence: 0.8,
            needs_review: false,
            rationale: String::new(),
        };

        let reviewed = enforce_review_guard(
            &proposal(&["request is rejected before handler entry"]),
            &issue(&["valid epoch fence is required"]),
            &suggestion,
        )
        .unwrap();

        assert!(!reviewed.needs_review);
        assert!(!reviewed.acceptance_criteria_missing);
    }

    #[test]
    fn insufficient_evidence_always_requires_review() {
        let suggestion = AdvisorySuggestion {
            existing_issue: 2255,
            relationship: AdvisoryRelation::InsufficientEvidence,
            confidence: 0.99,
            needs_review: false,
            rationale: String::new(),
        };

        let reviewed = enforce_review_guard(
            &proposal(&["observable proposal outcome"]),
            &issue(&["observable issue outcome"]),
            &suggestion,
        )
        .unwrap();

        assert!(reviewed.needs_review);
    }

    #[test]
    fn invalid_confidence_is_refused() {
        for confidence in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
            let suggestion = AdvisorySuggestion {
                existing_issue: 2255,
                relationship: AdvisoryRelation::Related,
                confidence,
                needs_review: false,
                rationale: String::new(),
            };
            assert_eq!(suggestion.validate(), Err(AdvisoryError::InvalidConfidence));
        }
    }

    #[test]
    fn a_suggestion_for_another_issue_is_refused() {
        let suggestion = AdvisorySuggestion {
            existing_issue: 2228,
            relationship: AdvisoryRelation::Related,
            confidence: 0.6,
            needs_review: true,
            rationale: String::new(),
        };

        assert_eq!(
            enforce_review_guard(&proposal(&["x"]), &issue(&["y"]), &suggestion),
            Err(AdvisoryError::IssueMismatch)
        );
    }

    #[test]
    fn request_is_versioned_and_requires_a_rubric() {
        let mut request = AdvisoryRequest::new(
            "jev-triage-v1",
            proposal(&["observable outcome"]),
            vec![issue(&["observable outcome"])],
        );
        assert_eq!(request.validate(), Ok(()));

        request.rubric_version.clear();
        assert_eq!(request.validate(), Err(AdvisoryError::MissingRubricVersion));
    }

    #[test]
    fn relation_enum_rejects_unknown_wire_values() {
        let raw = r#""automatic_close""#;
        assert!(serde_json::from_str::<AdvisoryRelation>(raw).is_err());
    }

    #[test]
    fn shadow_view_keeps_the_original_proposal_and_acceptance_criteria() {
        let proposal = proposal(&["stale request is rejected before handler entry"]);
        let issues = vec![
            ExistingIssue {
                number: 2228,
                title: "Port CellStore epoch fencing into Rust".into(),
                body: String::new(),
                acceptance_criteria: vec!["Rust rejects stale epochs".into()],
            },
            issue(&["stage handler requires a valid fence"]),
        ];
        let suggestions = vec![AdvisorySuggestion {
            existing_issue: 2255,
            relationship: AdvisoryRelation::PotentialDuplicate,
            confidence: 0.82,
            needs_review: true,
            rationale: "same observable handler boundary".into(),
        }];

        let view = build_shadow_view(&proposal, &issues, &suggestions, 5).unwrap();

        assert_eq!(view.proposal, proposal);
        assert_eq!(view.related.len(), 2);
        assert!(view
            .related
            .iter()
            .any(|row| row.issue.number == 2255 && row.advisory.is_some()));
        assert!(view
            .related
            .iter()
            .any(|row| row.issue.number == 2228 && row.advisory.is_none()));
    }

    #[test]
    fn deterministic_shortlist_is_stable_under_input_order() {
        let proposal = proposal(&["request is fenced before stage invocation"]);
        let first = ExistingIssue {
            number: 2228,
            title: "Port CellStore epoch fencing into Rust".into(),
            body: String::new(),
            acceptance_criteria: vec!["Rust rejects stale epochs".into()],
        };
        let second = issue(&["stage handler requires valid fence"]);

        let a = deterministic_related_issues(&proposal, &[first.clone(), second.clone()], 2);
        let b = deterministic_related_issues(&proposal, &[second, first], 2);

        assert_eq!(
            a.iter().map(|item| item.issue.number).collect::<Vec<_>>(),
            b.iter().map(|item| item.issue.number).collect::<Vec<_>>()
        );
    }

    #[test]
    fn no_adviser_still_returns_the_deterministic_shadow_view() {
        let proposal = proposal(&["request is fenced"]);
        let view = build_shadow_view(&proposal, &[issue(&["handler is fenced"])], &[], 5).unwrap();

        assert_eq!(view.related.len(), 1);
        assert!(view.related[0].advisory.is_none());
        assert_eq!(view.proposal.id, "proposal-1");
    }
}
