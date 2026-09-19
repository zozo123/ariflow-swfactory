use std::path::PathBuf;

use serde::Deserialize;
use swf_domain::advisory::{enforce_review_guard, AdvisorySuggestion, ExistingIssue, WorkItem};

#[derive(Debug, Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    proposal: WorkItem,
    issue: ExistingIssue,
    suggestion: AdvisorySuggestion,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct Expected {
    needs_review: bool,
    acceptance_criteria_missing: bool,
}

#[test]
fn jev_shadow_regressions_preserve_ambiguity_and_operator_review() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/advisory/jev_shadow_cases.json");
    let fixture: Fixture = serde_json::from_str(&std::fs::read_to_string(path).expect("fixture"))
        .expect("valid fixture");

    assert!(fixture.cases.len() >= 8);
    for case in fixture.cases {
        let reviewed =
            enforce_review_guard(&case.proposal, &case.issue, &case.suggestion).expect(&case.name);
        assert_eq!(
            reviewed.needs_review, case.expected.needs_review,
            "{}",
            case.name
        );
        assert_eq!(
            reviewed.acceptance_criteria_missing, case.expected.acceptance_criteria_missing,
            "{}",
            case.name
        );
    }
}
