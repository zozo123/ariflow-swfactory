use std::path::PathBuf;

use serde::Deserialize;
use swf_domain::phase_control::{
    assess, branching_ratio, normalized_entropy, AttentionClass, CandidateDirective, ContextDirective,
    ControlMode, FactoryPhase, PhaseObservation, QueueDirective, SpawnDirective, TrajectoryMode,
    VerificationDirective, PHASE_CONTROL_AUTHORITY, PHASE_CONTROL_SCHEMA_VERSION,
};

#[derive(Debug, Deserialize)]
struct Fixture {
    schema_version: u32,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    observation: PhaseObservation,
    #[serde(default)]
    previous_phase: Option<FactoryPhase>,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct Expected {
    raw_phase: FactoryPhase,
    phase: FactoryPhase,
    mode: ControlMode,
    spawn: SpawnDirective,
    trajectory: TrajectoryMode,
    context: ContextDirective,
    candidates: CandidateDirective,
    queue: QueueDirective,
    verification: VerificationDirective,
    attention: AttentionClass,
    allow_new_implementation_lanes: bool,
}

#[test]
fn python_and_rust_share_one_phase_contract() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/contract/phase_control.json");
    let fixture: Fixture =
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("valid fixture");

    assert_eq!(fixture.schema_version, PHASE_CONTROL_SCHEMA_VERSION);
    for case in fixture.cases {
        let assessment = assess(&case.observation, case.previous_phase).expect(&case.name);
        assert_eq!(assessment.raw_phase, case.expected.raw_phase, "{}", case.name);
        assert_eq!(assessment.phase, case.expected.phase, "{}", case.name);
        assert_eq!(assessment.recommendation.mode, case.expected.mode, "{}", case.name);
        assert_eq!(assessment.recommendation.spawn, case.expected.spawn, "{}", case.name);
        assert_eq!(
            assessment.recommendation.trajectory,
            case.expected.trajectory,
            "{}",
            case.name
        );
        assert_eq!(
            assessment.recommendation.context,
            case.expected.context,
            "{}",
            case.name
        );
        assert_eq!(
            assessment.recommendation.candidates,
            case.expected.candidates,
            "{}",
            case.name
        );
        assert_eq!(assessment.recommendation.queue, case.expected.queue, "{}", case.name);
        assert_eq!(
            assessment.recommendation.verification,
            case.expected.verification,
            "{}",
            case.name
        );
        assert_eq!(
            assessment.recommendation.attention,
            case.expected.attention,
            "{}",
            case.name
        );
        assert_eq!(
            assessment.recommendation.allow_new_implementation_lanes,
            case.expected.allow_new_implementation_lanes,
            "{}",
            case.name
        );
        assert_eq!(assessment.authority, PHASE_CONTROL_AUTHORITY, "{}", case.name);
        assert_eq!(assessment.observation, case.observation, "{}", case.name);
        assert_eq!(assessment.previous_phase, case.previous_phase, "{}", case.name);
    }
}

#[test]
fn entropy_and_branching_are_bounded_helpers_not_authority() {
    assert_eq!(normalized_entropy(&[1.0]).unwrap(), 0.0);
    assert!((normalized_entropy(&[1.0, 1.0, 1.0]).unwrap() - 1.0).abs() < 1e-12);
    assert!(normalized_entropy(&[9.0, 1.0]).unwrap() < normalized_entropy(&[1.0, 1.0]).unwrap());
    assert_eq!(branching_ratio(0, 0).unwrap(), 0.0);
    assert_eq!(branching_ratio(6, 3).unwrap(), 2.0);
    assert_eq!(branching_ratio(10, 0).unwrap(), 4.0);
}

#[test]
fn crystal_is_a_verification_posture_not_a_promotion_decision() {
    let observation = PhaseObservation {
        candidate_entropy: 0.10,
        coherence: 0.95,
        mobility: 0.20,
        queue_pressure: 0.10,
        queue_acceleration: -0.10,
        resource_pressure: 0.10,
        branching_ratio: 0.20,
        evidence_completeness: 0.96,
        context_pressure: 0.40,
        debt_pressure: 0.05,
        verifier_disagreement: 0.05,
    };
    let assessment = assess(&observation, None).unwrap();
    assert_eq!(assessment.phase, FactoryPhase::Crystal);
    assert_eq!(assessment.recommendation.mode, ControlMode::Verify);
    assert!(!assessment.recommendation.allow_new_implementation_lanes);
    assert_eq!(assessment.authority, "search-only");
}
