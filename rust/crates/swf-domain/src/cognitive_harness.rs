//! Cognitive harness contracts for the Liquid / Dark Software Factory.
//!
//! This module models cognition, never durable authority.
//!
//! System 1 creates useful entropy. System 2 measures and destroys entropy. The phase controller
//! chooses the useful cognitive posture. Gauge fixing collapses representation-equivalent worlds.
//! Promotion-relevant state is restricted to gauge-invariant observables. Crossing into durable
//! reality still requires the fenced authority/control kernel outside this module.

use std::collections::{BTreeMap, BTreeSet};

use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};

use crate::phase_control::{ControlMode, FactoryPhase, PhaseAssessment};

pub const COGNITIVE_HARNESS_SCHEMA_VERSION: u32 = 1;
pub const COGNITIVE_HARNESS_AUTHORITY: &str = "search-only";
pub const REALITY_BOUNDARY: &str = "rust-authority-kernel";

pub const GAUGE_DEPENDENT_QUANTITIES: &[&str] = &[
    "agent_id",
    "model",
    "prompt",
    "reasoning_style",
    "token_count",
    "branch_name",
    "explanation",
    "trajectory_id",
    "runtime_name",
];

pub const GAUGE_INVARIANT_QUANTITIES: &[&str] = &[
    "candidate_digest",
    "source_digest",
    "recipe_digest",
    "policy_digest",
    "evidence_digest",
    "artifact_digest",
    "external_effect_digest",
    "cell_id",
    "epoch",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CognitiveLayer {
    Reflex,
    System1,
    System2,
    Consolidation,
    AuthorityBoundary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CognitiveTimescale {
    MillisecondsSeconds,
    SecondsMinutes,
    MinutesHours,
    HoursDays,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QuantityKind {
    GaugeDependent,
    GaugeInvariant,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorldAction {
    ExpandWorlds,
    CoordinateWorlds,
    FreezeWorlds,
    PruneWorlds,
    VerifyWorld,
    PerturbWorld,
    DrainWorlds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryPhase {
    Observation,
    Trace,
    Correlated,
    CandidateBelief,
    MemoryCrystal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryAction {
    Record,
    Retain,
    Correlate,
    Challenge,
    Crystallize,
    Compact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CognitiveAttention {
    Routine,
    Exception,
    AuthorityBoundary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MeasurementKind {
    Test,
    StaticAnalysis,
    Replay,
    Performance,
    Security,
    Formal,
    Human,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImprovementDisposition {
    Shadow,
    Candidate,
    Adoptable,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldCandidate {
    pub world_id: String,
    pub trajectory_id: String,
    pub model: String,
    pub runtime: String,
    pub role: String,
    pub candidate_digest: String,
    pub source_digest: String,
    pub recipe_digest: String,
    pub policy_digest: String,
    pub evidence_digest: Option<String>,
    pub artifact_digest: Option<String>,
    pub external_effect_digest: Option<String>,
    pub verified: bool,
    pub score: f64,
    pub cost: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct InvariantObservables<'a> {
    candidate_digest: &'a str,
    source_digest: &'a str,
    recipe_digest: &'a str,
    policy_digest: &'a str,
    evidence_digest: Option<&'a str>,
    artifact_digest: Option<&'a str>,
    external_effect_digest: Option<&'a str>,
    verified: bool,
}

impl WorldCandidate {
    pub fn validate(&self) -> Result<(), CognitiveError> {
        for (name, value) in [
            ("world_id", self.world_id.as_str()),
            ("trajectory_id", self.trajectory_id.as_str()),
            ("candidate_digest", self.candidate_digest.as_str()),
            ("source_digest", self.source_digest.as_str()),
            ("recipe_digest", self.recipe_digest.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CognitiveError::EmptyField(name));
            }
        }
        if !self.score.is_finite() {
            return Err(CognitiveError::InvalidNumber("score"));
        }
        if !self.cost.is_finite() || self.cost < 0.0 {
            return Err(CognitiveError::InvalidNumber("cost"));
        }
        Ok(())
    }

    fn invariant_observables(&self) -> InvariantObservables<'_> {
        InvariantObservables {
            candidate_digest: &self.candidate_digest,
            source_digest: &self.source_digest,
            recipe_digest: &self.recipe_digest,
            policy_digest: &self.policy_digest,
            evidence_digest: self.evidence_digest.as_deref(),
            artifact_digest: self.artifact_digest.as_deref(),
            external_effect_digest: self.external_effect_digest.as_deref(),
            verified: self.verified,
        }
    }

    pub fn invariant_fingerprint(&self) -> Result<String, CognitiveError> {
        self.validate()?;
        stable_digest(&self.invariant_observables())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GaugeEquivalenceClass {
    pub invariant_fingerprint: String,
    pub canonical_world_id: String,
    pub member_world_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsembleMember {
    pub member_id: String,
    pub model: String,
    pub role: String,
    pub runtime: String,
    pub behavior_signature: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairwiseCorrelation {
    pub left: String,
    pub right: String,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StochasticField {
    pub source: String,
    pub probabilities: BTreeMap<String, f64>,
    pub authority: String,
}

impl StochasticField {
    pub fn new(source: impl Into<String>, probabilities: BTreeMap<String, f64>) -> Self {
        Self {
            source: source.into(),
            probabilities,
            authority: "exploration-only".into(),
        }
    }

    pub fn normalized(&self, allowed: &[String]) -> Result<BTreeMap<String, f64>, CognitiveError> {
        if allowed.is_empty() {
            return Err(CognitiveError::EmptyAllowedValues);
        }
        let allowed_set: BTreeSet<&str> = allowed.iter().map(String::as_str).collect();
        if self
            .probabilities
            .keys()
            .any(|value| !allowed_set.contains(value.as_str()))
        {
            return Err(CognitiveError::UndeclaredFieldValue);
        }

        let mut weights = BTreeMap::new();
        for value in allowed {
            let weight = *self.probabilities.get(value).unwrap_or(&0.0);
            if !weight.is_finite() || weight < 0.0 {
                return Err(CognitiveError::InvalidNumber("stochastic field weight"));
            }
            weights.insert(value.clone(), weight);
        }

        let total: f64 = weights.values().sum();
        if total <= 0.0 {
            let uniform = 1.0 / weights.len() as f64;
            return Ok(weights.into_keys().map(|key| (key, uniform)).collect());
        }
        Ok(weights
            .into_iter()
            .map(|(key, value)| (key, value / total))
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryEvidence {
    pub recurrence: f64,
    pub independent_confirmations: u32,
    pub contradictions: u32,
    pub evidence_strength: f64,
    pub context_pressure: f64,
}

impl MemoryEvidence {
    pub fn validate(&self) -> Result<(), CognitiveError> {
        for (name, value) in [
            ("recurrence", self.recurrence),
            ("evidence_strength", self.evidence_strength),
            ("context_pressure", self.context_pressure),
        ] {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(CognitiveError::InvalidNumber(name));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementReceipt {
    pub world_id: String,
    pub kind: MeasurementKind,
    pub observable: String,
    pub result_digest: String,
    pub evidence_digest: String,
    pub independent: bool,
    pub passed: bool,
}

impl MeasurementReceipt {
    pub fn validate(&self) -> Result<(), CognitiveError> {
        for (name, value) in [
            ("world_id", self.world_id.as_str()),
            ("observable", self.observable.as_str()),
            ("result_digest", self.result_digest.as_str()),
            ("evidence_digest", self.evidence_digest.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CognitiveError::EmptyField(name));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DissipationSnapshot {
    pub discarded_worlds: u64,
    pub reclaimed_contexts: u64,
    pub cleaned_sandboxes: u64,
    pub cancelled_retries: u64,
    pub stale_memories_retired: u64,
}

impl DissipationSnapshot {
    pub fn total(&self) -> u64 {
        self.discarded_worlds
            + self.reclaimed_contexts
            + self.cleaned_sandboxes
            + self.cancelled_retries
            + self.stale_memories_retired
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfImprovementExperiment {
    pub experiment_id: String,
    pub baseline_digest: String,
    pub candidate_world_ids: Vec<String>,
    pub objective_digest: String,
    pub evidence_digest: Option<String>,
    pub disposition: ImprovementDisposition,
    pub authority: String,
}

impl SelfImprovementExperiment {
    pub fn validate(&self) -> Result<(), CognitiveError> {
        for (name, value) in [
            ("experiment_id", self.experiment_id.as_str()),
            ("baseline_digest", self.baseline_digest.as_str()),
            ("objective_digest", self.objective_digest.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CognitiveError::EmptyField(name));
            }
        }
        if self.candidate_world_ids.is_empty() {
            return Err(CognitiveError::EmptyCandidateWorlds);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CognitivePlan {
    pub schema_version: u32,
    pub authority: String,
    pub reality_boundary: String,
    pub layer: CognitiveLayer,
    pub timescale: CognitiveTimescale,
    pub system1_enabled: bool,
    pub system2_enabled: bool,
    pub world_action: WorldAction,
    pub memory_action: MemoryAction,
    pub attention: CognitiveAttention,
    pub may_request_authority: bool,
    pub ensemble_diversity: f64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityRequest {
    pub cell_id: String,
    pub epoch: u64,
    pub candidate_digest: String,
    pub source_digest: String,
    pub recipe_digest: String,
    pub policy_digest: String,
    pub evidence_digest: String,
    pub requested_effect: String,
    pub authority: String,
    pub requires: String,
}

impl AuthorityRequest {
    pub fn new(
        cell_id: impl Into<String>,
        epoch: u64,
        candidate_digest: impl Into<String>,
        source_digest: impl Into<String>,
        recipe_digest: impl Into<String>,
        policy_digest: impl Into<String>,
        evidence_digest: impl Into<String>,
        requested_effect: impl Into<String>,
    ) -> Self {
        Self {
            cell_id: cell_id.into(),
            epoch,
            candidate_digest: candidate_digest.into(),
            source_digest: source_digest.into(),
            recipe_digest: recipe_digest.into(),
            policy_digest: policy_digest.into(),
            evidence_digest: evidence_digest.into(),
            requested_effect: requested_effect.into(),
            authority: "request-only".into(),
            requires: REALITY_BOUNDARY.into(),
        }
    }

    pub fn validate(&self) -> Result<(), CognitiveError> {
        for (name, value) in [
            ("cell_id", self.cell_id.as_str()),
            ("candidate_digest", self.candidate_digest.as_str()),
            ("source_digest", self.source_digest.as_str()),
            ("recipe_digest", self.recipe_digest.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
            ("evidence_digest", self.evidence_digest.as_str()),
            ("requested_effect", self.requested_effect.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CognitiveError::EmptyField(name));
            }
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, CognitiveError> {
        self.validate()?;
        stable_digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CognitiveReceipt {
    pub schema_version: u32,
    pub authority: String,
    pub phase_assessment_digest: String,
    pub worlds_digest: String,
    pub ensemble_digest: String,
    pub plan: CognitivePlan,
    pub authority_request_digest: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CognitiveError {
    #[error("{0} must be nonempty")]
    EmptyField(&'static str),
    #[error("{0} is outside the cognitive harness numeric contract")]
    InvalidNumber(&'static str),
    #[error("allowed stochastic field values must be nonempty")]
    EmptyAllowedValues,
    #[error("stochastic field contains an undeclared value")]
    UndeclaredFieldValue,
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("authority may only be requested from a crystal verification posture")]
    PrematureAuthorityRequest,
    #[error("self-improvement experiment requires at least one candidate world")]
    EmptyCandidateWorlds,
}

pub fn validate_measurement_set(
    world: &WorldCandidate,
    measurements: &[MeasurementReceipt],
) -> Result<bool, CognitiveError> {
    let mut relevant = 0usize;
    let mut independent = false;
    for measurement in measurements {
        if measurement.world_id == world.world_id {
            measurement.validate()?;
            relevant += 1;
            independent |= measurement.independent;
            if !measurement.passed {
                return Ok(false);
            }
        }
    }
    Ok(relevant > 0 && independent)
}

pub fn classify_self_improvement(
    experiment: &SelfImprovementExperiment,
    evidence_complete: bool,
    independent_verification: bool,
) -> Result<ImprovementDisposition, CognitiveError> {
    experiment.validate()?;
    Ok(if !evidence_complete {
        ImprovementDisposition::Shadow
    } else if !independent_verification {
        ImprovementDisposition::Candidate
    } else {
        ImprovementDisposition::Adoptable
    })
}

fn stable_digest<T: Serialize>(value: &T) -> Result<String, CognitiveError> {
    let payload = serde_json::to_vec(value)?;
    let hash = digest(&SHA256, &payload);
    Ok(hash.as_ref().iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn quantity_kind(name: &str) -> QuantityKind {
    if GAUGE_DEPENDENT_QUANTITIES.contains(&name) {
        QuantityKind::GaugeDependent
    } else if GAUGE_INVARIANT_QUANTITIES.contains(&name) {
        QuantityKind::GaugeInvariant
    } else {
        QuantityKind::Unknown
    }
}

pub fn gauge_fix(worlds: &[WorldCandidate]) -> Result<Vec<GaugeEquivalenceClass>, CognitiveError> {
    let mut groups: BTreeMap<String, Vec<&WorldCandidate>> = BTreeMap::new();
    for world in worlds {
        groups
            .entry(world.invariant_fingerprint()?)
            .or_default()
            .push(world);
    }

    let mut classes = Vec::with_capacity(groups.len());
    for (fingerprint, mut members) in groups {
        members.sort_by(|left, right| {
            (&left.world_id, &left.trajectory_id).cmp(&(&right.world_id, &right.trajectory_id))
        });
        classes.push(GaugeEquivalenceClass {
            invariant_fingerprint: fingerprint,
            canonical_world_id: members[0].world_id.clone(),
            member_world_ids: members.iter().map(|world| world.world_id.clone()).collect(),
        });
    }
    Ok(classes)
}

pub fn pairwise_correlation(left: &EnsembleMember, right: &EnsembleMember) -> PairwiseCorrelation {
    let union = left
        .behavior_signature
        .union(&right.behavior_signature)
        .count();
    let intersection = left
        .behavior_signature
        .intersection(&right.behavior_signature)
        .count();

    let value = if union == 0 {
        1.0
    } else {
        intersection as f64 / union as f64
    };

    PairwiseCorrelation {
        left: left.member_id.clone(),
        right: right.member_id.clone(),
        value: round6(value),
    }
}

pub fn ensemble_diversity(members: &[EnsembleMember]) -> f64 {
    if members.len() < 2 {
        return 0.0;
    }
    let mut sum = 0.0;
    let mut count = 0usize;
    for (index, left) in members.iter().enumerate() {
        for right in &members[index + 1..] {
            sum += pairwise_correlation(left, right).value;
            count += 1;
        }
    }
    round6(1.0 - (sum / count as f64))
}

pub fn marginal_information_value(
    correlation: f64,
    expected_information: f64,
    cost: f64,
) -> Result<f64, CognitiveError> {
    for (name, value) in [
        ("correlation", correlation),
        ("expected_information", expected_information),
    ] {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err(CognitiveError::InvalidNumber(name));
        }
    }
    if !cost.is_finite() || cost <= 0.0 {
        return Err(CognitiveError::InvalidNumber("cost"));
    }
    Ok(((1.0 - correlation) * expected_information) / cost)
}

pub fn memory_phase(evidence: &MemoryEvidence) -> Result<MemoryPhase, CognitiveError> {
    evidence.validate()?;
    if evidence.contradictions > 0 {
        return Ok(MemoryPhase::CandidateBelief);
    }
    if evidence.independent_confirmations >= 3
        && evidence.recurrence >= 0.75
        && evidence.evidence_strength >= 0.85
    {
        return Ok(MemoryPhase::MemoryCrystal);
    }
    if evidence.independent_confirmations >= 2 && evidence.evidence_strength >= 0.65 {
        return Ok(MemoryPhase::CandidateBelief);
    }
    if evidence.recurrence >= 0.50 {
        return Ok(MemoryPhase::Correlated);
    }
    if evidence.independent_confirmations >= 1 {
        return Ok(MemoryPhase::Trace);
    }
    Ok(MemoryPhase::Observation)
}

fn memory_action(
    evidence: Option<&MemoryEvidence>,
    mode: ControlMode,
) -> Result<MemoryAction, CognitiveError> {
    if matches!(mode, ControlMode::Drain | ControlMode::Verify) {
        return Ok(MemoryAction::Compact);
    }
    let Some(evidence) = evidence else {
        return Ok(MemoryAction::Record);
    };
    Ok(match memory_phase(evidence)? {
        MemoryPhase::Observation => MemoryAction::Record,
        MemoryPhase::Trace => MemoryAction::Retain,
        MemoryPhase::Correlated => MemoryAction::Correlate,
        MemoryPhase::CandidateBelief => MemoryAction::Challenge,
        MemoryPhase::MemoryCrystal => MemoryAction::Crystallize,
    })
}

pub fn plan_cognition(
    phase: &PhaseAssessment,
    ensemble_members: &[EnsembleMember],
    memory_evidence: Option<&MemoryEvidence>,
    human_attention_pressure: f64,
) -> Result<CognitivePlan, CognitiveError> {
    if !human_attention_pressure.is_finite()
        || !(0.0..=1.0).contains(&human_attention_pressure)
    {
        return Err(CognitiveError::InvalidNumber("human_attention_pressure"));
    }

    let diversity = ensemble_diversity(ensemble_members);
    let mode = phase.recommendation.mode;
    let (layer, timescale, world_action, system1_enabled, system2_enabled) = match mode {
        ControlMode::Diverge => (
            CognitiveLayer::System1,
            CognitiveTimescale::SecondsMinutes,
            WorldAction::ExpandWorlds,
            true,
            false,
        ),
        ControlMode::Coordinate => (
            CognitiveLayer::System1,
            CognitiveTimescale::SecondsMinutes,
            WorldAction::CoordinateWorlds,
            true,
            true,
        ),
        ControlMode::Measure => (
            CognitiveLayer::System2,
            CognitiveTimescale::MinutesHours,
            WorldAction::FreezeWorlds,
            false,
            true,
        ),
        ControlMode::Anneal => (
            CognitiveLayer::System2,
            CognitiveTimescale::MinutesHours,
            WorldAction::PruneWorlds,
            false,
            true,
        ),
        ControlMode::Verify => (
            CognitiveLayer::AuthorityBoundary,
            CognitiveTimescale::MinutesHours,
            WorldAction::VerifyWorld,
            false,
            true,
        ),
        ControlMode::Perturb => (
            CognitiveLayer::System1,
            CognitiveTimescale::SecondsMinutes,
            WorldAction::PerturbWorld,
            true,
            true,
        ),
        ControlMode::Drain => (
            CognitiveLayer::Reflex,
            CognitiveTimescale::MillisecondsSeconds,
            WorldAction::DrainWorlds,
            false,
            true,
        ),
    };

    let may_request_authority = mode == ControlMode::Verify
        && phase.phase == FactoryPhase::Crystal
        && phase.observation.evidence_completeness >= 0.90
        && phase.observation.verifier_disagreement <= 0.15;

    let attention = if may_request_authority {
        CognitiveAttention::AuthorityBoundary
    } else if human_attention_pressure >= 0.80
        || matches!(
            mode,
            ControlMode::Measure | ControlMode::Perturb | ControlMode::Drain
        )
    {
        CognitiveAttention::Exception
    } else {
        CognitiveAttention::Routine
    };

    Ok(CognitivePlan {
        schema_version: COGNITIVE_HARNESS_SCHEMA_VERSION,
        authority: COGNITIVE_HARNESS_AUTHORITY.into(),
        reality_boundary: REALITY_BOUNDARY.into(),
        layer,
        timescale,
        system1_enabled,
        system2_enabled,
        world_action,
        memory_action: memory_action(memory_evidence, mode)?,
        attention,
        may_request_authority,
        ensemble_diversity: diversity,
        reason: format!(
            "phase={:?} mode={mode:?}; layer={layer:?}; diversity={diversity:.3}; reality remains behind {REALITY_BOUNDARY}",
            phase.phase
        ),
    })
}

pub fn make_receipt(
    phase: &PhaseAssessment,
    worlds: &[WorldCandidate],
    ensemble_members: &[EnsembleMember],
    memory_evidence: Option<&MemoryEvidence>,
    human_attention_pressure: f64,
    authority_request: Option<&AuthorityRequest>,
) -> Result<CognitiveReceipt, CognitiveError> {
    let plan = plan_cognition(
        phase,
        ensemble_members,
        memory_evidence,
        human_attention_pressure,
    )?;

    let authority_request_digest = match authority_request {
        Some(request) if plan.may_request_authority => Some(request.digest()?),
        Some(_) => return Err(CognitiveError::PrematureAuthorityRequest),
        None => None,
    };

    let phase_assessment_digest = stable_digest(phase)?;
    let invariant_worlds: Vec<InvariantObservables<'_>> =
        worlds.iter().map(WorldCandidate::invariant_observables).collect();

    Ok(CognitiveReceipt {
        schema_version: COGNITIVE_HARNESS_SCHEMA_VERSION,
        authority: COGNITIVE_HARNESS_AUTHORITY.into(),
        phase_assessment_digest,
        worlds_digest: stable_digest(&invariant_worlds)?,
        ensemble_digest: stable_digest(&ensemble_members)?,
        plan,
        authority_request_digest,
    })
}

fn round6(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}
