//! Factory-wide phase estimation and search-only control recommendations.
//!
//! A phase is observed state. A mode is a reversible search posture. This module is pure domain
//! logic: it has no scheduler, mutation, publication, credential, or promotion authority.

use serde::{Deserialize, Serialize};

pub const PHASE_CONTROL_SCHEMA_VERSION: u32 = 1;
pub const PHASE_CONTROL_AUTHORITY: &str = "search-only";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactoryPhase {
    Gas,
    Liquid,
    Critical,
    Crystal,
    Glass,
    Jammed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlMode {
    Diverge,
    Coordinate,
    Measure,
    Anneal,
    Verify,
    Perturb,
    Drain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrajectoryMode {
    Isolated,
    Forked,
    Specialist,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnDirective {
    Increase,
    Hold,
    Decrease,
    Limited,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextDirective {
    Fresh,
    Retain,
    Compact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateDirective {
    Expand,
    Coordinate,
    Freeze,
    Prune,
    Verify,
    Reset,
    Hold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueDirective {
    Admit,
    Hold,
    Drain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationDirective {
    Normal,
    Increase,
    Maximum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AttentionClass {
    Routine,
    Exception,
    AuthorityBoundary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseObservation {
    pub candidate_entropy: f64,
    pub coherence: f64,
    pub mobility: f64,
    pub queue_pressure: f64,
    pub queue_acceleration: f64,
    pub resource_pressure: f64,
    pub branching_ratio: f64,
    pub evidence_completeness: f64,
    pub context_pressure: f64,
    pub debt_pressure: f64,
    pub verifier_disagreement: f64,
}

impl PhaseObservation {
    pub fn validate(&self) -> Result<(), PhaseControlError> {
        let unit = [
            ("candidate_entropy", self.candidate_entropy),
            ("coherence", self.coherence),
            ("mobility", self.mobility),
            ("queue_pressure", self.queue_pressure),
            ("resource_pressure", self.resource_pressure),
            ("evidence_completeness", self.evidence_completeness),
            ("context_pressure", self.context_pressure),
            ("debt_pressure", self.debt_pressure),
            ("verifier_disagreement", self.verifier_disagreement),
        ];
        for (name, value) in unit {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(PhaseControlError::OutOfRange(name));
            }
        }
        if !self.queue_acceleration.is_finite()
            || !(-1.0..=1.0).contains(&self.queue_acceleration)
        {
            return Err(PhaseControlError::OutOfRange("queue_acceleration"));
        }
        if !self.branching_ratio.is_finite() || !(0.0..=4.0).contains(&self.branching_ratio) {
            return Err(PhaseControlError::OutOfRange("branching_ratio"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseSignals {
    pub order_parameter: f64,
    pub jam_pressure: f64,
    pub transition_pressure: f64,
    pub branching_supercritical: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseRecommendation {
    pub mode: ControlMode,
    pub spawn: SpawnDirective,
    pub trajectory: TrajectoryMode,
    pub context: ContextDirective,
    pub candidates: CandidateDirective,
    pub queue: QueueDirective,
    pub verification: VerificationDirective,
    pub attention: AttentionClass,
    pub allow_new_implementation_lanes: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseAssessment {
    pub schema_version: u32,
    pub authority: String,
    pub observation: PhaseObservation,
    pub previous_phase: Option<FactoryPhase>,
    pub raw_phase: FactoryPhase,
    pub phase: FactoryPhase,
    pub signals: PhaseSignals,
    pub recommendation: PhaseRecommendation,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PhaseControlError {
    #[error("{0} is outside the phase-control contract")]
    OutOfRange(&'static str),
    #[error("entropy weights must be finite and non-negative")]
    InvalidEntropyWeight,
    #[error("branching counts must be non-negative")]
    InvalidBranchingCount,
}

pub fn normalized_entropy(weights: &[f64]) -> Result<f64, PhaseControlError> {
    if weights.is_empty() {
        return Ok(0.0);
    }
    if weights
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err(PhaseControlError::InvalidEntropyWeight);
    }
    let total: f64 = weights.iter().sum();
    if total <= 0.0 {
        return Ok(0.0);
    }
    let probabilities: Vec<f64> = weights
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .map(|value| value / total)
        .collect();
    if probabilities.len() <= 1 {
        return Ok(0.0);
    }
    let entropy = -probabilities
        .iter()
        .map(|probability| probability * probability.ln())
        .sum::<f64>();
    Ok(entropy / (probabilities.len() as f64).ln())
}

pub fn branching_ratio(
    children_or_retries: i64,
    terminal_parents: i64,
) -> Result<f64, PhaseControlError> {
    if children_or_retries < 0 || terminal_parents < 0 {
        return Err(PhaseControlError::InvalidBranchingCount);
    }
    if terminal_parents == 0 {
        return Ok(if children_or_retries == 0 { 0.0 } else { 4.0 });
    }
    Ok((children_or_retries as f64 / terminal_parents as f64).min(4.0))
}

fn round6(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

fn signals(observation: &PhaseObservation) -> Result<PhaseSignals, PhaseControlError> {
    observation.validate()?;
    let jam_pressure = (0.40 * observation.queue_pressure
        + 0.25 * observation.resource_pressure
        + 0.35 * observation.debt_pressure
        + 0.15 * observation.queue_acceleration.max(0.0))
    .min(1.0);

    let order_parameter = (0.35 * observation.coherence
        + 0.35 * observation.evidence_completeness
        + 0.15 * (1.0 - observation.candidate_entropy)
        + 0.15 * (1.0 - observation.verifier_disagreement))
        .clamp(0.0, 1.0);

    let transition_pressure = (0.50 * observation.verifier_disagreement
        + 0.20 * observation.queue_acceleration.abs()
        + 0.20 * (observation.branching_ratio / 1.5).min(1.0)
        + 0.10 * observation.context_pressure)
        .min(1.0);

    Ok(PhaseSignals {
        order_parameter: round6(order_parameter),
        jam_pressure: round6(jam_pressure),
        transition_pressure: round6(transition_pressure),
        branching_supercritical: observation.branching_ratio >= 1.0,
    })
}

fn raw_phase(observation: &PhaseObservation, signals: &PhaseSignals) -> FactoryPhase {
    if signals.jam_pressure >= 0.72
        || (observation.branching_ratio >= 1.25 && observation.queue_pressure >= 0.65)
    {
        return FactoryPhase::Jammed;
    }
    if signals.order_parameter >= 0.85
        && observation.evidence_completeness >= 0.90
        && observation.coherence >= 0.85
        && observation.candidate_entropy <= 0.25
        && observation.verifier_disagreement <= 0.15
        && signals.jam_pressure <= 0.25
        && observation.branching_ratio < 1.0
    {
        return FactoryPhase::Crystal;
    }
    if observation.candidate_entropy <= 0.40
        && observation.mobility <= 0.25
        && observation.evidence_completeness < 0.85
        && observation.coherence < 0.85
    {
        return FactoryPhase::Glass;
    }
    if observation.candidate_entropy >= 0.70
        && observation.coherence <= 0.40
        && observation.evidence_completeness < 0.75
    {
        return FactoryPhase::Gas;
    }
    if signals.transition_pressure >= 0.55 || observation.branching_ratio >= 1.0 {
        return FactoryPhase::Critical;
    }
    FactoryPhase::Liquid
}

fn with_hysteresis(
    raw: FactoryPhase,
    previous: Option<FactoryPhase>,
    observation: &PhaseObservation,
    signals: &PhaseSignals,
) -> FactoryPhase {
    let Some(previous) = previous else {
        return raw;
    };
    if previous == raw || matches!(raw, FactoryPhase::Crystal | FactoryPhase::Jammed) {
        return raw;
    }

    match previous {
        FactoryPhase::Crystal
            if signals.order_parameter >= 0.78
                && observation.evidence_completeness >= 0.82
                && observation.coherence >= 0.78
                && observation.candidate_entropy <= 0.35
                && signals.jam_pressure < 0.45
                && observation.branching_ratio < 1.0 =>
        {
            FactoryPhase::Crystal
        }
        FactoryPhase::Jammed if signals.jam_pressure >= 0.50 => FactoryPhase::Jammed,
        FactoryPhase::Glass
            if observation.mobility <= 0.35
                && observation.evidence_completeness < 0.90
                && observation.coherence < 0.90
                && signals.jam_pressure < 0.70 =>
        {
            FactoryPhase::Glass
        }
        FactoryPhase::Gas
            if raw == FactoryPhase::Liquid
                && observation.candidate_entropy >= 0.58
                && observation.coherence <= 0.50
                && signals.jam_pressure < 0.55 =>
        {
            FactoryPhase::Gas
        }
        FactoryPhase::Critical
            if raw == FactoryPhase::Liquid && signals.transition_pressure >= 0.40 =>
        {
            FactoryPhase::Critical
        }
        FactoryPhase::Liquid
            if raw == FactoryPhase::Gas && observation.candidate_entropy < 0.82 =>
        {
            FactoryPhase::Liquid
        }
        FactoryPhase::Liquid
            if raw == FactoryPhase::Critical
                && signals.transition_pressure < 0.68
                && observation.branching_ratio < 1.0 =>
        {
            FactoryPhase::Liquid
        }
        _ => raw,
    }
}

fn mode(
    phase: FactoryPhase,
    observation: &PhaseObservation,
    signals: &PhaseSignals,
) -> ControlMode {
    match phase {
        FactoryPhase::Jammed => ControlMode::Drain,
        FactoryPhase::Glass => ControlMode::Perturb,
        FactoryPhase::Crystal => ControlMode::Verify,
        FactoryPhase::Gas => ControlMode::Diverge,
        FactoryPhase::Liquid | FactoryPhase::Critical
            if signals.order_parameter >= 0.65
                && observation.evidence_completeness >= 0.70
                && observation.candidate_entropy <= 0.50
                && observation.verifier_disagreement <= 0.35
                && signals.jam_pressure < 0.45
                && observation.branching_ratio < 1.0 =>
        {
            ControlMode::Anneal
        }
        FactoryPhase::Critical => ControlMode::Measure,
        FactoryPhase::Liquid => ControlMode::Coordinate,
    }
}

fn recommendation(mode: ControlMode, observation: &PhaseObservation) -> PhaseRecommendation {
    match mode {
        ControlMode::Diverge => PhaseRecommendation {
            mode,
            spawn: SpawnDirective::Increase,
            trajectory: TrajectoryMode::Isolated,
            context: ContextDirective::Fresh,
            candidates: CandidateDirective::Expand,
            queue: QueueDirective::Admit,
            verification: VerificationDirective::Normal,
            attention: AttentionClass::Routine,
            allow_new_implementation_lanes: true,
            reason: "High diversity and weak coherence: widen independent search while keeping trajectories decorrelated.".into(),
        },
        ControlMode::Coordinate => PhaseRecommendation {
            mode,
            spawn: SpawnDirective::Hold,
            trajectory: TrajectoryMode::Specialist,
            context: if observation.context_pressure >= 0.85 {
                ContextDirective::Compact
            } else {
                ContextDirective::Retain
            },
            candidates: CandidateDirective::Coordinate,
            queue: QueueDirective::Admit,
            verification: VerificationDirective::Normal,
            attention: AttentionClass::Routine,
            allow_new_implementation_lanes: true,
            reason: "Productive liquid regime: keep specialist lanes mobile without adding another scheduler.".into(),
        },
        ControlMode::Measure => PhaseRecommendation {
            mode,
            spawn: SpawnDirective::Stop,
            trajectory: TrajectoryMode::Specialist,
            context: if observation.context_pressure >= 0.80 {
                ContextDirective::Compact
            } else {
                ContextDirective::Retain
            },
            candidates: CandidateDirective::Freeze,
            queue: QueueDirective::Hold,
            verification: VerificationDirective::Increase,
            attention: AttentionClass::Exception,
            allow_new_implementation_lanes: false,
            reason: "Near a transition: stop widening implementation space and spend budget on independent measurement.".into(),
        },
        ControlMode::Anneal => PhaseRecommendation {
            mode,
            spawn: SpawnDirective::Decrease,
            trajectory: TrajectoryMode::Forked,
            context: if observation.context_pressure >= 0.80 {
                ContextDirective::Compact
            } else {
                ContextDirective::Retain
            },
            candidates: CandidateDirective::Prune,
            queue: QueueDirective::Hold,
            verification: VerificationDirective::Increase,
            attention: AttentionClass::Exception,
            allow_new_implementation_lanes: false,
            reason: "Evidence and order are rising: reduce candidate count while increasing verifier independence.".into(),
        },
        ControlMode::Verify => PhaseRecommendation {
            mode,
            spawn: SpawnDirective::Stop,
            trajectory: TrajectoryMode::None,
            context: ContextDirective::Compact,
            candidates: CandidateDirective::Verify,
            queue: QueueDirective::Hold,
            verification: VerificationDirective::Maximum,
            attention: AttentionClass::AuthorityBoundary,
            allow_new_implementation_lanes: false,
            reason: "A low-entropy candidate exists: create no new implementation lanes; verify exact bytes before authority.".into(),
        },
        ControlMode::Perturb => PhaseRecommendation {
            mode,
            spawn: SpawnDirective::Limited,
            trajectory: TrajectoryMode::Isolated,
            context: ContextDirective::Fresh,
            candidates: CandidateDirective::Reset,
            queue: QueueDirective::Hold,
            verification: VerificationDirective::Increase,
            attention: AttentionClass::Exception,
            allow_new_implementation_lanes: true,
            reason: "Low mobility without sufficient evidence indicates a glassy local minimum: inject a bounded fresh trajectory.".into(),
        },
        ControlMode::Drain => PhaseRecommendation {
            mode,
            spawn: SpawnDirective::Stop,
            trajectory: TrajectoryMode::None,
            context: ContextDirective::Compact,
            candidates: CandidateDirective::Hold,
            queue: QueueDirective::Drain,
            verification: VerificationDirective::Maximum,
            attention: AttentionClass::Exception,
            allow_new_implementation_lanes: false,
            reason: "Queue/resource/debt pressure dominates: stop creating work, drain debt, reclaim resources, and recover.".into(),
        },
    }
}

pub fn assess(
    observation: &PhaseObservation,
    previous_phase: Option<FactoryPhase>,
) -> Result<PhaseAssessment, PhaseControlError> {
    let signals = signals(observation)?;
    let raw_phase = raw_phase(observation, &signals);
    let phase = with_hysteresis(raw_phase, previous_phase, observation, &signals);
    let mode = mode(phase, observation, &signals);
    Ok(PhaseAssessment {
        schema_version: PHASE_CONTROL_SCHEMA_VERSION,
        authority: PHASE_CONTROL_AUTHORITY.into(),
        observation: observation.clone(),
        previous_phase,
        raw_phase,
        phase,
        signals,
        recommendation: recommendation(mode, observation),
    })
}
