"""Factory-wide phase estimation and search-only control recommendations.

The Liquid Software Factory uses statistical-mechanics language only where it produces measurable,
falsifiable control behavior. A phase is an observed state of the factory. A mode is a reversible
search/control posture recommended for that state.

This module is deliberately pure: it schedules nothing, mutates nothing, grants no capability, and
never lowers a promotion, evidence, or security gate.

Airflow remains the lifecycle scheduler. Factory Cell epochs, policy and evidence remain authoritative.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Literal

PHASE_CONTROL_SCHEMA_VERSION = 1
PHASE_CONTROL_AUTHORITY = "search-only"

Phase = Literal["gas", "liquid", "critical", "crystal", "glass", "jammed"]


class ControlMode(StrEnum):
    DIVERGE = "diverge"
    COORDINATE = "coordinate"
    MEASURE = "measure"
    ANNEAL = "anneal"
    VERIFY = "verify"
    PERTURB = "perturb"
    DRAIN = "drain"


class TrajectoryMode(StrEnum):
    ISOLATED = "isolated"
    FORKED = "forked"
    SPECIALIST = "specialist"
    NONE = "none"


class SpawnDirective(StrEnum):
    INCREASE = "increase"
    HOLD = "hold"
    DECREASE = "decrease"
    LIMITED = "limited"
    STOP = "stop"


class ContextDirective(StrEnum):
    FRESH = "fresh"
    RETAIN = "retain"
    COMPACT = "compact"


class CandidateDirective(StrEnum):
    EXPAND = "expand"
    COORDINATE = "coordinate"
    FREEZE = "freeze"
    PRUNE = "prune"
    VERIFY = "verify"
    RESET = "reset"
    HOLD = "hold"


class QueueDirective(StrEnum):
    ADMIT = "admit"
    HOLD = "hold"
    DRAIN = "drain"


class VerificationDirective(StrEnum):
    NORMAL = "normal"
    INCREASE = "increase"
    MAXIMUM = "maximum"


class AttentionClass(StrEnum):
    ROUTINE = "routine"
    EXCEPTION = "exception"
    AUTHORITY_BOUNDARY = "authority-boundary"


@dataclass(frozen=True)
class PhaseObservation:
    """Dimensionless order parameters for one factory snapshot.

    All normalized fields are in [0, 1]. queue_acceleration is in [-1, 1].
    branching_ratio is children/retries produced per failed/terminal parent and is bounded here
    to [0, 4] only to reject nonsense input; values >= 1 are treated as supercritical.
    """

    candidate_entropy: float
    coherence: float
    mobility: float
    queue_pressure: float
    queue_acceleration: float
    resource_pressure: float
    branching_ratio: float
    evidence_completeness: float
    context_pressure: float
    debt_pressure: float
    verifier_disagreement: float

    def validate(self) -> None:
        unit = {
            "candidate_entropy": self.candidate_entropy,
            "coherence": self.coherence,
            "mobility": self.mobility,
            "queue_pressure": self.queue_pressure,
            "resource_pressure": self.resource_pressure,
            "evidence_completeness": self.evidence_completeness,
            "context_pressure": self.context_pressure,
            "debt_pressure": self.debt_pressure,
            "verifier_disagreement": self.verifier_disagreement,
        }
        for name, value in unit.items():
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be finite and in [0, 1]")
        if not math.isfinite(self.queue_acceleration) or not -1.0 <= self.queue_acceleration <= 1.0:
            raise ValueError("queue_acceleration must be finite and in [-1, 1]")
        if not math.isfinite(self.branching_ratio) or not 0.0 <= self.branching_ratio <= 4.0:
            raise ValueError("branching_ratio must be finite and in [0, 4]")


@dataclass(frozen=True)
class PhaseSignals:
    order_parameter: float
    jam_pressure: float
    transition_pressure: float
    branching_supercritical: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PhaseRecommendation:
    mode: ControlMode
    spawn: SpawnDirective
    trajectory: TrajectoryMode
    context: ContextDirective
    candidates: CandidateDirective
    queue: QueueDirective
    verification: VerificationDirective
    attention: AttentionClass
    allow_new_implementation_lanes: bool
    reason: str

    def as_dict(self) -> dict[str, object]:
        data = asdict(self)
        return {key: str(value) if isinstance(value, StrEnum) else value for key, value in data.items()}


@dataclass(frozen=True)
class PhaseAssessment:
    schema_version: int
    authority: str
    raw_phase: Phase
    phase: Phase
    signals: PhaseSignals
    recommendation: PhaseRecommendation

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "raw_phase": self.raw_phase,
            "phase": self.phase,
            "signals": self.signals.as_dict(),
            "recommendation": self.recommendation.as_dict(),
        }


def normalized_entropy(weights: Iterable[float]) -> float:
    """Return normalized Shannon entropy for non-negative weights."""

    values = list(weights)
    if not values:
        return 0.0
    if any((not math.isfinite(value) or value < 0.0) for value in values):
        raise ValueError("entropy weights must be finite and non-negative")
    total = sum(values)
    if total <= 0.0:
        return 0.0
    probabilities = [value / total for value in values if value > 0.0]
    if len(probabilities) <= 1:
        return 0.0
    entropy = -sum(probability * math.log(probability) for probability in probabilities)
    return entropy / math.log(len(probabilities))


def branching_ratio(*, children_or_retries: int, terminal_parents: int) -> float:
    if children_or_retries < 0 or terminal_parents < 0:
        raise ValueError("branching counts must be non-negative")
    if terminal_parents == 0:
        return 0.0 if children_or_retries == 0 else 4.0
    return min(4.0, children_or_retries / terminal_parents)


def _signals(observation: PhaseObservation) -> PhaseSignals:
    observation.validate()
    jam_pressure = min(
        1.0,
        0.40 * observation.queue_pressure
        + 0.25 * observation.resource_pressure
        + 0.35 * observation.debt_pressure
        + 0.15 * max(observation.queue_acceleration, 0.0),
    )
    order_parameter = min(
        1.0,
        max(
            0.0,
            0.35 * observation.coherence
            + 0.35 * observation.evidence_completeness
            + 0.15 * (1.0 - observation.candidate_entropy)
            + 0.15 * (1.0 - observation.verifier_disagreement),
        ),
    )
    transition_pressure = min(
        1.0,
        0.50 * observation.verifier_disagreement
        + 0.20 * abs(observation.queue_acceleration)
        + 0.20 * min(observation.branching_ratio / 1.5, 1.0)
        + 0.10 * observation.context_pressure,
    )
    return PhaseSignals(
        order_parameter=round(order_parameter, 6),
        jam_pressure=round(jam_pressure, 6),
        transition_pressure=round(transition_pressure, 6),
        branching_supercritical=observation.branching_ratio >= 1.0,
    )


def _raw_phase(observation: PhaseObservation, signals: PhaseSignals) -> Phase:
    if signals.jam_pressure >= 0.72 or (
        observation.branching_ratio >= 1.25 and observation.queue_pressure >= 0.65
    ):
        return "jammed"
    if (
        signals.order_parameter >= 0.85
        and observation.evidence_completeness >= 0.90
        and observation.coherence >= 0.85
        and observation.candidate_entropy <= 0.25
        and observation.verifier_disagreement <= 0.15
        and signals.jam_pressure <= 0.25
        and observation.branching_ratio < 1.0
    ):
        return "crystal"
    if (
        observation.candidate_entropy <= 0.40
        and observation.mobility <= 0.25
        and observation.evidence_completeness < 0.85
        and observation.coherence < 0.85
    ):
        return "glass"
    if (
        observation.candidate_entropy >= 0.70
        and observation.coherence <= 0.40
        and observation.evidence_completeness < 0.75
    ):
        return "gas"
    if signals.transition_pressure >= 0.55 or observation.branching_ratio >= 1.0:
        return "critical"
    return "liquid"


def _with_hysteresis(
    raw: Phase,
    previous_phase: Phase | None,
    observation: PhaseObservation,
    signals: PhaseSignals,
) -> Phase:
    if previous_phase is None or previous_phase == raw:
        return raw
    if raw in {"crystal", "jammed"}:
        return raw

    if previous_phase == "crystal" and (
        signals.order_parameter >= 0.78
        and observation.evidence_completeness >= 0.82
        and observation.coherence >= 0.78
        and observation.candidate_entropy <= 0.35
        and signals.jam_pressure < 0.45
        and observation.branching_ratio < 1.0
    ):
        return "crystal"
    if previous_phase == "jammed" and signals.jam_pressure >= 0.50:
        return "jammed"
    if previous_phase == "glass" and (
        observation.mobility <= 0.35
        and observation.evidence_completeness < 0.90
        and observation.coherence < 0.90
        and signals.jam_pressure < 0.70
    ):
        return "glass"
    if previous_phase == "gas" and raw == "liquid" and (
        observation.candidate_entropy >= 0.58
        and observation.coherence <= 0.50
        and signals.jam_pressure < 0.55
    ):
        return "gas"
    if previous_phase == "critical" and raw == "liquid" and signals.transition_pressure >= 0.40:
        return "critical"
    if previous_phase == "liquid":
        if raw == "gas" and observation.candidate_entropy < 0.82:
            return "liquid"
        if raw == "critical" and signals.transition_pressure < 0.68 and observation.branching_ratio < 1.0:
            return "liquid"
    return raw


def _mode(phase: Phase, observation: PhaseObservation, signals: PhaseSignals) -> ControlMode:
    if phase == "jammed":
        return ControlMode.DRAIN
    if phase == "glass":
        return ControlMode.PERTURB
    if phase == "crystal":
        return ControlMode.VERIFY
    if phase == "gas":
        return ControlMode.DIVERGE
    if (
        signals.order_parameter >= 0.65
        and observation.evidence_completeness >= 0.70
        and observation.candidate_entropy <= 0.50
        and observation.verifier_disagreement <= 0.35
        and signals.jam_pressure < 0.45
        and observation.branching_ratio < 1.0
    ):
        return ControlMode.ANNEAL
    if phase == "critical":
        return ControlMode.MEASURE
    return ControlMode.COORDINATE


def _recommend(mode: ControlMode, observation: PhaseObservation) -> PhaseRecommendation:
    table: dict[ControlMode, PhaseRecommendation] = {
        ControlMode.DIVERGE: PhaseRecommendation(
            mode, SpawnDirective.INCREASE, TrajectoryMode.ISOLATED, ContextDirective.FRESH,
            CandidateDirective.EXPAND, QueueDirective.ADMIT, VerificationDirective.NORMAL,
            AttentionClass.ROUTINE, True,
            "High diversity and weak coherence: widen independent search while keeping trajectories decorrelated.",
        ),
        ControlMode.COORDINATE: PhaseRecommendation(
            mode, SpawnDirective.HOLD, TrajectoryMode.SPECIALIST,
            ContextDirective.COMPACT if observation.context_pressure >= 0.85 else ContextDirective.RETAIN,
            CandidateDirective.COORDINATE, QueueDirective.ADMIT, VerificationDirective.NORMAL,
            AttentionClass.ROUTINE, True,
            "Productive liquid regime: keep specialist lanes mobile without adding another scheduler.",
        ),
        ControlMode.MEASURE: PhaseRecommendation(
            mode, SpawnDirective.STOP, TrajectoryMode.SPECIALIST,
            ContextDirective.COMPACT if observation.context_pressure >= 0.80 else ContextDirective.RETAIN,
            CandidateDirective.FREEZE, QueueDirective.HOLD, VerificationDirective.INCREASE,
            AttentionClass.EXCEPTION, False,
            "Near a transition: stop widening implementation space and spend budget on independent measurement.",
        ),
        ControlMode.ANNEAL: PhaseRecommendation(
            mode, SpawnDirective.DECREASE, TrajectoryMode.FORKED,
            ContextDirective.COMPACT if observation.context_pressure >= 0.80 else ContextDirective.RETAIN,
            CandidateDirective.PRUNE, QueueDirective.HOLD, VerificationDirective.INCREASE,
            AttentionClass.EXCEPTION, False,
            "Evidence and order are rising: reduce candidate count while increasing verifier independence.",
        ),
        ControlMode.VERIFY: PhaseRecommendation(
            mode, SpawnDirective.STOP, TrajectoryMode.NONE, ContextDirective.COMPACT,
            CandidateDirective.VERIFY, QueueDirective.HOLD, VerificationDirective.MAXIMUM,
            AttentionClass.AUTHORITY_BOUNDARY, False,
            "A low-entropy candidate exists: create no new implementation lanes; verify exact bytes before authority.",
        ),
        ControlMode.PERTURB: PhaseRecommendation(
            mode, SpawnDirective.LIMITED, TrajectoryMode.ISOLATED, ContextDirective.FRESH,
            CandidateDirective.RESET, QueueDirective.HOLD, VerificationDirective.INCREASE,
            AttentionClass.EXCEPTION, True,
            "Low mobility without sufficient evidence indicates a glassy local minimum: inject a bounded fresh trajectory.",
        ),
        ControlMode.DRAIN: PhaseRecommendation(
            mode, SpawnDirective.STOP, TrajectoryMode.NONE, ContextDirective.COMPACT,
            CandidateDirective.HOLD, QueueDirective.DRAIN, VerificationDirective.MAXIMUM,
            AttentionClass.EXCEPTION, False,
            "Queue/resource/debt pressure dominates: stop creating work, drain debt, reclaim resources, and recover.",
        ),
    }
    return table[mode]


def assess(observation: PhaseObservation, *, previous_phase: Phase | None = None) -> PhaseAssessment:
    """Classify a snapshot and return a search-only recommendation."""

    signals = _signals(observation)
    raw = _raw_phase(observation, signals)
    phase = _with_hysteresis(raw, previous_phase, observation, signals)
    mode = _mode(phase, observation, signals)
    return PhaseAssessment(
        schema_version=PHASE_CONTROL_SCHEMA_VERSION,
        authority=PHASE_CONTROL_AUTHORITY,
        raw_phase=raw,
        phase=phase,
        signals=signals,
        recommendation=_recommend(mode, observation),
    )
