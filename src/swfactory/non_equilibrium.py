"""Physics-informed control diagnostics for the software factory.

This module is intentionally scheduler-free. Apache Airflow remains the only lifecycle scheduler.
The physics is used where the mathematics transfers cleanly: stochastic currents, maximum-entropy
allocation, entropy production, metastability, hysteresis, barrier crossing, fluctuation relations,
and interacting model ensembles. Temperatures, free energies, and phases here are effective control
variables, not claims about literal thermodynamic matter.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum


_EPS = 1e-12


class Phase(StrEnum):
    GAS = "gas"
    LIQUID = "liquid"
    CRITICAL = "critical"
    CRYSTAL = "crystal"
    GLASS = "glass"
    JAMMED = "jammed"


class ControlAction(StrEnum):
    EXPLORE = "explore"
    PARALLELIZE = "parallelize"
    STABILIZE = "stabilize"
    VERIFY = "verify"
    RECOVER = "recover"
    CLEANUP = "cleanup"
    THROTTLE = "throttle"
    PROMOTE = "promote"


@dataclass(frozen=True)
class Current:
    """One observable current J and its conjugate control affinity X."""

    name: str
    value: float
    previous_value: float = 0.0
    dt: float = 1.0
    affinity: float = 0.0

    @property
    def acceleration(self) -> float:
        if self.dt <= 0:
            raise ValueError("current dt must be positive")
        return (self.value - self.previous_value) / self.dt

    @property
    def entropy_production(self) -> float:
        return self.value * self.affinity


@dataclass(frozen=True)
class FactoryState:
    """Minimal state needed by independent control models.

    Fractions are expected in [0, 1]. `configurational_entropy` is normalized to [0, 1].
    `effective_temperature` is a dimensionless exploration/noise scale, not kelvin.
    """

    currents: tuple[Current, ...]
    configurational_entropy: float
    failure_fraction: float
    blocked_fraction: float
    cleanup_debt: float
    evidence_gap: float
    security_refusal_fraction: float
    cost_pressure: float
    effective_temperature: float = 1.0

    def validate(self) -> None:
        for name, value in (
            ("configurational_entropy", self.configurational_entropy),
            ("failure_fraction", self.failure_fraction),
            ("blocked_fraction", self.blocked_fraction),
            ("evidence_gap", self.evidence_gap),
            ("security_refusal_fraction", self.security_refusal_fraction),
            ("cost_pressure", self.cost_pressure),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between zero and one")
        if self.cleanup_debt < 0:
            raise ValueError("cleanup_debt cannot be negative")
        if self.effective_temperature <= 0:
            raise ValueError("effective_temperature must be positive")
        names = [current.name for current in self.currents]
        if len(names) != len(set(names)):
            raise ValueError("current names must be unique")

    @property
    def total_flux(self) -> float:
        return sum(abs(current.value) for current in self.currents)

    @property
    def total_acceleration(self) -> float:
        return sum(abs(current.acceleration) for current in self.currents)

    @property
    def entropy_production(self) -> float:
        return sum(current.entropy_production for current in self.currents)


@dataclass(frozen=True)
class PhaseDecision:
    phase: Phase
    confidence: float
    order_parameter: float
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class Pitch:
    """One independent model's recommendation.

    Scores are utilities in [-1, 1]. Reliability is evidence-derived and belongs in [0, 1].
    """

    model: str
    scores: tuple[tuple[ControlAction, float], ...]
    reliability: float = 1.0

    def validate(self) -> None:
        if not self.model.strip():
            raise ValueError("pitch model name is required")
        if not 0.0 <= self.reliability <= 1.0:
            raise ValueError("pitch reliability must be between zero and one")
        actions = [action for action, _ in self.scores]
        if len(actions) != len(set(actions)):
            raise ValueError(f"pitch {self.model!r} repeats an action")
        for _, score in self.scores:
            if not -1.0 <= score <= 1.0:
                raise ValueError("pitch scores must be between -1 and one")

    def utility(self, action: ControlAction) -> float:
        return next((score for candidate, score in self.scores if candidate is action), 0.0)


@dataclass(frozen=True)
class Coupling:
    """Pairwise model coupling, analogous to activation/inhibition in regulatory networks."""

    source: str
    target: str
    strength: float

    def validate(self) -> None:
        if self.source == self.target:
            raise ValueError("self-coupling is not allowed")
        if not -1.0 <= self.strength <= 1.0:
            raise ValueError("coupling strength must be between -1 and one")


@dataclass(frozen=True)
class EnsembleDecision:
    phase: PhaseDecision
    action_probabilities: tuple[tuple[ControlAction, float], ...]
    model_weights: tuple[tuple[str, float], ...]
    disagreement_entropy: float
    entropy_production: float

    @property
    def action(self) -> ControlAction:
        return max(self.action_probabilities, key=lambda item: item[1])[0]


def shannon_entropy(probabilities: Sequence[float], *, normalize: bool = False) -> float:
    values = [max(0.0, float(value)) for value in probabilities]
    total = sum(values)
    if total <= _EPS:
        return 0.0
    normalized = [value / total for value in values if value > _EPS]
    entropy = -sum(value * math.log(value) for value in normalized)
    if normalize and len(values) > 1:
        entropy /= math.log(len(values))
    return entropy


def max_entropy_weights(costs: Mapping[str, float], *, beta: float) -> dict[str, float]:
    """Gibbs distribution: max entropy subject to an expected-cost constraint."""
    if beta < 0:
        raise ValueError("beta must be non-negative")
    if not costs:
        return {}
    minimum = min(float(value) for value in costs.values())
    raw = {name: math.exp(-beta * (float(cost) - minimum)) for name, cost in costs.items()}
    normalizer = sum(raw.values())
    return {name: value / normalizer for name, value in raw.items()}


def classify_phase(state: FactoryState, *, previous: Phase | None = None) -> PhaseDecision:
    """Classify the control regime with a small hysteresis band.

    The order parameter favors low defect/blockage/evidence debt and low configurational entropy.
    A mature, low-entropy state is crystal-like; a high-entropy healthy state is liquid-like;
    persistent low-mobility disorder is glass/jam-like.
    """
    state.validate()
    disorder = (
        state.failure_fraction
        + state.blocked_fraction
        + state.evidence_gap
        + state.security_refusal_fraction
    ) / 4.0
    order = max(0.0, min(1.0, 1.0 - 0.55 * disorder - 0.45 * state.configurational_entropy))
    mobility = min(1.0, state.total_flux / max(1.0, len(state.currents)))
    acceleration = min(1.0, state.total_acceleration / max(1.0, len(state.currents)))
    debt = min(1.0, state.cleanup_debt / 10.0)

    reasons: list[str] = []
    if state.blocked_fraction >= 0.6 or debt >= 0.8:
        phase = Phase.JAMMED
        reasons.append("blocked/debt pressure dominates mobility")
    elif mobility < 0.15 and disorder >= 0.35:
        phase = Phase.GLASS
        reasons.append("low mobility with persistent disorder is metastable")
    elif order >= 0.78 and state.configurational_entropy <= 0.25 and disorder <= 0.15:
        phase = Phase.CRYSTAL
        reasons.append("low-entropy ordered state with low defect pressure")
    elif acceleration >= 0.45 and 0.3 <= state.configurational_entropy <= 0.75:
        phase = Phase.CRITICAL
        reasons.append("large acceleration near an intermediate-entropy regime")
    elif state.configurational_entropy >= 0.78 and disorder <= 0.35:
        phase = Phase.GAS
        reasons.append("very high dispersion with weak structural coupling")
    else:
        phase = Phase.LIQUID
        reasons.append("mobile adaptive regime without a dominant failure mode")

    if (
        previous in {Phase.LIQUID, Phase.CRYSTAL}
        and phase in {Phase.LIQUID, Phase.CRYSTAL}
        and previous is not phase
    ):
        boundary_distance = abs(order - 0.65)
        if boundary_distance < 0.08:
            phase = previous
            reasons.append("hysteresis retained the previous stable phase")

    confidence = min(1.0, 0.5 + abs(order - 0.5) + 0.25 * max(disorder, acceleration, debt))
    return PhaseDecision(phase, confidence, order, tuple(reasons))


def entropy_production(currents: Sequence[Current]) -> float:
    """Bilinear non-equilibrium proxy sum_i J_i X_i."""
    return sum(current.entropy_production for current in currents)


def classical_nucleation_barrier(*, surface_penalty: float, driving_force: float) -> float:
    """Dimensionless barrier proxy proportional to gamma^3 / Delta^2.

    Useful for deciding when a candidate architecture has enough sustained driving force to justify
    paying a one-time stabilization/promotion barrier.
    """
    if surface_penalty < 0 or driving_force <= 0:
        raise ValueError("surface_penalty must be non-negative and driving_force positive")
    return (surface_penalty**3) / max(driving_force**2, _EPS)


def barrier_crossing_probability(*, barrier: float, effective_temperature: float) -> float:
    """Kramers/Arrhenius-style rare-event probability proxy exp(-barrier / T_eff)."""
    if barrier < 0 or effective_temperature <= 0:
        raise ValueError("barrier must be non-negative and effective_temperature positive")
    return math.exp(-barrier / effective_temperature)


def jarzynski_delta_free_energy(work_samples: Sequence[float], *, beta: float) -> float:
    """Estimate effective Delta F from repeated transition trajectories.

    This is a diagnostic for comparing repeated controlled transition protocols. It should not be
    interpreted as literal thermodynamic free energy for software.
    """
    if beta <= 0:
        raise ValueError("beta must be positive")
    if not work_samples:
        raise ValueError("at least one work sample is required")
    exponents = [-beta * float(work) for work in work_samples]
    maximum = max(exponents)
    log_mean_exp = maximum + math.log(
        sum(math.exp(value - maximum) for value in exponents) / len(exponents)
    )
    return -log_mean_exp / beta


def crooks_log_ratio(*, work: float, delta_free_energy: float, beta: float) -> float:
    """Crooks fluctuation-relation log forward/reverse ratio proxy."""
    if beta <= 0:
        raise ValueError("beta must be positive")
    return beta * (work - delta_free_energy)


def canonical_pitches(state: FactoryState) -> tuple[Pitch, ...]:
    """Independent control models run conceptually in parallel over the same state."""
    state.validate()
    flux = min(1.0, state.total_flux / max(1.0, len(state.currents)))
    acceleration = min(1.0, state.total_acceleration / max(1.0, len(state.currents)))
    entropy = state.configurational_entropy
    disorder = (state.failure_fraction + state.blocked_fraction) / 2.0
    debt = min(1.0, state.cleanup_debt / 10.0)

    return (
        Pitch(
            "flux",
            (
                (ControlAction.PARALLELIZE, 2.0 * (1.0 - flux) - 1.0),
                (ControlAction.THROTTLE, 2.0 * max(disorder, acceleration) - 1.0),
            ),
        ),
        Pitch(
            "entropy",
            (
                (ControlAction.EXPLORE, 2.0 * (1.0 - entropy) - 1.0),
                (ControlAction.STABILIZE, 2.0 * entropy - 1.0),
            ),
        ),
        Pitch(
            "stability",
            (
                (ControlAction.STABILIZE, 2.0 * disorder - 1.0),
                (ControlAction.PROMOTE, 1.0 - 2.0 * disorder),
            ),
        ),
        Pitch(
            "recovery",
            (
                (
                    ControlAction.RECOVER,
                    2.0 * max(state.failure_fraction, state.blocked_fraction) - 1.0,
                ),
                (ControlAction.CLEANUP, 2.0 * debt - 1.0),
            ),
        ),
        Pitch(
            "evidence",
            (
                (ControlAction.VERIFY, 2.0 * state.evidence_gap - 1.0),
                (ControlAction.PROMOTE, 1.0 - 2.0 * state.evidence_gap),
            ),
        ),
        Pitch(
            "security",
            (
                (ControlAction.VERIFY, 2.0 * state.security_refusal_fraction - 1.0),
                (
                    ControlAction.THROTTLE,
                    2.0 * state.security_refusal_fraction - 1.0,
                ),
            ),
        ),
        Pitch(
            "cost",
            (
                (ControlAction.THROTTLE, 2.0 * state.cost_pressure - 1.0),
                (ControlAction.PARALLELIZE, 1.0 - 2.0 * state.cost_pressure),
            ),
        ),
        Pitch(
            "rare-event",
            (
                (ControlAction.RECOVER, 2.0 * acceleration * disorder - 1.0),
                (ControlAction.VERIFY, 2.0 * acceleration - 1.0),
            ),
        ),
    )


def default_couplings() -> tuple[Coupling, ...]:
    """Protein-network-like activation/inhibition between independent control models."""
    return (
        Coupling("evidence", "stability", 0.35),
        Coupling("recovery", "flux", -0.45),
        Coupling("security", "flux", -0.35),
        Coupling("cost", "flux", -0.25),
        Coupling("entropy", "stability", -0.20),
        Coupling("rare-event", "recovery", 0.40),
        Coupling("stability", "entropy", -0.15),
    )


def mix_pitches(
    pitches: Sequence[Pitch],
    *,
    phase: PhaseDecision,
    couplings: Sequence[Coupling] = (),
    effective_temperature: float = 1.0,
    minimum_model_weight: float = 0.02,
) -> EnsembleDecision:
    """Maximum-entropy mixture with pairwise activation/inhibition and no single master model."""
    if effective_temperature <= 0:
        raise ValueError("effective_temperature must be positive")
    if minimum_model_weight < 0:
        raise ValueError("minimum_model_weight cannot be negative")
    if not pitches:
        raise ValueError("at least one pitch is required")
    for pitch in pitches:
        pitch.validate()
    for coupling in couplings:
        coupling.validate()

    by_name = {pitch.model: pitch for pitch in pitches}
    if len(by_name) != len(pitches):
        raise ValueError("pitch model names must be unique")

    costs = {pitch.model: 1.0 - pitch.reliability for pitch in pitches}
    beta = 1.0 / effective_temperature
    weights = max_entropy_weights(costs, beta=beta)

    activity = dict(weights)
    for coupling in couplings:
        if coupling.source not in by_name or coupling.target not in by_name:
            continue
        activity[coupling.target] *= math.exp(
            coupling.strength * weights[coupling.source]
        )

    floor = minimum_model_weight / len(pitches)
    normalizer = sum(max(floor, value) for value in activity.values())
    model_weights = {
        name: max(floor, value) / normalizer for name, value in activity.items()
    }

    action_utilities = {action: 0.0 for action in ControlAction}
    for pitch in pitches:
        weight = model_weights[pitch.model]
        for action in ControlAction:
            action_utilities[action] += weight * pitch.utility(action)

    if phase.phase is Phase.CRYSTAL:
        action_utilities[ControlAction.VERIFY] += 0.15
        action_utilities[ControlAction.PROMOTE] += 0.10
    elif phase.phase in {Phase.GLASS, Phase.JAMMED}:
        action_utilities[ControlAction.RECOVER] += 0.20
        action_utilities[ControlAction.CLEANUP] += 0.15
    elif phase.phase is Phase.CRITICAL:
        action_utilities[ControlAction.VERIFY] += 0.20
        action_utilities[ControlAction.THROTTLE] += 0.10
    elif phase.phase is Phase.GAS:
        action_utilities[ControlAction.STABILIZE] += 0.15

    maximum = max(action_utilities.values())
    raw = {
        action: math.exp((utility - maximum) / effective_temperature)
        for action, utility in action_utilities.items()
    }
    action_normalizer = sum(raw.values())
    probabilities = {
        action: value / action_normalizer for action, value in raw.items()
    }

    disagreement = shannon_entropy(list(probabilities.values()), normalize=True)
    return EnsembleDecision(
        phase=phase,
        action_probabilities=tuple(
            sorted(probabilities.items(), key=lambda item: item[0].value)
        ),
        model_weights=tuple(sorted(model_weights.items())),
        disagreement_entropy=disagreement,
        entropy_production=0.0,
    )


def evaluate_factory(
    state: FactoryState,
    *,
    previous_phase: Phase | None = None,
    couplings: Sequence[Coupling] | None = None,
) -> EnsembleDecision:
    """Run all canonical models and mix them into one bounded control recommendation."""
    phase = classify_phase(state, previous=previous_phase)
    mixed = mix_pitches(
        canonical_pitches(state),
        phase=phase,
        couplings=default_couplings() if couplings is None else couplings,
        effective_temperature=state.effective_temperature,
    )
    return EnsembleDecision(
        phase=mixed.phase,
        action_probabilities=mixed.action_probabilities,
        model_weights=mixed.model_weights,
        disagreement_entropy=mixed.disagreement_entropy,
        entropy_production=state.entropy_production,
    )
