"""Parallel physics-inspired observables for bounded software-factory control.

Every model in this module is advisory. Models may measure, rank, refuse, or recommend
control responses, but they never schedule lifecycle work, publish, promote, or mutate
provider state. Apache Airflow remains the sole lifecycle scheduler and Factory Cell
epoch fencing remains the mutation authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from math import exp, isfinite, log

_EPS = 1e-12


class MatterPhase(StrEnum):
    GAS = "gas"
    LIQUID = "liquid"
    MIXED = "mixed"
    SUPERCRITICAL = "supercritical"
    CRYSTAL_CANDIDATE = "crystal_candidate"


class ControlAction(StrEnum):
    ADMIT = "admit"
    REBALANCE = "rebalance"
    THROTTLE = "throttle"
    HOLD = "hold"
    FENCE = "fence"
    SHED = "shed"
    REFUSE = "refuse"
    CRYSTALLIZE_CANDIDATE = "crystallize_candidate"


class PhysicsModel(StrEnum):
    FLUID = "fluid"
    BOLTZMANN_GIBBS = "boltzmann_gibbs"
    NONEQUILIBRIUM = "nonequilibrium"
    PHASE_FIELD = "phase_field"
    REACTION_CRITICALITY = "reaction_criticality"
    DISCRETE_QUANTUM = "discrete_quantum"
    PROTEIN_NETWORK = "protein_network"


@dataclass(frozen=True, slots=True)
class Current:
    """One operational current with its own flux, speed, acceleration, and gradient."""

    name: str
    flux: float
    velocity: float
    acceleration: float = 0.0
    gradient: float = 0.0
    diffusivity: float = 0.0

    def validate(self) -> None:
        if not self.name:
            raise ValueError("current name is required")
        values = (self.flux, self.velocity, self.acceleration, self.gradient, self.diffusivity)
        if not all(isfinite(value) for value in values):
            raise ValueError("current values must be finite")
        if self.diffusivity < 0:
            raise ValueError("diffusivity cannot be negative")


@dataclass(frozen=True, slots=True)
class PhysicsSample:
    cell_id: str
    epoch: int
    currents: tuple[Current, ...]
    work_samples: tuple[float, ...] = ()
    delta_free_energy: float = 0.0
    entropy: float = 0.0
    entropy_ceiling: float = 1.0
    order_parameter: float = 0.0
    retry_branching_ratio: float = 0.0
    nucleation_barrier: float = 1.0
    occupancy: int = 0
    capacity: int = 1
    coherence: float = 0.0
    cooperative_gain: float = 0.0
    stoichiometric_stress: float = 0.0
    degradation_rate: float = 0.0
    condensate_fraction: float = 0.0
    scheduler: str = "airflow"

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_"):
            raise ValueError("physics samples require canonical cell_ identity")
        if self.epoch < 1:
            raise ValueError("physics samples require a positive Cell epoch")
        if self.capacity < 1 or self.occupancy < 0:
            raise ValueError("capacity must be positive and occupancy non-negative")
        if self.entropy_ceiling <= 0:
            raise ValueError("entropy_ceiling must be positive")
        if self.retry_branching_ratio < 0 or self.nucleation_barrier < 0:
            raise ValueError("branching ratio and nucleation barrier cannot be negative")
        bounded = (self.order_parameter, self.coherence, self.condensate_fraction)
        if not all(0.0 <= value <= 1.0 for value in bounded):
            raise ValueError("order_parameter, coherence, and condensate_fraction must be in [0, 1]")
        nonnegative = (
            self.entropy,
            self.cooperative_gain,
            self.stoichiometric_stress,
            self.degradation_rate,
        )
        if any(value < 0 or not isfinite(value) for value in nonnegative):
            raise ValueError("entropy and many-body rates must be finite and non-negative")
        if not isfinite(self.delta_free_energy):
            raise ValueError("delta_free_energy must be finite")
        if any(not isfinite(value) for value in self.work_samples):
            raise ValueError("work_samples must be finite")
        for current in self.currents:
            current.validate()


@dataclass(frozen=True, slots=True)
class ModelSignal:
    model: PhysicsModel
    action: ControlAction
    phase: MatterPhase
    risk: float
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MixtureDecision:
    action: ControlAction
    phase: MatterPhase
    risk: float
    entropy_production: float
    crystallization_score: float
    signals: tuple[ModelSignal, ...]
    reason: str


def jarzynski_delta_free_energy(work_samples: tuple[float, ...], beta: float = 1.0) -> float | None:
    """Estimate ΔF from non-equilibrium work using a stable Jarzynski log-mean-exp."""

    if not work_samples:
        return None
    if beta <= 0 or not isfinite(beta):
        raise ValueError("beta must be finite and positive")
    transformed = tuple(-beta * work for work in work_samples)
    anchor = max(transformed)
    mean_exp = sum(exp(value - anchor) for value in transformed) / len(transformed)
    return -(anchor + log(max(mean_exp, _EPS))) / beta


def crooks_log_ratio(work: float, delta_free_energy: float, beta: float = 1.0) -> float:
    """Return the Crooks forward/reverse log-ratio proxy β(W-ΔF)."""

    if beta <= 0 or not all(isfinite(value) for value in (work, delta_free_energy, beta)):
        raise ValueError("Crooks inputs must be finite and beta positive")
    return beta * (work - delta_free_energy)


def entropy_production(sample: PhysicsSample) -> float:
    """Generalized irreversible entropy-production proxy Σ max(0, J·X)."""

    return sum(max(0.0, current.flux * current.gradient) for current in sample.currents)


def _fluid_signal(sample: PhysicsSample) -> ModelSignal:
    flux = sum(abs(current.flux) for current in sample.currents)
    acceleration = max((abs(current.acceleration) for current in sample.currents), default=0.0)
    occupancy = sample.occupancy / sample.capacity
    risk = min(1.0, 0.25 * flux + 0.25 * acceleration + 0.5 * occupancy)
    if occupancy >= 1.0:
        action = ControlAction.SHED
        phase = MatterPhase.SUPERCRITICAL
    elif risk >= 0.75:
        action = ControlAction.THROTTLE
        phase = MatterPhase.MIXED
    elif risk >= 0.4:
        action = ControlAction.REBALANCE
        phase = MatterPhase.LIQUID
    else:
        action = ControlAction.ADMIT
        phase = MatterPhase.GAS
    return ModelSignal(PhysicsModel.FLUID, action, phase, risk, (f"flux={flux:.6g}", f"accel={acceleration:.6g}"))


def _boltzmann_signal(sample: PhysicsSample) -> ModelSignal:
    entropy_fraction = min(1.0, sample.entropy / sample.entropy_ceiling)
    order = sample.order_parameter
    risk = min(1.0, max(0.0, entropy_fraction - 0.5 * order))
    if entropy_fraction > 0.9 and order < 0.2:
        action = ControlAction.HOLD
        phase = MatterPhase.GAS
    elif order > 0.85 and entropy_fraction < 0.25:
        action = ControlAction.CRYSTALLIZE_CANDIDATE
        phase = MatterPhase.CRYSTAL_CANDIDATE
    else:
        action = ControlAction.REBALANCE
        phase = MatterPhase.LIQUID if entropy_fraction < 0.7 else MatterPhase.MIXED
    evidence = (f"entropy_fraction={entropy_fraction:.6g}", f"order={order:.6g}")
    return ModelSignal(PhysicsModel.BOLTZMANN_GIBBS, action, phase, risk, evidence)


def _nonequilibrium_signal(sample: PhysicsSample) -> ModelSignal:
    sigma = entropy_production(sample)
    estimate = jarzynski_delta_free_energy(sample.work_samples)
    dissipated = 0.0 if estimate is None else max(0.0, sample.delta_free_energy - estimate)
    risk = min(1.0, sigma + dissipated)
    if risk >= 0.8:
        action = ControlAction.HOLD
        phase = MatterPhase.SUPERCRITICAL
    elif risk >= 0.35:
        action = ControlAction.THROTTLE
        phase = MatterPhase.MIXED
    else:
        action = ControlAction.REBALANCE
        phase = MatterPhase.LIQUID
    evidence = (f"entropy_production={sigma:.6g}", f"dissipated_work={dissipated:.6g}")
    return ModelSignal(PhysicsModel.NONEQUILIBRIUM, action, phase, risk, evidence)


def _phase_field_signal(sample: PhysicsSample) -> ModelSignal:
    nucleation = exp(-sample.nucleation_barrier)
    crystal_score = sample.order_parameter * (1.0 - min(1.0, sample.entropy / sample.entropy_ceiling)) * nucleation
    risk = min(1.0, max(0.0, 1.0 - crystal_score))
    if crystal_score >= 0.55:
        action = ControlAction.CRYSTALLIZE_CANDIDATE
        phase = MatterPhase.CRYSTAL_CANDIDATE
    elif sample.order_parameter >= 0.5:
        action = ControlAction.HOLD
        phase = MatterPhase.MIXED
    else:
        action = ControlAction.REBALANCE
        phase = MatterPhase.LIQUID
    evidence = (f"nucleation={nucleation:.6g}", f"crystal_score={crystal_score:.6g}")
    return ModelSignal(PhysicsModel.PHASE_FIELD, action, phase, risk, evidence)


def _reaction_signal(sample: PhysicsSample) -> ModelSignal:
    k_eff = sample.retry_branching_ratio
    if k_eff >= 1.2:
        action = ControlAction.SHED
        phase = MatterPhase.SUPERCRITICAL
        risk = 1.0
    elif k_eff >= 1.0:
        action = ControlAction.HOLD
        phase = MatterPhase.MIXED
        risk = min(1.0, k_eff - 0.2)
    elif k_eff >= 0.8:
        action = ControlAction.THROTTLE
        phase = MatterPhase.LIQUID
        risk = k_eff
    else:
        action = ControlAction.ADMIT
        phase = MatterPhase.GAS
        risk = k_eff
    return ModelSignal(
        PhysicsModel.REACTION_CRITICALITY,
        action,
        phase,
        risk,
        (f"software_branching_ratio={k_eff:.6g}",),
    )


def _discrete_signal(sample: PhysicsSample) -> ModelSignal:
    occupancy = sample.occupancy / sample.capacity
    tunnel_proxy = exp(-sample.nucleation_barrier) * sample.coherence
    if sample.occupancy > sample.capacity:
        action = ControlAction.FENCE
        phase = MatterPhase.SUPERCRITICAL
        risk = 1.0
    elif occupancy >= 1.0:
        action = ControlAction.HOLD
        phase = MatterPhase.MIXED
        risk = occupancy
    else:
        action = ControlAction.REBALANCE
        phase = MatterPhase.LIQUID
        risk = min(1.0, 0.5 * occupancy + 0.5 * tunnel_proxy)
    evidence = (f"occupancy={occupancy:.6g}", f"rare_transition_proxy={tunnel_proxy:.6g}")
    return ModelSignal(PhysicsModel.DISCRETE_QUANTUM, action, phase, risk, evidence)


def _protein_signal(sample: PhysicsSample) -> ModelSignal:
    cooperative_load = sample.cooperative_gain * max(sample.condensate_fraction, _EPS)
    repair_capacity = sample.degradation_rate
    imbalance = max(0.0, sample.stoichiometric_stress + cooperative_load - repair_capacity)
    risk = min(1.0, imbalance)
    if risk >= 0.85:
        action = ControlAction.HOLD
        phase = MatterPhase.MIXED
    elif risk >= 0.45:
        action = ControlAction.THROTTLE
        phase = MatterPhase.LIQUID
    else:
        action = ControlAction.REBALANCE
        phase = MatterPhase.LIQUID
    evidence = (
        f"cooperative_load={cooperative_load:.6g}",
        f"stoichiometric_stress={sample.stoichiometric_stress:.6g}",
        f"repair_capacity={repair_capacity:.6g}",
    )
    return ModelSignal(PhysicsModel.PROTEIN_NETWORK, action, phase, risk, evidence)


_SEVERITY = {
    ControlAction.ADMIT: 0,
    ControlAction.REBALANCE: 1,
    ControlAction.CRYSTALLIZE_CANDIDATE: 1,
    ControlAction.THROTTLE: 2,
    ControlAction.HOLD: 3,
    ControlAction.SHED: 4,
    ControlAction.FENCE: 5,
    ControlAction.REFUSE: 6,
}


def evaluate_mixture(sample: PhysicsSample, *, release_gate_open: bool = False) -> MixtureDecision:
    """Evaluate every model in parallel and deterministically fan signals into one recommendation."""

    sample.validate()
    if sample.scheduler != "airflow":
        refuse = ModelSignal(
            PhysicsModel.FLUID,
            ControlAction.REFUSE,
            MatterPhase.SUPERCRITICAL,
            1.0,
            ("Airflow is the sole lifecycle scheduler",),
        )
        return MixtureDecision(
            ControlAction.REFUSE,
            MatterPhase.SUPERCRITICAL,
            1.0,
            entropy_production(sample),
            0.0,
            (refuse,),
            "non-Airflow lifecycle authority refused",
        )

    signals = (
        _fluid_signal(sample),
        _boltzmann_signal(sample),
        _nonequilibrium_signal(sample),
        _phase_field_signal(sample),
        _reaction_signal(sample),
        _discrete_signal(sample),
        _protein_signal(sample),
    )
    dominant = max(signals, key=lambda signal: (_SEVERITY[signal.action], signal.risk, signal.model.value))
    risk = max(signal.risk for signal in signals)
    crystal_votes = [signal for signal in signals if signal.action == ControlAction.CRYSTALLIZE_CANDIDATE]
    crystallization_score = sum(1.0 - signal.risk for signal in crystal_votes) / max(len(signals), 1)

    action = dominant.action
    phase = dominant.phase
    reason = f"dominant={dominant.model.value}:{dominant.action.value}"
    if crystal_votes and not release_gate_open and _SEVERITY[action] <= _SEVERITY[ControlAction.REBALANCE]:
        action = ControlAction.HOLD
        phase = MatterPhase.CRYSTAL_CANDIDATE
        reason = "crystallization candidate held until explicit release gate"
    elif crystal_votes and release_gate_open and _SEVERITY[action] <= _SEVERITY[ControlAction.REBALANCE]:
        action = ControlAction.CRYSTALLIZE_CANDIDATE
        phase = MatterPhase.CRYSTAL_CANDIDATE
        reason = "explicit release gate permits crystallization candidate"

    return MixtureDecision(
        action,
        phase,
        risk,
        entropy_production(sample),
        crystallization_score,
        signals,
        reason,
    )
