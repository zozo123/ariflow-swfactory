"""Non-equilibrium stochastic thermodynamics for software-factory trajectories.

The module is advisory and side-effect free. It scores trajectories and irreversible work while
Apache Airflow remains the sole lifecycle scheduler and Factory Cell/epoch remains the mutation
identity boundary.
"""

from dataclasses import dataclass
from math import exp, log


@dataclass(frozen=True, slots=True)
class Current:
    name: str
    flux: float
    velocity: float = 0.0
    acceleration: float = 0.0
    relaxation_time: float = 1.0

    def validate(self) -> None:
        if not self.name:
            raise ValueError("current name required")
        if self.relaxation_time <= 0:
            raise ValueError("relaxation_time must be positive")


@dataclass(frozen=True, slots=True)
class TrajectoryStep:
    state_id: str
    work: float
    heat: float
    entropy_production: float
    currents: tuple[Current, ...] = ()

    def validate(self) -> None:
        if not self.state_id:
            raise ValueError("state_id required")
        if self.entropy_production < 0:
            raise ValueError("entropy production cannot be negative")
        for current in self.currents:
            current.validate()


@dataclass(frozen=True, slots=True)
class Trajectory:
    trajectory_id: str
    steps: tuple[TrajectoryStep, ...]
    action: float = 0.0

    def validate(self) -> None:
        if not self.trajectory_id or not self.steps:
            raise ValueError("non-empty trajectory required")
        for step in self.steps:
            step.validate()

    @property
    def total_work(self) -> float:
        return sum(step.work for step in self.steps)

    @property
    def total_entropy_production(self) -> float:
        return sum(step.entropy_production for step in self.steps)


@dataclass(frozen=True, slots=True)
class PathEnsemble:
    probabilities: tuple[tuple[str, float], ...]
    caliber: float
    mean_work: float
    mean_entropy_production: float


def maximum_caliber(trajectories: tuple[Trajectory, ...], *, beta: float = 1.0) -> PathEnsemble:
    """Maximum-Caliber path ensemble using trajectory action plus dissipative work.

    This is the path-space analogue of MaxEnt: it ranks entire histories instead of snapshots.
    """

    if beta <= 0:
        raise ValueError("beta must be positive")
    if not trajectories:
        return PathEnsemble((), 0.0, 0.0, 0.0)
    for trajectory in trajectories:
        trajectory.validate()

    raw = []
    for trajectory in trajectories:
        effective_action = trajectory.action + trajectory.total_work + trajectory.total_entropy_production
        raw.append((trajectory.trajectory_id, exp(-beta * effective_action)))
    partition = sum(weight for _, weight in raw)
    probabilities = tuple((trajectory_id, weight / partition) for trajectory_id, weight in raw)
    by_id = {trajectory.trajectory_id: trajectory for trajectory in trajectories}
    caliber = -sum(p * log(p) for _, p in probabilities if p > 0)
    mean_work = sum(p * by_id[trajectory_id].total_work for trajectory_id, p in probabilities)
    mean_entropy = sum(p * by_id[trajectory_id].total_entropy_production for trajectory_id, p in probabilities)
    return PathEnsemble(probabilities, caliber, mean_work, mean_entropy)


def jarzynski_free_energy(work_samples: tuple[float, ...], *, beta: float = 1.0) -> float:
    """Estimate free-energy difference from non-equilibrium work: exp(-beta dF)=<exp(-beta W)>."""

    if beta <= 0 or not work_samples:
        raise ValueError("positive beta and at least one work sample required")
    average = sum(exp(-beta * work) for work in work_samples) / len(work_samples)
    if average <= 0:
        raise ValueError("invalid exponential-work average")
    return -log(average) / beta


def crooks_log_ratio(*, forward_work: float, delta_free_energy: float, beta: float = 1.0) -> float:
    """Log forward/reverse trajectory ratio from the Crooks fluctuation relation."""

    if beta <= 0:
        raise ValueError("beta must be positive")
    return beta * (forward_work - delta_free_energy)


def irreversible_bias(forward_flux: float, reverse_flux: float, *, epsilon: float = 1e-12) -> float:
    """Signed log-ratio used to detect broken detailed balance."""

    if forward_flux < 0 or reverse_flux < 0:
        raise ValueError("fluxes cannot be negative")
    return log((forward_flux + epsilon) / (reverse_flux + epsilon))
