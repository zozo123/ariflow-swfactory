"""Maximum-Caliber path ensembles for non-equilibrium factory trajectories.

MaxEnt chooses a least-committal distribution over states. Maximum Caliber applies the same
principle to complete trajectories. This module is advisory and side-effect free; Apache Airflow
remains the only lifecycle scheduler.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import exp, log


@dataclass(frozen=True, slots=True)
class PathStep:
    state_id: str
    work: float = 0.0
    entropy_production: float = 0.0
    action_increment: float = 0.0

    def validate(self) -> None:
        if not self.state_id:
            raise ValueError("state_id required")
        if self.entropy_production < 0:
            raise ValueError("entropy production cannot be negative")


@dataclass(frozen=True, slots=True)
class Trajectory:
    trajectory_id: str
    steps: tuple[PathStep, ...]

    def validate(self) -> None:
        if not self.trajectory_id or not self.steps:
            raise ValueError("non-empty trajectory required")
        for step in self.steps:
            step.validate()

    @property
    def work(self) -> float:
        return sum(step.work for step in self.steps)

    @property
    def entropy_production(self) -> float:
        return sum(step.entropy_production for step in self.steps)

    @property
    def action(self) -> float:
        return sum(step.action_increment for step in self.steps)


@dataclass(frozen=True, slots=True)
class CaliberResult:
    probabilities: tuple[tuple[str, float], ...]
    caliber: float
    mean_work: float
    mean_entropy_production: float
    dominant_trajectory: str | None


def maximum_caliber(
    trajectories: tuple[Trajectory, ...],
    *,
    beta_work: float = 1.0,
    lambda_entropy: float = 1.0,
    lambda_action: float = 1.0,
) -> CaliberResult:
    """Return a Gibbs-like path ensemble over whole trajectories.

    The multipliers are explicit Lagrange multipliers for observed path constraints. Nothing in
    this function authorizes a lifecycle transition or external mutation.
    """

    if min(beta_work, lambda_entropy, lambda_action) < 0:
        raise ValueError("Maximum-Caliber multipliers cannot be negative")
    if not trajectories:
        return CaliberResult((), 0.0, 0.0, 0.0, None)
    for trajectory in trajectories:
        trajectory.validate()

    costs = []
    for trajectory in trajectories:
        cost = (
            beta_work * trajectory.work
            + lambda_entropy * trajectory.entropy_production
            + lambda_action * trajectory.action
        )
        costs.append((trajectory, cost))

    minimum = min(cost for _, cost in costs)
    raw = [(trajectory, exp(-(cost - minimum))) for trajectory, cost in costs]
    partition = sum(weight for _, weight in raw)
    probabilities = tuple((trajectory.trajectory_id, weight / partition) for trajectory, weight in raw)
    by_id = {trajectory.trajectory_id: trajectory for trajectory in trajectories}
    caliber = -sum(probability * log(probability) for _, probability in probabilities if probability > 0)
    mean_work = sum(probability * by_id[trajectory_id].work for trajectory_id, probability in probabilities)
    mean_entropy = sum(
        probability * by_id[trajectory_id].entropy_production
        for trajectory_id, probability in probabilities
    )
    dominant = max(probabilities, key=lambda item: item[1])[0]
    return CaliberResult(probabilities, caliber, mean_work, mean_entropy, dominant)


def path_log_odds(result: CaliberResult, left: str, right: str) -> float:
    """Compare two trajectory probabilities without discarding the rest of the ensemble."""

    probabilities = dict(result.probabilities)
    if left not in probabilities or right not in probabilities:
        raise KeyError("both trajectories must exist in the ensemble")
    if probabilities[left] <= 0 or probabilities[right] <= 0:
        raise ValueError("path probabilities must be positive")
    return log(probabilities[left] / probabilities[right])
