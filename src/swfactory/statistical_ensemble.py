"""Statistical ensemble models for development pressure and strategy populations.

The model is advisory and deterministic. It does not schedule work or perform mutations.
"""

from dataclasses import dataclass
from math import exp, log


@dataclass(frozen=True, slots=True)
class StrategyState:
    strategy_id: str
    energy: float
    risk: float
    cost: float
    evidence: float

    def validate(self) -> None:
        if not self.strategy_id:
            raise ValueError("strategy_id required")
        if min(self.energy, self.risk, self.cost, self.evidence) < 0:
            raise ValueError("strategy observables cannot be negative")


@dataclass(frozen=True, slots=True)
class EnsembleResult:
    partition: float
    probabilities: tuple[tuple[str, float], ...]
    entropy: float
    dominant_strategy: str | None


def canonical_ensemble(states: tuple[StrategyState, ...], *, beta: float = 1.0) -> EnsembleResult:
    if beta <= 0:
        raise ValueError("beta must be positive")
    if not states:
        return EnsembleResult(0.0, (), 0.0, None)
    for state in states:
        state.validate()
    weights = []
    for state in states:
        effective_energy = state.energy + state.risk + state.cost - state.evidence
        weights.append((state.strategy_id, exp(-beta * effective_energy)))
    partition = sum(weight for _, weight in weights)
    if partition == 0:
        probabilities = tuple((strategy_id, 0.0) for strategy_id, _ in weights)
        return EnsembleResult(0.0, probabilities, 0.0, None)
    probabilities = tuple((strategy_id, weight / partition) for strategy_id, weight in weights)
    entropy = -sum(p * log(p) for _, p in probabilities if p > 0)
    dominant = max(probabilities, key=lambda item: item[1])[0]
    return EnsembleResult(partition, probabilities, entropy, dominant)


def susceptibility(samples: tuple[float, ...]) -> float:
    if not samples:
        return 0.0
    mean = sum(samples) / len(samples)
    return sum((value - mean) ** 2 for value in samples) / len(samples)


def criticality_score(*, susceptibility_value: float, correlation_length: float, authority_overlap: int) -> float:
    if susceptibility_value < 0 or correlation_length < 0 or authority_overlap < 0:
        raise ValueError("criticality inputs cannot be negative")
    return susceptibility_value * (1.0 + correlation_length) + authority_overlap * 10.0
