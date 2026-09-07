"""Runaway-chain and decay model for recursive factory effects.

This module borrows branching-process/reactor language only for reproductive cascades such as
retry storms, recursive fan-out, event amplification, and stale-state persistence. It is advisory.
"""

from dataclasses import dataclass
from math import exp, log


@dataclass(frozen=True, slots=True)
class CascadeSample:
    parents: int
    children: int
    absorber_fraction: float = 0.0

    def validate(self) -> None:
        if self.parents < 0 or self.children < 0:
            raise ValueError("cascade counts cannot be negative")
        if not 0 <= self.absorber_fraction <= 1:
            raise ValueError("absorber_fraction must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class Criticality:
    k_effective: float
    regime: str
    contained: bool


def effective_multiplication(samples: tuple[CascadeSample, ...]) -> Criticality:
    """Estimate effective reproduction after deliberate absorbers/circuit breakers."""

    if not samples:
        return Criticality(0.0, "subcritical", True)
    effective_children = 0.0
    total_parents = 0
    for sample in samples:
        sample.validate()
        total_parents += sample.parents
        effective_children += sample.children * (1.0 - sample.absorber_fraction)
    k_effective = effective_children / total_parents if total_parents else 0.0
    if k_effective < 0.95:
        return Criticality(k_effective, "subcritical", True)
    if k_effective <= 1.05:
        return Criticality(k_effective, "critical", False)
    return Criticality(k_effective, "supercritical", False)


def surviving_fraction(age: float, *, half_life: float) -> float:
    """Exponential survival for stale branches, leases, cache claims, or transient authority."""

    if age < 0 or half_life <= 0:
        raise ValueError("age must be non-negative and half_life positive")
    return exp(-log(2.0) * age / half_life)


def absorber_needed(k_uncontrolled: float, *, target_k: float = 0.8) -> float:
    """Minimum idealized fraction of reproductions that must be absorbed to reach target k."""

    if k_uncontrolled < 0 or not 0 <= target_k < 1:
        raise ValueError("invalid multiplication inputs")
    if k_uncontrolled == 0 or k_uncontrolled <= target_k:
        return 0.0
    return min(1.0, 1.0 - target_k / k_uncontrolled)


def binding_margin(*, cohesion: float, interface_cost: float, defect_cost: float) -> float:
    """Positive margin indicates a module/release is more cohesive than its interface/defect burden."""

    if min(cohesion, interface_cost, defect_cost) < 0:
        raise ValueError("binding inputs cannot be negative")
    return cohesion - interface_cost - defect_cost
