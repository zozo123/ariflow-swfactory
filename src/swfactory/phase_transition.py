"""Phase-transition control for high-entropy development waves.

Only SOLID is eligible for promotion to main. GAS, LIQUID, MIXED, and
SUPERCRITICAL are deliberately non-rigid development phases.
"""

from dataclasses import dataclass
from enum import StrEnum


class DevelopmentPhase(StrEnum):
    GAS = "gas"
    LIQUID = "liquid"
    MIXED = "mixed"
    SUPERCRITICAL = "supercritical"
    SOLID = "solid"


@dataclass(frozen=True, slots=True)
class PhaseMetrics:
    open_branches: int
    open_prs: int
    unresolved_conflicts: int
    duplicate_authorities: int
    failing_required_checks: int
    open_generated_issues: int
    entropy: float

    def validate(self) -> None:
        counters = (
            self.open_branches,
            self.open_prs,
            self.unresolved_conflicts,
            self.duplicate_authorities,
            self.failing_required_checks,
            self.open_generated_issues,
        )
        if any(value < 0 for value in counters):
            raise ValueError("phase counters cannot be negative")
        if self.entropy < 0:
            raise ValueError("entropy cannot be negative")


@dataclass(frozen=True, slots=True)
class PhaseDecision:
    phase: DevelopmentPhase
    promote_to_main: bool
    reason: str


def classify_phase(metrics: PhaseMetrics, *, crystallize: bool = False) -> PhaseDecision:
    metrics.validate()

    if not crystallize:
        if metrics.entropy >= 8 or metrics.open_branches >= 24:
            return PhaseDecision(DevelopmentPhase.SUPERCRITICAL, False, "fan-out intentionally exceeds liquid regime")
        if metrics.unresolved_conflicts or metrics.duplicate_authorities:
            return PhaseDecision(DevelopmentPhase.MIXED, False, "multiple phases or authorities remain unresolved")
        if metrics.open_prs or metrics.open_generated_issues:
            return PhaseDecision(DevelopmentPhase.LIQUID, False, "work remains fluid and mergeable")
        return PhaseDecision(DevelopmentPhase.GAS, False, "exploration remains unconstrained and non-rigid")

    if metrics.unresolved_conflicts:
        return PhaseDecision(DevelopmentPhase.MIXED, False, "conflicts block crystallization")
    if metrics.duplicate_authorities:
        return PhaseDecision(DevelopmentPhase.MIXED, False, "authority duplication blocks crystallization")
    if metrics.failing_required_checks:
        return PhaseDecision(DevelopmentPhase.LIQUID, False, "required checks block crystallization")
    if metrics.open_generated_issues:
        return PhaseDecision(DevelopmentPhase.LIQUID, False, "generated backlog remains unresolved")
    return PhaseDecision(DevelopmentPhase.SOLID, True, "all crystallization invariants satisfied")
