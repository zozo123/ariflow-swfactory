"""Shared engine for Phase240 high-entropy development currents.

This module is intentionally side-effect free. It classifies pressure and phase; Apache Airflow
remains the sole lifecycle scheduler and all mutation authority remains Cell/epoch scoped.
"""

from dataclasses import dataclass
from enum import StrEnum

from swfactory.phase_transition import DevelopmentPhase


class FluidAction(StrEnum):
    EXPAND = "expand"
    MIX = "mix"
    THROTTLE = "throttle"
    FENCE = "fence"
    DISSIPATE = "dissipate"
    HOLD = "hold"
    REFUSE = "refuse"


@dataclass(frozen=True, slots=True)
class FluidDomain:
    domain_id: str
    slug: str
    owner: str
    invariant: str


@dataclass(frozen=True, slots=True)
class FluidSignal:
    cell_id: str
    epoch: int
    entropy: float
    pressure: float
    saturation: float
    retry_rate: float = 0.0
    authority_count: int = 1
    scheduler: str = "airflow"

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_") or self.epoch < 1:
            raise ValueError("canonical Cell identity and positive epoch required")
        if min(self.entropy, self.pressure, self.saturation, self.retry_rate) < 0:
            raise ValueError("fluid signals cannot be negative")
        if self.authority_count < 1:
            raise ValueError("at least one authority must exist")


@dataclass(frozen=True, slots=True)
class FluidDecision:
    phase: DevelopmentPhase
    action: FluidAction
    reason: str


def evaluate(signal: FluidSignal, *, crystallize: bool = False) -> FluidDecision:
    signal.validate()
    if signal.scheduler != "airflow":
        return FluidDecision(DevelopmentPhase.MIXED, FluidAction.REFUSE, "Airflow is the sole lifecycle scheduler")
    if signal.authority_count > 1:
        return FluidDecision(DevelopmentPhase.MIXED, FluidAction.FENCE, "duplicate authorities must remain explicit")
    if crystallize:
        if signal.entropy or signal.retry_rate or signal.pressure > 0.5:
            return FluidDecision(DevelopmentPhase.LIQUID, FluidAction.HOLD, "crystallization invariants are not satisfied")
        return FluidDecision(DevelopmentPhase.SOLID, FluidAction.DISSIPATE, "candidate may enter final solidification gate")
    if signal.entropy >= 8 or signal.saturation >= 0.95:
        return FluidDecision(DevelopmentPhase.SUPERCRITICAL, FluidAction.THROTTLE, "maximum productive stress regime")
    if signal.pressure >= 1 or signal.retry_rate >= 0.25:
        return FluidDecision(DevelopmentPhase.MIXED, FluidAction.MIX, "pressure or retries require explicit mixed-phase handling")
    if signal.entropy > 0:
        return FluidDecision(DevelopmentPhase.LIQUID, FluidAction.EXPAND, "useful entropy remains available for recombination")
    return FluidDecision(DevelopmentPhase.GAS, FluidAction.EXPAND, "exploration remains unconstrained")
