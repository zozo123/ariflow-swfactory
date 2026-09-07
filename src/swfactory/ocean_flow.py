"""Fluid-inspired control primitives for the software-factory Ocean wave.

The vocabulary is intentionally metaphorical; the invariants are not. Apache Airflow
remains the only lifecycle scheduler, Factory Cell epoch fencing remains authoritative,
and every decision emitted here is advisory input to Airflow-owned orchestration.
"""

from dataclasses import dataclass
from enum import StrEnum


class FlowRegime(StrEnum):
    LAMINAR = "laminar"
    PRESSURIZED = "pressurized"
    TURBULENT = "turbulent"
    CAVITATING = "cavitating"
    CHOKED = "choked"


class FlowAction(StrEnum):
    ADMIT = "admit"
    REBALANCE = "rebalance"
    THROTTLE = "throttle"
    FENCE = "fence"
    HOLD = "hold"
    SHED = "shed"
    REFUSE = "refuse"


@dataclass(frozen=True, slots=True)
class FlowSample:
    cell_id: str
    epoch: int
    queue_depth: int
    inflight: int
    retry_rate: float = 0.0
    saturation: float = 0.0
    stale_writers: int = 0
    evidence_lag_s: float = 0.0
    scheduler: str = "airflow"

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_"):
            raise ValueError("Ocean flow samples require a canonical cell_ identity")
        if self.epoch < 1:
            raise ValueError("Ocean flow samples require a positive Cell epoch")
        if self.queue_depth < 0 or self.inflight < 0 or self.stale_writers < 0:
            raise ValueError("flow counters cannot be negative")
        if self.retry_rate < 0 or self.saturation < 0 or self.evidence_lag_s < 0:
            raise ValueError("flow rates and lag cannot be negative")


@dataclass(frozen=True, slots=True)
class OceanPolicy:
    soft_queue: int = 32
    hard_queue: int = 128
    retry_cavitation: float = 0.25
    saturation_choke: float = 0.95
    max_evidence_lag_s: float = 30.0

    def validate(self) -> None:
        if self.soft_queue < 1 or self.hard_queue < self.soft_queue:
            raise ValueError("queue thresholds must be positive and ordered")
        if not 0 <= self.retry_cavitation <= 1:
            raise ValueError("retry_cavitation must be in [0, 1]")
        if not 0 <= self.saturation_choke <= 1:
            raise ValueError("saturation_choke must be in [0, 1]")
        if self.max_evidence_lag_s < 0:
            raise ValueError("max_evidence_lag_s cannot be negative")


@dataclass(frozen=True, slots=True)
class FlowDecision:
    regime: FlowRegime
    action: FlowAction
    reason: str
    pressure: float


def decide_flow(sample: FlowSample, policy: OceanPolicy = OceanPolicy()) -> FlowDecision:
    """Return a deterministic, side-effect-free control decision for one Cell sample."""

    sample.validate()
    policy.validate()
    pressure = sample.queue_depth / policy.soft_queue

    if sample.scheduler != "airflow":
        return FlowDecision(FlowRegime.CHOKED, FlowAction.REFUSE, "Airflow is the sole lifecycle scheduler", pressure)
    if sample.stale_writers:
        return FlowDecision(FlowRegime.TURBULENT, FlowAction.FENCE, "stale writers require epoch fencing", pressure)
    if sample.queue_depth >= policy.hard_queue or sample.saturation >= policy.saturation_choke:
        return FlowDecision(FlowRegime.CHOKED, FlowAction.SHED, "hard pressure or saturation boundary exceeded", pressure)
    if sample.retry_rate >= policy.retry_cavitation:
        return FlowDecision(FlowRegime.CAVITATING, FlowAction.HOLD, "retry cavitation detected", pressure)
    if sample.evidence_lag_s > policy.max_evidence_lag_s:
        return FlowDecision(FlowRegime.PRESSURIZED, FlowAction.HOLD, "evidence is lagging mutation flow", pressure)
    if sample.queue_depth >= policy.soft_queue:
        return FlowDecision(FlowRegime.PRESSURIZED, FlowAction.THROTTLE, "soft queue pressure exceeded", pressure)
    if sample.queue_depth and sample.inflight == 0:
        return FlowDecision(FlowRegime.LAMINAR, FlowAction.ADMIT, "queued work has available execution capacity", pressure)
    return FlowDecision(FlowRegime.LAMINAR, FlowAction.REBALANCE, "flow is within the laminar operating envelope", pressure)
