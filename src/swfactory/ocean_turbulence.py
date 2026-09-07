"""Turbulence detection and ownership-safe rebalancing for Ocean120 D03-D04."""

from dataclasses import dataclass

from swfactory.ocean_flow import FlowAction, FlowDecision, FlowRegime, FlowSample, OceanPolicy


@dataclass(frozen=True, slots=True)
class TurbulenceWindow:
    retry_delta: float
    cancellation_rate: float
    queue_variance: float
    ownership_conflicts: int = 0

    def energy(self) -> float:
        if min(self.retry_delta, self.cancellation_rate, self.queue_variance) < 0 or self.ownership_conflicts < 0:
            raise ValueError("turbulence signals cannot be negative")
        return self.retry_delta * 2 + self.cancellation_rate + self.queue_variance + self.ownership_conflicts * 4


def turbulence_decision(
    sample: FlowSample,
    window: TurbulenceWindow,
    policy: OceanPolicy = OceanPolicy(),
    threshold: float = 1.5,
) -> FlowDecision:
    sample.validate()
    policy.validate()
    energy = window.energy()
    pressure = sample.queue_depth / policy.soft_queue
    if sample.scheduler != "airflow":
        return FlowDecision(FlowRegime.CHOKED, FlowAction.REFUSE, "rebalancing cannot become a scheduler", pressure)
    if sample.stale_writers or window.ownership_conflicts:
        return FlowDecision(FlowRegime.TURBULENT, FlowAction.FENCE, "ownership conflict requires epoch fencing", pressure)
    if energy >= threshold:
        return FlowDecision(FlowRegime.TURBULENT, FlowAction.HOLD, "turbulent feedback detected", pressure)
    if sample.queue_depth and sample.inflight:
        return FlowDecision(FlowRegime.LAMINAR, FlowAction.REBALANCE, "capacity may rebalance within Cell ownership", pressure)
    return FlowDecision(FlowRegime.LAMINAR, FlowAction.ADMIT, "flow remains stable", pressure)
