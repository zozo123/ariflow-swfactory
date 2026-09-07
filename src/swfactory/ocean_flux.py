"""Flux accounting and deterministic regime switching for Ocean120 D07-D08."""

from dataclasses import dataclass, field

from swfactory.ocean_flow import FlowAction, FlowDecision, FlowRegime, FlowSample, OceanPolicy


@dataclass(slots=True)
class FluxLedger:
    work_units: int = 0
    mutation_units: int = 0
    evidence_units: int = 0
    cost_micros: int = 0
    records: list[tuple[str, int]] = field(default_factory=list)

    def record(self, kind: str, units: int) -> None:
        if units < 0:
            raise ValueError("flux units cannot be negative")
        if kind not in {"work", "mutation", "evidence", "cost"}:
            raise ValueError("unknown flux kind")
        setattr(self, f"{kind}_units" if kind != "cost" else "cost_micros", getattr(self, f"{kind}_units" if kind != "cost" else "cost_micros") + units)
        self.records.append((kind, units))

    @property
    def unsealed_mutation_flux(self) -> int:
        return max(0, self.mutation_units - self.evidence_units)


def reynolds_regime(sample: FlowSample, policy: OceanPolicy = OceanPolicy()) -> FlowDecision:
    sample.validate()
    policy.validate()
    pressure = sample.queue_depth / policy.soft_queue
    reynolds = (sample.inflight + 1) * (sample.retry_rate + pressure + sample.saturation)
    if sample.scheduler != "airflow":
        return FlowDecision(FlowRegime.CHOKED, FlowAction.REFUSE, "scheduler authority violation", pressure)
    if sample.stale_writers:
        return FlowDecision(FlowRegime.TURBULENT, FlowAction.FENCE, "stale writer turbulence", pressure)
    if reynolds >= 4 or sample.queue_depth >= policy.hard_queue:
        return FlowDecision(FlowRegime.CHOKED, FlowAction.SHED, "supercritical Reynolds pressure", pressure)
    if reynolds >= 1.5:
        return FlowDecision(FlowRegime.TURBULENT, FlowAction.HOLD, "turbulent Reynolds regime", pressure)
    if reynolds >= 0.75:
        return FlowDecision(FlowRegime.PRESSURIZED, FlowAction.THROTTLE, "transitional Reynolds regime", pressure)
    return FlowDecision(FlowRegime.LAMINAR, FlowAction.ADMIT, "laminar Reynolds regime", pressure)
