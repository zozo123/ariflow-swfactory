"""Pressure routing and viscous backpressure for Ocean120 D01-D02."""

from dataclasses import dataclass

from swfactory.ocean_flow import FlowAction, FlowDecision, FlowRegime, FlowSample, OceanPolicy


@dataclass(frozen=True, slots=True)
class PressureState:
    previous_pressure: float = 0.0
    throttled: bool = False


def pressure_gradient(sample: FlowSample, policy: OceanPolicy | None = None) -> float:
    policy = policy or OceanPolicy()
    sample.validate()
    policy.validate()
    return sample.queue_depth / policy.soft_queue + sample.saturation + sample.retry_rate


def viscous_decision(
    sample: FlowSample,
    state: PressureState | None = None,
    policy: OceanPolicy | None = None,
    release_ratio: float = 0.6,
) -> FlowDecision:
    """Apply hysteresis so burst pressure does not oscillate admission state."""

    state = state or PressureState()
    policy = policy or OceanPolicy()
    sample.validate()
    policy.validate()
    if not 0 < release_ratio < 1:
        raise ValueError("release_ratio must be in (0, 1)")
    pressure = pressure_gradient(sample, policy)

    if sample.scheduler != "airflow":
        return FlowDecision(
            FlowRegime.CHOKED,
            FlowAction.REFUSE,
            "non-Airflow lifecycle authority",
            pressure,
        )
    if sample.stale_writers:
        return FlowDecision(
            FlowRegime.TURBULENT,
            FlowAction.FENCE,
            "epoch fence stale writers",
            pressure,
        )

    release = release_ratio
    if state.throttled and pressure > release:
        return FlowDecision(
            FlowRegime.PRESSURIZED,
            FlowAction.THROTTLE,
            "viscous hysteresis holds throttle",
            pressure,
        )
    if pressure >= 1.0:
        return FlowDecision(
            FlowRegime.PRESSURIZED,
            FlowAction.THROTTLE,
            "pressure crossed soft boundary",
            pressure,
        )
    if state.previous_pressure > pressure and pressure <= release:
        return FlowDecision(
            FlowRegime.LAMINAR,
            FlowAction.ADMIT,
            "pressure dissipated below release boundary",
            pressure,
        )
    return FlowDecision(
        FlowRegime.LAMINAR,
        FlowAction.REBALANCE,
        "pressure remains inside laminar envelope",
        pressure,
    )
