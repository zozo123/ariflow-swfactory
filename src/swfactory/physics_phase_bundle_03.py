"""Phase240 bundle 03: P09-P12, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "Phase240",
    "phase-03",
    (
        WaveDomain(
            "P09",
            "condensation-fanin",
            "authority",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain(
            "P10",
            "supercritical-concurrency",
            "airflow",
            (PhysicsModel.FLUID, PhysicsModel.REACTION_CRITICALITY),
        ),
        WaveDomain(
            "P11",
            "shock-load-shedding",
            "security",
            (PhysicsModel.FLUID, PhysicsModel.REACTION_CRITICALITY),
        ),
        WaveDomain(
            "P12",
            "mach-event-bursts",
            "authority",
            (PhysicsModel.FLUID, PhysicsModel.REACTION_CRITICALITY),
        ),
    ),
)
BUNDLE.validate()


def run(
    domain_id: str,
    concern: Concern,
    sample: PhysicsSample,
    *,
    release_gate_open: bool = False,
    safety_case: SafetyCase | None = None,
) -> WaveDecision:
    return execute_wave(
        BUNDLE,
        domain_id=domain_id,
        concern=concern,
        sample=sample,
        release_gate_open=release_gate_open,
        safety_case=safety_case,
    )
