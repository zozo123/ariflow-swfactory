"""Phase240 bundle 04: P13-P16, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "Phase240",
    "phase-04",
    (
        WaveDomain(
            "P13",
            "capillary-small-change-flow",
            "operator",
            (PhysicsModel.FLUID, PhysicsModel.DISCRETE_QUANTUM),
        ),
        WaveDomain(
            "P14",
            "surface-tension-contracts",
            "workgraph",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain(
            "P15",
            "marangoni-priority-gradients",
            "airflow",
            (PhysicsModel.FLUID, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "P16",
            "porous-provider-media",
            "security",
            (PhysicsModel.FLUID, PhysicsModel.DISCRETE_QUANTUM),
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
