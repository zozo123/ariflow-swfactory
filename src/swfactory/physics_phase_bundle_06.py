"""Phase240 bundle 06: P21-P24, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "Phase240",
    "phase-06",
    (
        WaveDomain(
            "P21",
            "turbulent-mixing-merges",
            "workgraph",
            (PhysicsModel.FLUID, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain(
            "P22",
            "laminar-fast-path",
            "operator",
            (PhysicsModel.FLUID, PhysicsModel.BOLTZMANN_GIBBS),
        ),
        WaveDomain(
            "P23",
            "crystallization-readiness",
            "evidence",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "P24",
            "solidification-release-gate",
            "authority",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.DISCRETE_QUANTUM),
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
