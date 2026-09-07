"""StatMech360 bundle 04: Q13-Q16, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-04",
    (
        WaveDomain("Q13", "order-parameter-authority", "authority", (PhysicsModel.PHASE_FIELD,)),
        WaveDomain("Q14", "symmetry-breaking-selection", "authority", (PhysicsModel.PHASE_FIELD,)),
        WaveDomain(
            "Q15",
            "spontaneous-branch-dominance",
            "evidence",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.PHASE_FIELD),
        ),
        WaveDomain(
            "Q16",
            "critical-point-detection",
            "evidence",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.NONEQUILIBRIUM),
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
