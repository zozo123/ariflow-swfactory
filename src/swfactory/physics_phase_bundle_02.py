"""Phase240 bundle 02: P05-P08, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "Phase240",
    "phase-02",
    (
        WaveDomain(
            "P05",
            "multiphase-boundaries",
            "security",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.DISCRETE_QUANTUM),
        ),
        WaveDomain(
            "P06",
            "phase-fraction-admission",
            "authority",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.BOLTZMANN_GIBBS),
        ),
        WaveDomain(
            "P07",
            "nucleation-branch-seeding",
            "workgraph",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.BOLTZMANN_GIBBS),
        ),
        WaveDomain(
            "P08",
            "boiling-retry-transition",
            "recovery",
            (PhysicsModel.REACTION_CRITICALITY, PhysicsModel.FLUID),
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
