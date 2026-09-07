"""StatMech360 bundle 06: Q21-Q24, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-06",
    (
        WaveDomain(
            "Q21",
            "percolation-dependency",
            "recovery",
            (PhysicsModel.REACTION_CRITICALITY, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain(
            "Q22",
            "spin-glass-conflict",
            "workgraph",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain(
            "Q23",
            "frustration-detection",
            "security",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain(
            "Q24",
            "metastability-branch-hold",
            "authority",
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
