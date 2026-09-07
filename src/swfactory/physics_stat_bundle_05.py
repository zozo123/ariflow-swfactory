"""StatMech360 bundle 05: Q17-Q20, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-05",
    (
        WaveDomain(
            "Q17",
            "susceptibility-alerts",
            "operator",
            (PhysicsModel.NONEQUILIBRIUM, PhysicsModel.PHASE_FIELD),
        ),
        WaveDomain(
            "Q18",
            "correlation-length-propagation",
            "evidence",
            (PhysicsModel.NONEQUILIBRIUM, PhysicsModel.FLUID),
        ),
        WaveDomain(
            "Q19",
            "renormalization-group-coarsegrain",
            "workgraph",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain("Q20", "universality-contracts", "authority", (PhysicsModel.BOLTZMANN_GIBBS,)),
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
