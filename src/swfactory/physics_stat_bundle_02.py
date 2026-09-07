"""StatMech360 bundle 02: Q05-Q08, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-02",
    (
        WaveDomain(
            "Q05",
            "fock-branch-population",
            "workgraph",
            (PhysicsModel.DISCRETE_QUANTUM, PhysicsModel.BOLTZMANN_GIBBS),
        ),
        WaveDomain("Q06", "canonical-ensemble", "evidence", (PhysicsModel.BOLTZMANN_GIBBS,)),
        WaveDomain(
            "Q07",
            "grand-canonical-admission",
            "airflow",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.FLUID),
        ),
        WaveDomain(
            "Q08",
            "microcanonical-replay",
            "recovery",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.NONEQUILIBRIUM),
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
