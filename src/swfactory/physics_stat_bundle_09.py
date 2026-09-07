"""StatMech360 bundle 09: Q33-Q36, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-09",
    (
        WaveDomain(
            "Q33",
            "mean-field-capacity",
            "airflow",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.FLUID),
        ),
        WaveDomain(
            "Q34",
            "bose-like-batching",
            "operator",
            (PhysicsModel.DISCRETE_QUANTUM, PhysicsModel.PROTEIN_NETWORK),
        ),
        WaveDomain("Q35", "fermi-like-exclusion", "security", (PhysicsModel.DISCRETE_QUANTUM,)),
        WaveDomain(
            "Q36",
            "vacuum-state-reconciliation",
            "operator",
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
