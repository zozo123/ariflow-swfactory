"""StatMech360 bundle 03: Q09-Q12, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-03",
    (
        WaveDomain("Q09", "partition-function-strategy", "evidence", (PhysicsModel.BOLTZMANN_GIBBS,)),
        WaveDomain(
            "Q10",
            "free-energy-selection",
            "authority",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "Q11",
            "entropy-budget",
            "evidence",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "Q12",
            "chemical-potential-priority",
            "airflow",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.FLUID),
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
