"""StatMech360 bundle 08: Q29-Q32, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-08",
    (
        WaveDomain(
            "Q29",
            "monte-carlo-exploration",
            "workgraph",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "Q30",
            "detailed-balance-retry",
            "recovery",
            (PhysicsModel.NONEQUILIBRIUM, PhysicsModel.BOLTZMANN_GIBBS),
        ),
        WaveDomain(
            "Q31",
            "green-function-response",
            "evidence",
            (PhysicsModel.NONEQUILIBRIUM, PhysicsModel.FLUID),
        ),
        WaveDomain(
            "Q32",
            "fluctuation-dissipation",
            "airflow",
            (PhysicsModel.NONEQUILIBRIUM, PhysicsModel.FLUID),
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
