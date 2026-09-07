"""Phase240 bundle 01: P01-P04, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "Phase240",
    "phase-01",
    (
        WaveDomain("P01", "bernoulli-energy-budget", "evidence", (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.NONEQUILIBRIUM, PhysicsModel.FLUID)),
        WaveDomain("P02", "pressure-velocity-coupling", "airflow", (PhysicsModel.FLUID, PhysicsModel.NONEQUILIBRIUM)),
        WaveDomain("P03", "continuity-work-conservation", "recovery", (PhysicsModel.FLUID, PhysicsModel.NONEQUILIBRIUM)),
        WaveDomain("P04", "control-volume-accounting", "evidence", (PhysicsModel.FLUID, PhysicsModel.BOLTZMANN_GIBBS)),
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
