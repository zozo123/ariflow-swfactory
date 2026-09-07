"""Phase240 bundle 05: P17-P20, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "Phase240",
    "phase-05",
    (
        WaveDomain(
            "P17",
            "diffusive-evidence-propagation",
            "evidence",
            (PhysicsModel.FLUID, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "P18",
            "convective-work-dispatch",
            "airflow",
            (PhysicsModel.FLUID, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "P19",
            "heat-flux-budgeting",
            "evidence",
            (PhysicsModel.NONEQUILIBRIUM, PhysicsModel.BOLTZMANN_GIBBS),
        ),
        WaveDomain(
            "P20",
            "latent-heat-migrations",
            "recovery",
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
