"""StatMech360 bundle 01: Q01-Q04, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-01",
    (
        WaveDomain(
            "Q01",
            "creation-operator",
            "workgraph",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.PHASE_FIELD),
        ),
        WaveDomain(
            "Q02",
            "annihilation-operator",
            "recovery",
            (PhysicsModel.BOLTZMANN_GIBBS, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain("Q03", "number-operator", "evidence", (PhysicsModel.BOLTZMANN_GIBBS,)),
        WaveDomain(
            "Q04",
            "commutator-conflict",
            "authority",
            (PhysicsModel.DISCRETE_QUANTUM, PhysicsModel.REACTION_CRITICALITY),
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
