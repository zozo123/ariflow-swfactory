"""StatMech360 bundle 07: Q25-Q28, 40 issue slices."""

from swfactory.physics_mixture import PhysicsModel, PhysicsSample
from swfactory.physics_wave_runtime import Concern, WaveBundle, WaveDecision, WaveDomain, execute_wave
from swfactory.safety_kernel import SafetyCase

BUNDLE = WaveBundle(
    "StatMech360",
    "stat-07",
    (
        WaveDomain(
            "Q25",
            "nucleation-barrier",
            "evidence",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.NONEQUILIBRIUM),
        ),
        WaveDomain(
            "Q26",
            "topological-defect-state",
            "recovery",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.DISCRETE_QUANTUM),
        ),
        WaveDomain(
            "Q27",
            "domain-wall-boundary",
            "security",
            (PhysicsModel.PHASE_FIELD, PhysicsModel.DISCRETE_QUANTUM),
        ),
        WaveDomain(
            "Q28",
            "lattice-workgraph",
            "workgraph",
            (PhysicsModel.DISCRETE_QUANTUM, PhysicsModel.PROTEIN_NETWORK),
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
