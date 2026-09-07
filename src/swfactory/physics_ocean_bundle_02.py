"""Ocean120 bundle 02: D05-D08, 40 semantic issues."""

from swfactory.physics_mixture import PhysicsModel as M
from swfactory.physics_wave_runtime import WaveBundle, WaveDomain

BUNDLE = WaveBundle(
    "Ocean120",
    "ocean-02",
    (
        WaveDomain(
            "D05",
            "boundary-layer-admission",
            "security",
            (M.FLUID, M.DISCRETE_QUANTUM, M.PROTEIN_NETWORK),
        ),
        WaveDomain(
            "D06",
            "incompressible-cell-conservation",
            "recovery",
            (M.NONEQUILIBRIUM, M.DISCRETE_QUANTUM, M.BOLTZMANN_GIBBS),
        ),
        WaveDomain("D07", "flux-accounting", "evidence", (M.FLUID, M.NONEQUILIBRIUM, M.BOLTZMANN_GIBBS)),
        WaveDomain(
            "D08",
            "reynolds-regime-switching",
            "airflow",
            (M.FLUID, M.PHASE_FIELD, M.REACTION_CRITICALITY),
        ),
    ),
)
BUNDLE.validate()
ISSUE_KEYS = BUNDLE.issue_keys
assert len(ISSUE_KEYS) == 40
