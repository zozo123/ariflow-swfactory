"""Ocean120 bundle 01: D01-D04, 40 semantic issues."""

from swfactory.physics_mixture import PhysicsModel as M
from swfactory.physics_wave_runtime import WaveBundle, WaveDomain

BUNDLE = WaveBundle(
    "Ocean120",
    "ocean-01",
    (
        WaveDomain("D01", "pressure-routing", "authority", (M.FLUID, M.NONEQUILIBRIUM, M.BOLTZMANN_GIBBS)),
        WaveDomain("D02", "viscosity-backpressure", "airflow", (M.FLUID, M.NONEQUILIBRIUM, M.PHASE_FIELD)),
        WaveDomain("D03", "turbulence-detection", "evidence", (M.FLUID, M.REACTION_CRITICALITY, M.NONEQUILIBRIUM)),
        WaveDomain("D04", "vorticity-work-stealing", "workgraph", (M.FLUID, M.PROTEIN_NETWORK, M.DISCRETE_QUANTUM)),
    ),
)
BUNDLE.validate()
ISSUE_KEYS = BUNDLE.issue_keys
assert len(ISSUE_KEYS) == 40
