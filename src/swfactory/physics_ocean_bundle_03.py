"""Ocean120 bundle 03: D09-D12, 40 semantic issues."""

from swfactory.physics_mixture import PhysicsModel as M
from swfactory.physics_wave_runtime import WaveBundle, WaveDomain

BUNDLE = WaveBundle(
    "Ocean120",
    "ocean-03",
    (
        WaveDomain("D09", "cavitation-retry-collapse", "recovery", (M.FLUID, M.NONEQUILIBRIUM, M.REACTION_CRITICALITY)),
        WaveDomain("D10", "wave-propagation-events", "authority", (M.FLUID, M.REACTION_CRITICALITY, M.DISCRETE_QUANTUM)),
        WaveDomain("D11", "eddy-cache-locality", "operator", (M.PROTEIN_NETWORK, M.FLUID, M.BOLTZMANN_GIBBS)),
        WaveDomain("D12", "dissipation-entropy-collapse", "operator", (M.BOLTZMANN_GIBBS, M.PHASE_FIELD, M.NONEQUILIBRIUM)),
    ),
)
BUNDLE.validate()
ISSUE_KEYS = BUNDLE.issue_keys
assert len(ISSUE_KEYS) == 40
