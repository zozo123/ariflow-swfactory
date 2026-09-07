"""Registry of advisory physics/control lenses used by the software factory.

Every lens observes the same durable Factory Cell trajectory. None owns lifecycle scheduling.
Apache Airflow is the sole lifecycle scheduler and Cell/epoch identity fences mutation.
"""

from dataclasses import dataclass
from enum import StrEnum


class ModelFamily(StrEnum):
    HYDRODYNAMICS = "hydrodynamics"
    PHASE_FIELD = "phase-field"
    EQUILIBRIUM_STATMECH = "equilibrium-statmech"
    NON_EQUILIBRIUM = "non-equilibrium"
    INFORMATION = "information-theory"
    CRITICAL_PHENOMENA = "critical-phenomena"
    CONTROL = "control-theory"
    REACTION_NETWORK = "reaction-network"
    NUCLEAR_CASCADE = "nuclear-cascade"
    DISCRETE_OPERATOR = "discrete-operator"
    HIGH_ASSURANCE = "high-assurance"


@dataclass(frozen=True, slots=True)
class ModelLens:
    family: ModelFamily
    question: str
    observables: tuple[str, ...]
    use_when: tuple[str, ...]
    authority: str = "advisory"

    def validate(self) -> None:
        if not self.question or not self.observables or not self.use_when:
            raise ValueError(f"incomplete model lens: {self.family}")
        if self.authority != "advisory":
            raise ValueError(f"physics lens cannot own lifecycle authority: {self.family}")


LENSES: tuple[ModelLens, ...] = (
    ModelLens(
        ModelFamily.HYDRODYNAMICS,
        "Where is work flowing, accumulating, accelerating, recirculating, or becoming unstable?",
        ("currents", "pressure", "velocity", "acceleration", "vorticity", "saturation", "relaxation"),
        ("queue-surge", "provider-saturation", "event-burst", "retry-flow", "evidence-lag"),
    ),
    ModelLens(
        ModelFamily.PHASE_FIELD,
        "Which development phase exists and is crystallization physically analogous to a safe release?",
        ("phase", "order-parameters", "metastability", "hysteresis", "nucleation-barrier", "defects"),
        ("fanout", "competing-implementations", "fanin", "release"),
    ),
    ModelLens(
        ModelFamily.EQUILIBRIUM_STATMECH,
        "How should uncertain alternatives be distributed without inventing confidence?",
        ("partition-function", "probabilities", "entropy", "free-energy", "dominant-strategy"),
        ("strategy-comparison", "branch-population", "uncertain-selection"),
    ),
    ModelLens(
        ModelFamily.NON_EQUILIBRIUM,
        "What trajectory produced the state and how irreversible or biased was that path?",
        ("path-caliber", "work", "heat", "entropy-production", "detailed-balance-bias", "fluctuation-ratio"),
        ("retry", "repair", "rollback", "mutation", "failure-recovery", "driven-workload"),
    ),
    ModelLens(
        ModelFamily.INFORMATION,
        "Is observed entropy useful information or coordination noise, and which signals predict outcomes?",
        ("shannon-entropy", "kl-divergence", "mutual-information", "channel-capacity", "rate-distortion"),
        ("evidence-compression", "drift", "signal-quality", "model-selection"),
    ),
    ModelLens(
        ModelFamily.CRITICAL_PHENOMENA,
        "Are small perturbations beginning to create system-scale responses?",
        ("susceptibility", "correlation-length", "percolation", "frustration", "criticality"),
        ("cross-repo-propagation", "retry-storm", "dependency-cluster", "policy-perturbation"),
    ),
    ModelLens(
        ModelFamily.CONTROL,
        "Which bounded feedback stabilizes the system without becoming a scheduler?",
        ("error", "gain", "damping", "hysteresis", "stability-margin", "forecast"),
        ("admission", "backpressure", "load-shedding", "capacity-control"),
    ),
    ModelLens(
        ModelFamily.REACTION_NETWORK,
        "Which multi-component complexes can form and which scarce partner or regulator limits them?",
        ("stoichiometry", "reaction-flux", "occupancy", "cooperativity", "proofreading", "compartments"),
        ("shared-dependencies", "scarce-review", "provider-competition", "multi-agent-complex", "phase-separation"),
    ),
    ModelLens(
        ModelFamily.NUCLEAR_CASCADE,
        "Can one retry/fanout event reproduce itself into a runaway chain and how is it contained?",
        ("k-effective", "half-life", "critical-mass", "absorber-strength", "binding-margin"),
        ("retry-chain", "recursive-fanout", "event-amplification", "stale-state-decay"),
    ),
    ModelLens(
        ModelFamily.DISCRETE_OPERATOR,
        "Which discrete populations or order-sensitive operations require algebraic exclusion/fencing?",
        ("population", "commutator", "exclusion", "creation", "annihilation", "ordering"),
        ("concurrent-mutation", "branch-population", "exclusive-authority", "batched-locality"),
    ),
    ModelLens(
        ModelFamily.HIGH_ASSURANCE,
        "What hazard can escape containment and what evidence proves the protected transition safe?",
        ("hazards", "fault-containment", "degradation", "safety-case", "replayability", "watchdogs"),
        ("external-mutation", "promotion", "security-boundary", "provider-loss", "release"),
    ),
)


def validate_registry() -> None:
    families: set[ModelFamily] = set()
    for lens in LENSES:
        lens.validate()
        if lens.family in families:
            raise ValueError(f"duplicate model family: {lens.family}")
        families.add(lens.family)


def select_lenses(*contexts: str) -> tuple[ModelLens, ...]:
    """Return all relevant lenses; overlapping contexts intentionally select multiple models."""

    wanted = set(contexts)
    if not wanted:
        return LENSES
    return tuple(lens for lens in LENSES if wanted.intersection(lens.use_when))


validate_registry()
