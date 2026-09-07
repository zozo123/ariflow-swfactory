"""Constrained fusion of parallel advisory physics lenses.

The fusion kernel never schedules lifecycle work. It preserves model disagreement, applies hard
invariants before soft evidence, and emits bounded advisory actions for Airflow-owned orchestration.
"""

from dataclasses import dataclass
from enum import StrEnum

from swfactory.physics_registry import ModelFamily


class FusionAction(StrEnum):
    EXPAND = "expand"
    MIX = "mix"
    HOLD = "hold"
    THROTTLE = "throttle"
    REPAIR = "repair"
    ISOLATE = "isolate"
    CRYSTALLIZE = "crystallize"
    REFUSE = "refuse"


@dataclass(frozen=True, slots=True)
class HardInvariants:
    scheduler: str = "airflow"
    cell_identity_valid: bool = True
    epoch_positive: bool = True
    external_mutation_fenced: bool = True
    authority_singular: bool = True
    evidence_durable: bool = True
    security_ok: bool = True
    exact_head_checks_green: bool = False

    def violations(self) -> tuple[str, ...]:
        failures: list[str] = []
        if self.scheduler != "airflow":
            failures.append("scheduler-must-be-airflow")
        checks = (
            (self.cell_identity_valid, "invalid-cell-identity"),
            (self.epoch_positive, "invalid-epoch"),
            (self.external_mutation_fenced, "unfenced-external-mutation"),
            (self.authority_singular, "duplicate-authority"),
            (self.evidence_durable, "non-durable-evidence"),
            (self.security_ok, "security-boundary-failed"),
        )
        failures.extend(name for ok, name in checks if not ok)
        return tuple(failures)


@dataclass(frozen=True, slots=True)
class LensObservation:
    family: ModelFamily
    risk: float
    confidence: float
    recommended: FusionAction
    reason: str
    observables: tuple[tuple[str, float], ...] = ()

    def validate(self) -> None:
        if not 0 <= self.risk <= 1:
            raise ValueError("risk must be in [0, 1]")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be in [0, 1]")
        if not self.reason:
            raise ValueError("observation reason required")


@dataclass(frozen=True, slots=True)
class FusionDecision:
    action: FusionAction
    reason: str
    observations: tuple[LensObservation, ...]
    dissent: tuple[ModelFamily, ...]
    hard_violations: tuple[str, ...]
    max_risk: float
    weighted_risk: float


def fuse(
    invariants: HardInvariants,
    observations: tuple[LensObservation, ...],
    *,
    crystallize: bool = False,
    containment_threshold: float = 0.85,
    throttle_threshold: float = 0.65,
    crystallize_risk_ceiling: float = 0.20,
) -> FusionDecision:
    """Fuse model outputs without averaging away hard failures or high-risk dissent."""

    if not 0 <= crystallize_risk_ceiling <= throttle_threshold <= containment_threshold <= 1:
        raise ValueError("invalid fusion thresholds")
    for observation in observations:
        observation.validate()

    violations = invariants.violations()
    max_risk = max((observation.risk for observation in observations), default=0.0)
    confidence_mass = sum(observation.confidence for observation in observations)
    weighted_risk = (
        sum(observation.risk * observation.confidence for observation in observations) / confidence_mass
        if confidence_mass
        else 0.0
    )

    if violations:
        return FusionDecision(
            FusionAction.REFUSE,
            "hard factory invariant failed; advisory models cannot vote through it",
            observations,
            tuple(observation.family for observation in observations),
            violations,
            max_risk,
            weighted_risk,
        )

    high_risk = tuple(observation for observation in observations if observation.risk >= containment_threshold)
    if high_risk:
        return FusionDecision(
            FusionAction.ISOLATE,
            "one or more lenses require fault containment",
            observations,
            tuple(observation.family for observation in high_risk),
            (),
            max_risk,
            weighted_risk,
        )

    throttlers = tuple(observation for observation in observations if observation.risk >= throttle_threshold)
    if throttlers:
        return FusionDecision(
            FusionAction.THROTTLE,
            "parallel lenses detect unstable pressure/criticality below containment threshold",
            observations,
            tuple(observation.family for observation in throttlers),
            (),
            max_risk,
            weighted_risk,
        )

    recommendations = {observation.recommended for observation in observations}
    dissent = tuple(
        observation.family
        for observation in observations
        if len(recommendations) > 1 and observation.recommended not in {FusionAction.EXPAND, FusionAction.MIX}
    )

    if crystallize:
        if not invariants.exact_head_checks_green:
            return FusionDecision(
                FusionAction.HOLD,
                "crystallization requires exact-head required checks",
                observations,
                dissent,
                (),
                max_risk,
                weighted_risk,
            )
        if max_risk > crystallize_risk_ceiling:
            return FusionDecision(
                FusionAction.HOLD,
                "at least one relevant model remains above the crystallization risk ceiling",
                observations,
                dissent,
                (),
                max_risk,
                weighted_risk,
            )
        if any(observation.recommended in {FusionAction.REPAIR, FusionAction.HOLD, FusionAction.ISOLATE} for observation in observations):
            return FusionDecision(
                FusionAction.HOLD,
                "model dissent still requests repair/hold/containment",
                observations,
                dissent,
                (),
                max_risk,
                weighted_risk,
            )
        return FusionDecision(
            FusionAction.CRYSTALLIZE,
            "hard invariants, exact-head checks, and all relevant model ceilings permit solidification",
            observations,
            dissent,
            (),
            max_risk,
            weighted_risk,
        )

    if FusionAction.REPAIR in recommendations:
        action = FusionAction.REPAIR
        reason = "at least one advisory lens identifies repairable structural debt"
    elif FusionAction.HOLD in recommendations:
        action = FusionAction.HOLD
        reason = "at least one advisory lens requests a metastability/evidence hold"
    elif len(recommendations) > 1:
        action = FusionAction.MIX
        reason = "model disagreement is preserved explicitly during liquid development"
    else:
        action = next(iter(recommendations), FusionAction.EXPAND)
        reason = "parallel advisory lenses agree within current safety bounds"

    return FusionDecision(action, reason, observations, dissent, (), max_risk, weighted_risk)
