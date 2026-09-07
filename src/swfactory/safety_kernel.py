"""High-assurance safety kernel for protected software-factory transitions.

The kernel records hazards and safety-case evidence. It never schedules lifecycle work and cannot
weaken Cell/epoch/security/authority invariants. Apache Airflow remains the sole scheduler.
"""

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    MINOR = "minor"
    MAJOR = "major"
    HAZARDOUS = "hazardous"
    CATASTROPHIC = "catastrophic"


class Containment(StrEnum):
    NONE = "none"
    THROTTLE = "throttle"
    ISOLATE = "isolate"
    FAIL_CLOSED = "fail-closed"


@dataclass(frozen=True, slots=True)
class Hazard:
    hazard_id: str
    description: str
    severity: Severity
    likelihood: float
    containment: Containment
    fault_zone: str

    def validate(self) -> None:
        if not self.hazard_id or not self.description or not self.fault_zone:
            raise ValueError("hazard identity, description, and fault zone required")
        if not 0 <= self.likelihood <= 1:
            raise ValueError("hazard likelihood must be in [0, 1]")
        if self.severity in {Severity.HAZARDOUS, Severity.CATASTROPHIC} and self.containment == Containment.NONE:
            raise ValueError("high-severity hazard requires explicit containment")


@dataclass(frozen=True, slots=True)
class SafetyEvidence:
    claim: str
    evidence_id: str
    invariant: str
    exact_head: bool
    independently_observed: bool = False

    def validate(self) -> None:
        if not self.claim or not self.evidence_id or not self.invariant:
            raise ValueError("complete safety evidence required")


@dataclass(frozen=True, slots=True)
class SafetyCase:
    transition: str
    hazards: tuple[Hazard, ...]
    evidence: tuple[SafetyEvidence, ...]
    graceful_degradation_defined: bool
    deterministic_replay: bool
    watchdog_defined: bool


@dataclass(frozen=True, slots=True)
class SafetyDecision:
    allowed: bool
    reason: str
    blocking_hazards: tuple[str, ...]
    missing_claims: tuple[str, ...]


def evaluate_safety_case(case: SafetyCase, *, release: bool = False) -> SafetyDecision:
    if not case.transition:
        raise ValueError("transition required")
    for hazard in case.hazards:
        hazard.validate()
    for item in case.evidence:
        item.validate()

    blocking = tuple(
        hazard.hazard_id
        for hazard in case.hazards
        if hazard.severity in {Severity.HAZARDOUS, Severity.CATASTROPHIC}
        and hazard.likelihood > 0
        and hazard.containment not in {Containment.ISOLATE, Containment.FAIL_CLOSED}
    )

    missing: list[str] = []
    if not case.graceful_degradation_defined:
        missing.append("graceful-degradation")
    if not case.deterministic_replay:
        missing.append("deterministic-replay")
    if not case.watchdog_defined:
        missing.append("watchdog")
    if release and not any(item.exact_head for item in case.evidence):
        missing.append("exact-head-evidence")
    if release and not any(item.independently_observed for item in case.evidence):
        missing.append("independent-observation")

    if blocking:
        return SafetyDecision(False, "high-severity hazard lacks sufficient containment", blocking, tuple(missing))
    if missing:
        return SafetyDecision(False, "safety case is incomplete", (), tuple(missing))
    return SafetyDecision(True, "hazards are contained and required safety evidence is present", (), ())
