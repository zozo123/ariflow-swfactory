"""Canonical runtime seam for Ocean120, Phase240, and StatMech360 implementation bundles.

Bundles describe bounded domain slices. They do not create schedulers or mutation paths;
all dynamic recommendations come from ``physics_mixture`` and all protected transitions
may additionally require the high-assurance ``safety_kernel``. Apache Airflow remains the
sole lifecycle scheduler.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from swfactory.physics_mixture import (
    ControlAction,
    MixtureDecision,
    PhysicsModel,
    PhysicsSample,
    evaluate_mixture,
)
from swfactory.safety_kernel import SafetyCase, SafetyDecision, evaluate_safety_case


class Concern(StrEnum):
    C01 = "C01"
    C02 = "C02"
    C03 = "C03"
    C04 = "C04"
    C05 = "C05"
    C06 = "C06"
    C07 = "C07"
    C08 = "C08"
    C09 = "C09"
    C10 = "C10"


class Capability(StrEnum):
    INVARIANT = "invariant"
    DURABLE_STATE = "durable_state"
    VERSIONED_CONTRACT = "versioned_contract"
    AIRFLOW_ADVISORY = "airflow_advisory"
    OPERATOR_EXPLANATION = "operator_explanation"
    SECURITY_BOUNDARY = "security_boundary"
    RECOVERY_DYNAMICS = "recovery_dynamics"
    STRESS_ENVELOPE = "stress_envelope"
    EVIDENCE = "evidence"
    CONVERGENCE = "convergence"


_CONCERN_CAPABILITY = {
    Concern.C01: Capability.INVARIANT,
    Concern.C02: Capability.DURABLE_STATE,
    Concern.C03: Capability.VERSIONED_CONTRACT,
    Concern.C04: Capability.AIRFLOW_ADVISORY,
    Concern.C05: Capability.OPERATOR_EXPLANATION,
    Concern.C06: Capability.SECURITY_BOUNDARY,
    Concern.C07: Capability.RECOVERY_DYNAMICS,
    Concern.C08: Capability.STRESS_ENVELOPE,
    Concern.C09: Capability.EVIDENCE,
    Concern.C10: Capability.CONVERGENCE,
}

_WAVE_PREFIX = {
    "Ocean120": "D",
    "Phase240": "P",
    "StatMech360": "Q",
}


@dataclass(frozen=True, slots=True)
class WaveDomain:
    domain_id: str
    slug: str
    owner: str
    models: tuple[PhysicsModel, ...]

    def validate(self) -> None:
        if not self.domain_id or not self.slug or not self.owner:
            raise ValueError("wave domain identity, slug, and owner are required")
        if not self.models:
            raise ValueError("each wave domain must name at least one physics model")
        if any(ch.isspace() for ch in self.slug):
            raise ValueError("wave domain slug cannot contain whitespace")


@dataclass(frozen=True, slots=True)
class WaveBundle:
    wave: str
    bundle_id: str
    domains: tuple[WaveDomain, ...]

    def validate(self) -> None:
        prefix = _WAVE_PREFIX.get(self.wave)
        if prefix is None:
            raise ValueError("unsupported physics wave")
        if len(self.domains) != 4:
            raise ValueError("each implementation bundle must contain exactly four domains")
        if len({domain.domain_id for domain in self.domains}) != 4:
            raise ValueError("bundle domains must be unique")
        for domain in self.domains:
            domain.validate()
            if not domain.domain_id.startswith(prefix):
                raise ValueError(f"domain {domain.domain_id!r} has wrong prefix for {self.wave}")

    @property
    def issue_count(self) -> int:
        return len(self.domains) * len(Concern)

    @property
    def issue_keys(self) -> tuple[str, ...]:
        """Semantic issue keys remain stable even when GitHub issue numbers interleave."""
        return tuple(
            f"{self.wave}/{domain.domain_id}-{concern.value}"
            for domain in self.domains
            for concern in Concern
        )


@dataclass(frozen=True, slots=True)
class WaveDecision:
    wave: str
    bundle_id: str
    domain_id: str
    concern: Concern
    capability: Capability
    mixture: MixtureDecision
    safety: SafetyDecision | None
    promotable: bool
    reason: str


def execute_wave(
    bundle: WaveBundle,
    *,
    domain_id: str,
    concern: Concern,
    sample: PhysicsSample,
    release_gate_open: bool = False,
    safety_case: SafetyCase | None = None,
) -> WaveDecision:
    """Evaluate one issue slice through the canonical parallel-model control path."""

    bundle.validate()
    if bundle.issue_count != 40:
        raise ValueError("physics implementation bundles must account for exactly 40 issues")
    domain = next((candidate for candidate in bundle.domains if candidate.domain_id == domain_id), None)
    if domain is None:
        raise ValueError(f"domain {domain_id!r} is not part of bundle {bundle.bundle_id!r}")

    mixture = evaluate_mixture(sample, release_gate_open=release_gate_open)
    safety = evaluate_safety_case(safety_case, release=release_gate_open) if safety_case is not None else None
    capability = _CONCERN_CAPABILITY[concern]

    blocked_by_safety = safety is not None and not safety.allowed
    blocked_by_physics = mixture.action in {
        ControlAction.REFUSE,
        ControlAction.FENCE,
        ControlAction.SHED,
        ControlAction.HOLD,
    }
    convergence_concern = concern == Concern.C10
    promotable = release_gate_open and not blocked_by_safety and not blocked_by_physics and convergence_concern

    if blocked_by_safety:
        reason = f"safety-case-blocked:{safety.reason}"
    elif blocked_by_physics:
        reason = f"physics-blocked:{mixture.reason}"
    elif convergence_concern and not release_gate_open:
        reason = "convergence remains non-promotable before the explicit release gate"
    else:
        reason = f"{capability.value} evaluated by {','.join(model.value for model in domain.models)}"

    return WaveDecision(
        bundle.wave,
        bundle.bundle_id,
        domain.domain_id,
        concern,
        capability,
        mixture,
        safety,
        promotable,
        reason,
    )
