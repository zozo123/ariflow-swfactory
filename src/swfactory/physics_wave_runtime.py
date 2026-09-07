"""Canonical runtime for Ocean120, Phase240, and StatMech360 issue waves.

These waves are stress/control models around the existing software-factory runtime. They are advisory
and observational only: Apache Airflow remains the sole lifecycle scheduler, durable Factory Cell
identity plus epoch remains the mutation authority, and every external side effect is routed through
existing canonical authorities rather than through a second scheduler or control plane.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .liquid_bundle_engine import BundleAction, Concern, DomainSpec, ExecutionIntent, execute


class PhysicsWave(StrEnum):
    OCEAN120 = "Ocean120"
    PHASE240 = "Phase240"
    STATMECH360 = "StatMech360"


class PhaseState(StrEnum):
    GAS = "GAS"
    LIQUID = "LIQUID"
    MIXED = "MIXED"
    SUPERCRITICAL = "SUPERCRITICAL"
    CRYSTALLIZED = "CRYSTALLIZED"


DOMAIN_COUNTS: dict[PhysicsWave, int] = {
    PhysicsWave.OCEAN120: 12,
    PhysicsWave.PHASE240: 24,
    PhysicsWave.STATMECH360: 36,
}

ISSUE_COUNTS: dict[PhysicsWave, int] = {
    PhysicsWave.OCEAN120: 120,
    PhysicsWave.PHASE240: 240,
    PhysicsWave.STATMECH360: 360,
}

ANCHORS: dict[PhysicsWave, str] = {
    PhysicsWave.OCEAN120: "admission_backpressure",
    PhysicsWave.PHASE240: "release_promotion",
    PhysicsWave.STATMECH360: "lifecycle_evidence",
}

NON_PROMOTABLE_PHASES = {
    PhaseState.GAS,
    PhaseState.LIQUID,
    PhaseState.MIXED,
    PhaseState.SUPERCRITICAL,
}


@dataclass(frozen=True)
class WaveBundle:
    """A 40-issue implementation slice: four domains times ten recurring concerns."""

    bundle_id: str
    wave: PhysicsWave
    domain_start: int
    domain_end: int

    def validate(self) -> None:
        if not self.bundle_id:
            raise ValueError("wave bundle requires a stable bundle id")
        if self.domain_end - self.domain_start + 1 != 4:
            raise ValueError("physics implementation bundles must cover exactly four domains")
        if self.domain_start < 1 or self.domain_end > DOMAIN_COUNTS[self.wave]:
            raise ValueError("wave bundle domain range is outside the generated wave")

    @property
    def issue_count(self) -> int:
        self.validate()
        return (self.domain_end - self.domain_start + 1) * len(Concern)

    def contains(self, domain_ordinal: int) -> bool:
        self.validate()
        return self.domain_start <= domain_ordinal <= self.domain_end


@dataclass(frozen=True)
class WaveIntent:
    wave: PhysicsWave
    domain_ordinal: int
    domain_slug: str
    concern: Concern
    action: BundleAction
    anchor: str
    cell_id: str
    epoch: int
    promotable: bool
    reason: str


def _validate_domain(wave: PhysicsWave, domain_ordinal: int, domain_slug: str, owner: str) -> DomainSpec:
    if not 1 <= domain_ordinal <= DOMAIN_COUNTS[wave]:
        raise ValueError("domain ordinal is outside the generated wave")
    if not domain_slug:
        raise ValueError("wave execution requires a domain slug")
    if not owner:
        raise ValueError("wave execution requires an owner lane")
    return DomainSpec(
        slug=f"{wave.value}:{domain_ordinal:02d}:{domain_slug}",
        owner=owner,
        anchor=ANCHORS[wave],
    )


def execute_wave(
    wave: PhysicsWave,
    *,
    domain_ordinal: int,
    domain_slug: str,
    owner: str,
    concern: Concern,
    cell_id: str,
    epoch: int,
    scheduler: str = "airflow",
    policy_ok: bool = True,
    evidence_ok: bool = True,
    overloaded: bool = False,
    failed: bool = False,
    cancelled: bool = False,
    phase_state: PhaseState = PhaseState.CRYSTALLIZED,
    operator_external_mutation: bool = False,
) -> WaveIntent:
    """Route a physics-wave concern into the existing canonical runtime authorities."""

    spec = _validate_domain(wave, domain_ordinal, domain_slug, owner)
    base: ExecutionIntent = execute(
        spec,
        concern=concern,
        cell_id=cell_id,
        epoch=epoch,
        scheduler=scheduler,
        policy_ok=policy_ok,
        evidence_ok=evidence_ok,
        overloaded=overloaded,
        failed=failed,
        cancelled=cancelled,
    )

    action = base.action
    reason = base.reason
    promotable = True

    if wave is PhysicsWave.OCEAN120 and scheduler != "airflow":
        action = BundleAction.REFUSE
        reason = "fluid-control signals are advisory; Airflow remains the only lifecycle scheduler"
        promotable = False
    elif wave is PhysicsWave.PHASE240 and phase_state in NON_PROMOTABLE_PHASES:
        promotable = False
        if concern is Concern.STABILIZE:
            action = BundleAction.REFUSE
            reason = f"phase {phase_state.value} cannot promote; crystallization is an explicit final gate"
    elif wave is PhysicsWave.STATMECH360 and operator_external_mutation:
        action = BundleAction.REFUSE
        reason = "operator algebra is observational and cannot mutate GitHub, provider, or release authority"
        promotable = False

    return WaveIntent(
        wave=wave,
        domain_ordinal=domain_ordinal,
        domain_slug=domain_slug,
        concern=concern,
        action=action,
        anchor=base.anchor,
        cell_id=base.cell_id,
        epoch=base.epoch,
        promotable=promotable,
        reason=reason,
    )


def wave_issue_slots(wave: PhysicsWave) -> tuple[tuple[int, Concern], ...]:
    """Return the deterministic 10-concern slot set for every domain in a wave."""

    return tuple((domain_ordinal, concern) for domain_ordinal in range(1, DOMAIN_COUNTS[wave] + 1) for concern in Concern)
