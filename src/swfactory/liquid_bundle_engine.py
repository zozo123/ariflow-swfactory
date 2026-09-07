"""Executable rough implementation engine for consolidated Liquid issue bundles.

The generated Liquid400/Liquid500 backlogs describe ten recurring concern classes.  This module
turns those concerns into one bounded runtime contract so bundle workers register domains instead
of inventing duplicate schedulers, stores or policy engines.  Apache Airflow remains the only
lifecycle scheduler; this engine only validates and emits deterministic execution intents.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Concern(StrEnum):
    INVARIANT = "C01"
    PERSISTENCE = "C02"
    API = "C03"
    RUNTIME = "C04"
    OPERATOR = "C05"
    SECURITY = "C06"
    RECOVERY = "C07"
    SCALE = "C08"
    EVIDENCE = "C09"
    STABILIZE = "C10"


class BundleAction(StrEnum):
    ASSERT = "assert_invariant"
    PERSIST = "persist"
    EXPOSE = "expose_versioned_api"
    DISPATCH = "dispatch_from_airflow"
    INSPECT = "inspect_or_repair"
    AUTHORIZE = "authorize"
    RECOVER = "recover"
    CANCEL = "cancel"
    THROTTLE = "throttle"
    MEASURE = "measure_and_seal"
    STABILIZE = "stabilize_and_delete_superseded"
    REFUSE = "refuse"


@dataclass(frozen=True)
class DomainSpec:
    slug: str
    owner: str
    anchor: str

    def validate(self) -> None:
        if not self.slug or not self.owner or not self.anchor:
            raise ValueError("domain spec requires slug, owner and canonical anchor")


@dataclass(frozen=True)
class BundleSpec:
    bundle_id: str
    issue_start: int
    issue_end: int
    domains: tuple[DomainSpec, ...]
    source: str

    def validate(self) -> None:
        if self.issue_end - self.issue_start + 1 != 50:
            raise ValueError("implementation bundles must cover exactly 50 issues")
        if len(self.domains) != 5:
            raise ValueError("a 50-issue bundle must cover five ten-concern domains")
        if len({domain.slug for domain in self.domains}) != len(self.domains):
            raise ValueError("bundle domains must be unique")
        for domain in self.domains:
            domain.validate()


@dataclass(frozen=True)
class ExecutionIntent:
    domain: str
    concern: Concern
    action: BundleAction
    anchor: str
    cell_id: str
    epoch: int
    reason: str


def execute(
    spec: DomainSpec,
    *,
    concern: Concern,
    cell_id: str,
    epoch: int,
    scheduler: str = "airflow",
    policy_ok: bool = True,
    evidence_ok: bool = True,
    overloaded: bool = False,
    failed: bool = False,
    cancelled: bool = False,
) -> ExecutionIntent:
    spec.validate()
    if scheduler != "airflow":
        raise ValueError("Apache Airflow is the only lifecycle scheduler")
    if not cell_id.startswith("cell_"):
        raise ValueError("bundle execution requires durable Factory Cell identity")
    if epoch < 1:
        raise ValueError("bundle execution requires positive Cell epoch")

    if concern is Concern.SECURITY and not policy_ok:
        action, reason = BundleAction.REFUSE, "policy failed closed"
    elif concern is Concern.RECOVERY and cancelled:
        action, reason = BundleAction.CANCEL, "durable cancellation wins during recovery"
    elif concern is Concern.RECOVERY and failed:
        action, reason = BundleAction.RECOVER, "repair from durable Cell intent"
    elif concern is Concern.SCALE and overloaded:
        action, reason = BundleAction.THROTTLE, "bounded backpressure instead of second scheduler"
    elif concern is Concern.EVIDENCE and not evidence_ok:
        action, reason = BundleAction.REFUSE, "claims require retained evidence"
    else:
        action = {
            Concern.INVARIANT: BundleAction.ASSERT,
            Concern.PERSISTENCE: BundleAction.PERSIST,
            Concern.API: BundleAction.EXPOSE,
            Concern.RUNTIME: BundleAction.DISPATCH,
            Concern.OPERATOR: BundleAction.INSPECT,
            Concern.SECURITY: BundleAction.AUTHORIZE,
            Concern.RECOVERY: BundleAction.RECOVER,
            Concern.SCALE: BundleAction.MEASURE,
            Concern.EVIDENCE: BundleAction.MEASURE,
            Concern.STABILIZE: BundleAction.STABILIZE,
        }[concern]
        reason = "canonical bundle concern routed to existing authority"

    return ExecutionIntent(spec.slug, concern, action, spec.anchor, cell_id, epoch, reason)


def issue_keys(bundle: BundleSpec) -> tuple[int, ...]:
    bundle.validate()
    return tuple(range(bundle.issue_start, bundle.issue_end + 1))
