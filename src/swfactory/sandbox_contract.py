"""Provider-neutral sandbox contract used by runtime selection and conformance.

Capabilities are explicit facts supplied by adapter code.  Provider names are labels only and are
never used by callers as a proxy for behavior.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any, Literal

from swfactory.sandbox_capabilities import SandboxCapabilities, SandboxLineage

TerminationReason = Literal[
    "completed",
    "command_failed",
    "timeout",
    "cancelled",
    "provider_terminated",
    "infrastructure_lost",
]


@dataclass(frozen=True)
class CapabilityRequirement:
    attach: bool = False
    snapshot: bool = False
    fork: bool = False
    pause_resume: bool = False
    network_policy: bool = False
    filesystem_isolation: bool = True
    ttl: bool = False
    exact_teardown: bool = True

    def missing(self, caps: SandboxCapabilities) -> tuple[str, ...]:
        return tuple(name for name, needed in asdict(self).items() if needed and not bool(getattr(caps, name, False)))


@dataclass(frozen=True)
class ProviderDocument:
    provider: str
    capabilities: SandboxCapabilities
    implementation: str
    schema_version: int = 1
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "provider": self.provider,
            "implementation": self.implementation,
            "capabilities": self.capabilities.to_dict(),
            "notes": list(self.notes),
        }

    def digest(self) -> str:
        raw = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class NetworkPolicyEvidence:
    requested_domains: tuple[str, ...]
    enforcement: Literal["native", "host_runtime", "none"]
    observed_domains: tuple[str, ...] = ()
    detail: str | None = None


@dataclass(frozen=True)
class ProviderResult:
    reason: TerminationReason
    exit_code: int | None
    timed_out: bool = False
    provider_terminated: bool = False
    detail: str | None = None

    @property
    def ok(self) -> bool:
        return self.reason == "completed" and self.exit_code == 0


def local_document() -> ProviderDocument:
    return ProviderDocument(
        provider="local",
        implementation="LocalSandbox",
        capabilities=SandboxCapabilities(
            create=True,
            attach=True,
            snapshot=False,
            fork=False,
            pause_resume=False,
            network_policy=False,
            filesystem_isolation=False,
            ttl=False,
            exact_teardown=True,
        ),
        notes=("development host directory; not an isolation boundary",),
    )


def srt_document() -> ProviderDocument:
    return ProviderDocument(
        provider="srt",
        implementation="SrtSandbox",
        capabilities=SandboxCapabilities(
            create=True,
            attach=True,
            snapshot=False,
            fork=False,
            pause_resume=False,
            network_policy=True,
            filesystem_isolation=True,
            ttl=False,
            exact_teardown=True,
        ),
        notes=("host directory with Sandbox Runtime filesystem/network confinement",),
    )


def docker_document() -> ProviderDocument:
    return ProviderDocument(
        provider="docker",
        implementation="DockerSandbox",
        capabilities=SandboxCapabilities(
            create=True,
            attach=True,
            snapshot=False,
            fork=False,
            pause_resume=False,
            network_policy=True,
            filesystem_isolation=True,
            ttl=False,
            exact_teardown=True,
        ),
        notes=("container isolation shares the host kernel",),
    )


def toolset_document(backend: str, *, attach: bool = True) -> ProviderDocument:
    return ProviderDocument(
        provider=f"toolset:{backend}",
        implementation="ToolsetSandbox",
        capabilities=SandboxCapabilities(
            create=True,
            attach=attach,
            snapshot=False,
            fork=False,
            pause_resume=False,
            network_policy=True,
            filesystem_isolation=True,
            ttl=True,
            exact_teardown=True,
        ),
        notes=("optional backend features remain false until conformance proves them",),
    )


def islo_document(*, snapshot: bool = False, fork: bool = False) -> ProviderDocument:
    return ProviderDocument(
        provider="islo",
        implementation="IsloSandbox",
        capabilities=SandboxCapabilities(
            create=True,
            attach=True,
            snapshot=snapshot,
            fork=fork,
            pause_resume=True,
            network_policy=True,
            filesystem_isolation=True,
            ttl=True,
            exact_teardown=True,
        ),
        notes=(
            "pause/resume and TTL are native",
            "snapshot/fork are advertised only when the configured provider contract enables them",
        ),
    )


def provider_documents(*, islo_snapshot: bool = False, islo_fork: bool = False) -> tuple[ProviderDocument, ...]:
    return (
        local_document(),
        srt_document(),
        docker_document(),
        toolset_document("configured"),
        islo_document(snapshot=islo_snapshot, fork=islo_fork),
    )


def select_provider(
    documents: Iterable[ProviderDocument],
    requirement: CapabilityRequirement,
    *,
    preferred: Iterable[str] = (),
) -> ProviderDocument:
    docs = tuple(documents)
    preferred_order = {name: index for index, name in enumerate(preferred)}
    compatible = [doc for doc in docs if not requirement.missing(doc.capabilities)]
    if not compatible:
        detail = {
            doc.provider: requirement.missing(doc.capabilities) for doc in sorted(docs, key=lambda item: item.provider)
        }
        raise ValueError(f"no sandbox provider satisfies capability requirement: {detail}")
    return min(
        compatible,
        key=lambda doc: (preferred_order.get(doc.provider, len(preferred_order)), doc.provider),
    )


def validate_lineage(
    lineage: SandboxLineage,
    *,
    cell_id: str,
    epoch: int,
    policy_digest: str | None,
) -> None:
    if lineage.cell_id != cell_id:
        raise ValueError(f"compute belongs to {lineage.cell_id}, not {cell_id}")
    if lineage.epoch != epoch:
        raise ValueError(f"stale compute epoch {lineage.epoch}; current epoch is {epoch}")
    if policy_digest is not None and lineage.policy_digest != policy_digest:
        raise ValueError("compute policy digest does not match the current Factory Cell")


def normalize_result(
    *,
    exit_code: int | None,
    timed_out: bool = False,
    cancelled: bool = False,
    provider_terminated: bool = False,
    infrastructure_lost: bool = False,
    detail: str | None = None,
) -> ProviderResult:
    if cancelled:
        reason: TerminationReason = "cancelled"
    elif timed_out:
        reason = "timeout"
    elif provider_terminated:
        reason = "provider_terminated"
    elif infrastructure_lost:
        reason = "infrastructure_lost"
    elif exit_code == 0:
        reason = "completed"
    else:
        reason = "command_failed"
    return ProviderResult(
        reason=reason,
        exit_code=exit_code,
        timed_out=timed_out,
        provider_terminated=provider_terminated,
        detail=detail,
    )
