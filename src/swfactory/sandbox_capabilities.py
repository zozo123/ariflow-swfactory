"""Typed sandbox capabilities and lineage negotiation.

Provider names are never treated as capability claims. Callers must negotiate explicitly.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class SandboxCapabilities:
    create: bool = True
    attach: bool = False
    snapshot: bool = False
    fork: bool = False
    pause_resume: bool = False
    network_policy: bool = False
    filesystem_isolation: bool = True
    ttl: bool = False
    exact_teardown: bool = True

    def require(self, *names: str) -> None:
        missing = [name for name in names if not getattr(self, name, False)]
        if missing:
            raise UnsupportedSandboxCapability(", ".join(missing))

    def to_dict(self) -> dict[str, bool]:
        return asdict(self)


class UnsupportedSandboxCapability(RuntimeError):
    pass


@dataclass(frozen=True)
class SandboxLineage:
    cell_id: str
    epoch: int
    incarnation_id: str
    provider: str
    parent_incarnation_id: str | None = None
    parent_commit: str | None = None
    policy_digest: str | None = None
    artifact_digests: tuple[str, ...] = field(default_factory=tuple)

    def child(self, *, incarnation_id: str, provider: str | None = None) -> "SandboxLineage":
        return SandboxLineage(
            cell_id=self.cell_id,
            epoch=self.epoch,
            incarnation_id=incarnation_id,
            provider=provider or self.provider,
            parent_incarnation_id=self.incarnation_id,
            parent_commit=self.parent_commit,
            policy_digest=self.policy_digest,
            artifact_digests=self.artifact_digests,
        )


def negotiate(
    advertised: SandboxCapabilities,
    *,
    need_attach: bool = False,
    need_snapshot: bool = False,
    need_fork: bool = False,
    need_pause_resume: bool = False,
    need_network_policy: bool = False,
) -> SandboxCapabilities:
    required = []
    if need_attach:
        required.append("attach")
    if need_snapshot:
        required.append("snapshot")
    if need_fork:
        required.append("fork")
    if need_pause_resume:
        required.append("pause_resume")
    if need_network_policy:
        required.append("network_policy")
    advertised.require(*required)
    return advertised


def capability_document(provider: str, caps: SandboxCapabilities, **metadata: Any) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "provider": provider,
        "capabilities": caps.to_dict(),
        "metadata": metadata,
    }
