"""Provider-neutral sandbox contract used by runtime selection and conformance.

Capabilities are explicit facts supplied by adapter code.  Provider names are labels only and are
never used by callers as a proxy for behavior.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
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

    def to_dict(self) -> dict[str, bool]:
        return asdict(self)


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


def _doc(provider: str, implementation: str, notes: tuple[str, ...], **caps: bool) -> ProviderDocument:
    return ProviderDocument(provider, SandboxCapabilities(attach=True, **caps), implementation, notes=notes)


def local_document() -> ProviderDocument:
    notes = ("development host directory; not an isolation boundary",)
    return _doc("local", "LocalSandbox", notes, filesystem_isolation=False)


def srt_document() -> ProviderDocument:
    notes = ("host directory with Sandbox Runtime filesystem/network confinement",)
    return _doc("srt", "SrtSandbox", notes, network_policy=True)


def docker_document() -> ProviderDocument:
    notes = ("container isolation shares the host kernel",)
    return _doc("docker", "DockerSandbox", notes, network_policy=True)


def toolset_document(backend: str) -> ProviderDocument:
    notes = ("optional backend features, including TTL, remain false until conformance proves them",)
    return _doc(f"toolset:{backend}", "ToolsetSandbox", notes, network_policy=True)


def islo_document(*, snapshot: bool = False, fork: bool = False) -> ProviderDocument:
    notes = (
        "pause/resume and TTL are native",
        "snapshot/fork are advertised only when the configured provider contract enables them",
    )
    return _doc(
        "islo",
        "IsloSandbox",
        notes,
        snapshot=snapshot,
        fork=fork,
        pause_resume=True,
        network_policy=True,
        ttl=True,
    )


def boat_document() -> ProviderDocument:
    """boat.dev (formerly Box by ASCII): the first provider to demonstrate fork for this factory.

    Every capability below was observed against a live account rather than read off a product page,
    because `workgraph.provider-fork` has sat experimental for exactly as long as nobody could show
    the invariant holding somewhere real:

        create   `boat new --ttl 1800` -> state ready
        snapshot `boat stop` -> snapshotAvailable true, snapshotCompletedAt stamped
        fork     `boat fork` twice off ONE snapshot -> two independent sandboxes
        lineage  both forks read the parent's file: immutable parent identity
        isolation each fork's own write is invisible to its sibling
        teardown `boat delete` -> both gone from `boat list`

    `network_policy` is False and stays False. The CLI exposes `--no-env` and per-sandbox
    environment control, but egress policy was NOT exercised here, and a capability document is the
    one place a guess is indistinguishable from a measurement.
    """
    notes = (
        "fork observed: two forks from one snapshot inherited parent state and stayed isolated",
        "egress policy unexercised; network_policy is not claimed",
    )
    return _doc("boat", "BoatSandbox", notes, snapshot=True, fork=True, pause_resume=True, ttl=True)


def provider_documents(*, islo_fork: bool = False) -> tuple[ProviderDocument, ...]:
    return (
        local_document(),
        srt_document(),
        docker_document(),
        toolset_document("configured"),
        islo_document(fork=islo_fork),
        boat_document(),
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
