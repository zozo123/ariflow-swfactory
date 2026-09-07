"""Rough least-privilege implementation for Liquid400 trust and tenant-isolation domains."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Capability(StrEnum):
    READ_SOURCE = "read_source"
    WRITE_WORKSPACE = "write_workspace"
    READ_CACHE = "read_cache"
    WRITE_CACHE = "write_cache"
    USE_MODEL = "use_model"
    PUBLISH_GIT = "publish_git"
    READ_SECRET = "read_secret"
    ADMIN_REPAIR = "admin_repair"


@dataclass(frozen=True)
class SecurityContext:
    tenant: str
    cell_id: str
    epoch: int
    role: str
    capabilities: frozenset[Capability]
    secret_scopes: frozenset[str] = frozenset()

    def validate(self) -> None:
        if not self.tenant.strip():
            raise ValueError("tenant is required")
        if not self.cell_id.startswith("cell_"):
            raise ValueError("invalid cell id")
        if self.epoch < 1:
            raise ValueError("epoch must be positive")


def authorize(
    context: SecurityContext,
    *,
    capability: Capability,
    tenant: str,
    secret_scope: str | None = None,
) -> bool:
    context.validate()
    if tenant != context.tenant:
        return False
    if capability not in context.capabilities:
        return False
    if capability is Capability.READ_SECRET:
        return secret_scope is not None and secret_scope in context.secret_scopes
    return True


def sandbox_environment(context: SecurityContext) -> dict[str, str]:
    """Return non-secret identity only; secret material must be brokered per operation."""
    context.validate()
    return {
        "SWF_TENANT": context.tenant,
        "SWF_CELL_ID": context.cell_id,
        "SWF_CELL_EPOCH": str(context.epoch),
        "SWF_ROLE": context.role,
    }


def policy_fingerprint(*, version: str, capabilities: frozenset[Capability], scopes: frozenset[str]) -> str:
    import hashlib

    material = "\0".join([version, *(sorted(item.value for item in capabilities)), *(sorted(scopes))]).encode()
    return hashlib.sha256(material).hexdigest()


def detect_policy_drift(*, expected_digest: str, observed_digest: str) -> str:
    return "ok" if expected_digest == observed_digest else "refuse_and_reconcile"
