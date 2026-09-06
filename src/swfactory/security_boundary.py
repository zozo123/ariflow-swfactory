"""Trust-zone policy and secret-redaction primitives for multi-repository operation."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable

_SECRET_KEYS = re.compile(r"(?i)(token|secret|password|authorization|cookie|private[_-]?key)")


@dataclass(frozen=True)
class WorkPolicy:
    repo: str
    allowed_paths: tuple[str, ...]
    denied_paths: tuple[str, ...] = ()
    allowed_hosts: tuple[str, ...] = ()
    publication_allowed: bool = True

    def digest(self) -> str:
        raw = "\0".join(
            (self.repo, *sorted(self.allowed_paths), "--deny--", *sorted(self.denied_paths), "--hosts--", *sorted(self.allowed_hosts))
        ).encode()
        return hashlib.sha256(raw).hexdigest()

    def allows_path(self, path: str) -> bool:
        normalized = path.replace("\\", "/").lstrip("./")
        if any(normalized == d or normalized.startswith(d.rstrip("/") + "/") for d in self.denied_paths):
            return False
        return any(normalized == a or normalized.startswith(a.rstrip("/") + "/") for a in self.allowed_paths)


@dataclass(frozen=True)
class CredentialEnvelope:
    name: str
    scope: str
    audience: str
    secret: str

    def exportable_to(self, zone: str) -> bool:
        return self.audience == zone


def sandbox_environment(credentials: Iterable[CredentialEnvelope], base: dict[str, str]) -> dict[str, str]:
    """Return a sandbox env that excludes control-plane/publication credentials."""
    out = dict(base)
    for cred in credentials:
        if cred.exportable_to("sandbox"):
            out[cred.name] = cred.secret
    return out


def redact(value):
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _SECRET_KEYS.search(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    if isinstance(value, str):
        value = re.sub(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", value)
        value = re.sub(r"gh[pousr]_[A-Za-z0-9_]{20,}", "[REDACTED_GITHUB_TOKEN]", value)
    return value


def mutation_identity(cell_id: str, epoch: int, policy: WorkPolicy) -> dict[str, str | int]:
    return {"cell_id": cell_id, "epoch": epoch, "repo": policy.repo, "policy_digest": policy.digest()}
