"""Canonical trust-zone, mutation-envelope and redaction contracts.

Security metadata is data, not prose: policy digests are canonical, credentials have audiences and
expiry, every external mutation carries the same envelope, and durable evidence uses one redaction
vocabulary.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

POLICY_SCHEMA_VERSION = 1
POLICY_DIGEST_FAMILY = f"v{POLICY_SCHEMA_VERSION}"
POLICY_DIGEST_PREFIX = f"policy:{POLICY_DIGEST_FAMILY}:"
MUTATION_SCHEMA_VERSION = 1
REDACTION_SCHEMA_VERSION = 1
REDACTED = "[REDACTED]"

_SECRET_KEY = re.compile(
    r"(?:token|secret|password|passwd|api[_-]?key|access[_-]?key|private[_-]?key|credential|authorization)",
    re.IGNORECASE,
)
_TOKEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]{8,}", re.IGNORECASE),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bapikey_[A-Za-z0-9]{8,}_[A-Za-z0-9_-]{32,}\b", re.IGNORECASE),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
)


@dataclass(frozen=True)
class CanonicalPolicy:
    repo: str
    target: str
    protected_paths: tuple[str, ...] = ()
    allowed_domains: tuple[str, ...] = ()
    sandbox_provider: str | None = None
    sandbox_credentials: tuple[str, ...] = ()
    publication_backend_only: bool = True
    metadata: tuple[tuple[str, str], ...] = ()
    schema_version: int = POLICY_SCHEMA_VERSION

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "repo": self.repo.strip(),
            "target": self.target.strip(),
            "protected_paths": sorted(set(path.strip() for path in self.protected_paths if path.strip())),
            "allowed_domains": sorted(set(domain.strip().lower() for domain in self.allowed_domains if domain.strip())),
            "sandbox_provider": self.sandbox_provider,
            "sandbox_credentials": sorted(set(self.sandbox_credentials)),
            "publication_backend_only": self.publication_backend_only,
            "metadata": {key: value for key, value in sorted(self.metadata)},
        }

    @classmethod
    def for_factory_job(cls, line_name: str, job: Mapping[str, Any]) -> CanonicalPolicy:
        """Project one scheduled factory job into the single Cell policy coordinate system.

        Issue identity is deliberately absent: the Factory Cell id already binds issue x repo x
        target. Policy describes what authority that Cell may exercise at its epoch, not which
        issue caused the work.
        """

        line = str(line_name).strip()
        repo = str(job.get("repo", "")).strip()
        directory = str(job.get("dir", "")).strip() or "."
        base_branch = str(job.get("base_branch", "main")).strip() or "main"
        sandbox = str(job.get("sandbox", "configured")).strip() or "configured"
        if not line:
            raise ValueError("factory policy line must be nonempty")
        if not repo:
            raise ValueError("factory policy repo must be nonempty")
        return cls(
            repo=repo,
            target=f"{directory}@{base_branch}",
            sandbox_provider=sandbox,
            metadata=(("line", line),),
        )

    def digest(self) -> str:
        return policy_digest_for_mapping(self.canonical_dict())


@dataclass(frozen=True)
class MutationEnvelope:
    cell_id: str
    epoch: int
    operation_key: str
    policy_digest: str
    trace_id: str
    actor: str
    schema_version: int = MUTATION_SCHEMA_VERSION

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_") or len(self.cell_id) != 29:
            raise ValueError("invalid Factory Cell id")
        if self.epoch < 1:
            raise ValueError("mutation epoch must be positive")
        if not self.operation_key or len(self.operation_key) > 256:
            raise ValueError("operation key must be nonempty and bounded")
        require_current_policy_digest(self.policy_digest)
        if not self.trace_id or len(self.trace_id) > 128:
            raise ValueError("trace id must be nonempty and bounded")
        if not self.actor.strip() or len(self.actor) > 128:
            raise ValueError("actor must be nonempty and bounded")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class CredentialGrant:
    name: str
    audience: str
    purpose: str
    expires_at: float | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def validate_for(self, audience: str, *, now: float | None = None) -> None:
        now = time.time() if now is None else now
        if self.audience != audience:
            raise PermissionError(f"credential {self.name!r} is scoped to {self.audience!r}, not {audience!r}")
        if self.expires_at is not None and self.expires_at <= now:
            raise PermissionError(f"credential {self.name!r} has expired")


@dataclass(frozen=True)
class EnvironmentProof:
    allowed_names: tuple[str, ...]
    scrubbed_names: tuple[str, ...]
    audience: str
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "audience": self.audience,
            "allowed_names": sorted(set(self.allowed_names)),
            "scrubbed_names": sorted(set(self.scrubbed_names)),
        }


def environment_proof(
    source: Mapping[str, str],
    allowed_names: Iterable[str],
    *,
    audience: str,
) -> EnvironmentProof:
    allowed = {name for name in allowed_names if name in source}
    scrubbed = {name for name in source if looks_secret_key(name) and name not in allowed}
    return EnvironmentProof(tuple(sorted(allowed)), tuple(sorted(scrubbed)), audience)


def looks_secret_key(key: str) -> bool:
    return bool(_SECRET_KEY.search(key)) or key.upper().endswith(("_TOKEN", "_SECRET", "_PASSWORD"))


def redact_text(text: str) -> str:
    out = text
    for pattern in _TOKEN_PATTERNS:
        out = pattern.sub(REDACTED, out)
    return out


def redact(value: Any, *, key: str | None = None) -> Any:
    """Recursively redact structured data while preserving shape for debugging."""
    if key is not None and looks_secret_key(key):
        return REDACTED
    if isinstance(value, Mapping):
        return {str(k): redact(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    if isinstance(value, str):
        return redact_text(value)
    return value


def redaction_document(value: Any) -> dict[str, Any]:
    return {
        "schema_version": REDACTION_SCHEMA_VERSION,
        "value": redact(value),
    }


def assert_publication_credentials_backend_only(
    sandbox_environment: Mapping[str, str],
    *,
    publication_names: Iterable[str] = ("GH_TOKEN", "GITHUB_TOKEN"),
) -> None:
    leaked = sorted(name for name in publication_names if name in sandbox_environment)
    if leaked:
        raise PermissionError("publication credentials crossed into sandbox trust zone: " + ", ".join(leaked))


def policy_digest_for_mapping(policy: Mapping[str, Any]) -> str:
    """Return the only current policy digest family.

    The version is in both the visible family marker and the hashed domain separator. The marker
    makes legacy rows distinguishable without trusting the bytes they contain; the domain separator
    prevents a future family from accidentally reusing a v1 digest.
    """

    payload = json.dumps(
        _canonical_value(policy),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    digest = hashlib.sha256(f"{POLICY_DIGEST_FAMILY}\0{payload}".encode()).hexdigest()
    return POLICY_DIGEST_PREFIX + digest


def policy_digest_family(digest: str) -> int:
    """Return 0 for the unmarked legacy family, or the explicit current schema version."""

    if re.fullmatch(r"policy:[0-9a-f]{64}", digest):
        return 0
    match = re.fullmatch(r"policy:v([1-9][0-9]*):[0-9a-f]{64}", digest)
    if match is None:
        raise ValueError("invalid Factory Cell policy digest")
    return int(match.group(1))


def require_current_policy_digest(digest: str) -> None:
    """Refuse legacy/future policy authority at a mutation boundary."""

    family = policy_digest_family(digest)
    if family != POLICY_SCHEMA_VERSION:
        raise ValueError(
            f"mutation requires policy digest family v{POLICY_SCHEMA_VERSION}; "
            f"got {'legacy' if family == 0 else f'v{family}'}; reactivate the Cell at a new epoch"
        )


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, set):
        normalized = [_canonical_value(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True))
    return value
