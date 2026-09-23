"""Trusted-backend adapter contract for managed population search.

Population manifests describe which trajectory to run. An invocation binds that trajectory to the
exact bounded instruction/context being sent to a provider. Adapters execute only inside the trusted
backend process; raw credential material is passed as an ephemeral argument and is never part of an
invocation, artifact, receipt, log document, or digest.

This module intentionally does not schedule Airflow and carries no promotion authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from swfactory.population_manifest import PopulationManifestError
from swfactory.provider_binding import BoundPopulationTask

POPULATION_ADAPTER_SCHEMA_VERSION = 1
POPULATION_ADAPTER_AUTHORITY = "search-only"
MAX_INSTRUCTION_BYTES = 256 * 1024
MAX_ADAPTER_RESPONSE_BYTES = 4 * 1024 * 1024


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _require_sha256(value: str, *, field: str) -> None:
    raw = value.removeprefix("sha256:")
    if len(raw) != 64 or any(char not in "0123456789abcdef" for char in raw):
        raise PopulationManifestError(f"{field} must be a canonical sha256 digest")


@dataclass(frozen=True)
class PopulationInvocation:
    """Exact provider input for one already-bound population task."""

    task: BoundPopulationTask
    population_manifest_digest: str
    provider_binding_digest: str
    instruction: str
    context_artifact_digests: tuple[str, ...] = ()
    objective_digest: str | None = None
    authority: str = POPULATION_ADAPTER_AUTHORITY
    schema_version: int = POPULATION_ADAPTER_SCHEMA_VERSION

    def validate(self) -> None:
        self.task.validate()
        if self.authority != POPULATION_ADAPTER_AUTHORITY:
            raise PopulationManifestError("population invocation must remain search-only")
        if self.schema_version != POPULATION_ADAPTER_SCHEMA_VERSION:
            raise PopulationManifestError("unsupported population invocation schema")
        _require_sha256(self.population_manifest_digest, field="population_manifest_digest")
        _require_sha256(self.provider_binding_digest, field="provider_binding_digest")
        if not self.instruction.strip():
            raise PopulationManifestError("population invocation instruction must be nonempty")
        if len(self.instruction.encode()) > MAX_INSTRUCTION_BYTES:
            raise PopulationManifestError("population invocation instruction exceeds size limit")
        for digest in self.context_artifact_digests:
            _require_sha256(digest, field="context_artifact_digest")
        if self.objective_digest is not None:
            _require_sha256(self.objective_digest, field="objective_digest")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "task": self.task.canonical_dict() | {"binding_digest": self.task.binding_digest},
            "population_manifest_digest": self.population_manifest_digest,
            "provider_binding_digest": self.provider_binding_digest,
            "instruction": self.instruction,
            "context_artifact_digests": list(self.context_artifact_digests),
            "objective_digest": self.objective_digest,
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


@dataclass(frozen=True)
class PopulationAdapterResult:
    """Sanitized provider result before host-owned artifact retention."""

    output: str
    behavior_signature: tuple[str, ...]
    evidence_digest: str | None = None
    cost_usd: float = 0.0
    duration_s: float = 0.0
    state: str = "answered"

    def validate(self) -> None:
        if self.state not in {"answered", "failed", "cancelled", "refused"}:
            raise PopulationManifestError(f"unknown population adapter state {self.state!r}")
        if self.state == "answered" and not self.output:
            raise PopulationManifestError("answered provider result must contain output")
        if self.state == "answered" and not self.behavior_signature:
            raise PopulationManifestError("answered provider result needs a behavior signature")
        if self.evidence_digest is not None:
            _require_sha256(self.evidence_digest, field="evidence_digest")
        if not math.isfinite(self.cost_usd) or self.cost_usd < 0.0:
            raise PopulationManifestError("provider result cost must be finite and non-negative")
        if not math.isfinite(self.duration_s) or self.duration_s < 0.0:
            raise PopulationManifestError("provider result duration must be finite and non-negative")


class PopulationAdapter(Protocol):
    provider: str
    credential_capability: str | None
    credential_env: str | None

    def execute(
        self,
        invocation: PopulationInvocation,
        *,
        credential: str | None,
    ) -> PopulationAdapterResult: ...


@dataclass(frozen=True)
class HttpPopulationAdapterConfig:
    """Configuration contains secret names/capabilities, never secret values."""

    provider: str
    endpoint: str
    credential_capability: str | None = None
    credential_env: str | None = None
    timeout_s: float = 120.0
    max_response_bytes: int = MAX_ADAPTER_RESPONSE_BYTES
    auth_header: str = "Authorization"
    auth_prefix: str = "Bearer "

    def validate(self) -> None:
        if not self.provider or len(self.provider) > 128:
            raise PopulationManifestError("population adapter provider must be nonempty and bounded")
        parsed = urllib.parse.urlsplit(self.endpoint)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname:
            raise PopulationManifestError("population adapter endpoint must be absolute HTTP(S)")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise PopulationManifestError("population adapter endpoint cannot contain credentials, query, or fragment")
        if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise PopulationManifestError("plaintext population adapter endpoints are loopback-only")
        if self.credential_capability is None:
            if self.credential_env is not None:
                raise PopulationManifestError("credential_env requires credential_capability")
        else:
            capability = self.credential_capability.strip()
            if "." not in capability or capability.casefold() in {"github", "all", "any"}:
                raise PopulationManifestError("population adapter credential capability must be explicit")
            if not self.credential_env or not self.credential_env.strip():
                raise PopulationManifestError("credentialed population adapter requires a credential env name")
        if not math.isfinite(self.timeout_s) or not 0.1 <= self.timeout_s <= 600.0:
            raise PopulationManifestError("population adapter timeout must be in [0.1, 600] seconds")
        if not 1 <= self.max_response_bytes <= MAX_ADAPTER_RESPONSE_BYTES:
            raise PopulationManifestError("population adapter response limit is invalid")
        if not self.auth_header.strip() or "\n" in self.auth_header or "\r" in self.auth_header:
            raise PopulationManifestError("population adapter auth header is invalid")
        if "\n" in self.auth_prefix or "\r" in self.auth_prefix:
            raise PopulationManifestError("population adapter auth prefix is invalid")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "provider": self.provider,
            "endpoint": self.endpoint,
            "credential_capability": self.credential_capability,
            "credential_env": self.credential_env,
            "timeout_s": self.timeout_s,
            "max_response_bytes": self.max_response_bytes,
            "auth_header": self.auth_header,
            "auth_prefix": self.auth_prefix,
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, _req, _fp, _code, _msg, _headers, _newurl):
        return None


class HttpPopulationAdapter:
    """Small JSON adapter for a trusted provider gateway."""

    def __init__(
        self,
        config: HttpPopulationAdapterConfig,
        *,
        opener=None,
    ) -> None:
        config.validate()
        self.config = config
        self.provider = config.provider
        self.credential_capability = config.credential_capability
        self.credential_env = config.credential_env
        self._open = opener or urllib.request.build_opener(_NoRedirect()).open

    def execute(
        self,
        invocation: PopulationInvocation,
        *,
        credential: str | None,
    ) -> PopulationAdapterResult:
        invocation.validate()
        if invocation.task.provider != self.provider:
            raise PopulationManifestError(
                f"population invocation provider {invocation.task.provider!r} does not match adapter {self.provider!r}"
            )
        if self.credential_capability is None and credential is not None:
            raise PopulationManifestError("credential supplied to an uncredentialed population adapter")
        if self.credential_capability is not None and not credential:
            raise PopulationManifestError("credentialed population adapter received no credential")

        payload = json.dumps(
            {
                "schema_version": POPULATION_ADAPTER_SCHEMA_VERSION,
                "authority": POPULATION_ADAPTER_AUTHORITY,
                "invocation": invocation.canonical_dict(),
                "invocation_digest": invocation.digest(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if credential is not None:
            headers[self.config.auth_header] = self.config.auth_prefix + credential
        request = urllib.request.Request(
            self.config.endpoint,
            data=payload,
            headers=headers,
            method="POST",
        )
        try:
            response = self._open(request, timeout=self.config.timeout_s)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            status = int(response.code)
            raw = response.read(self.config.max_response_bytes + 1)
        if len(raw) > self.config.max_response_bytes:
            raise PopulationProviderError("population adapter response exceeds configured limit")
        if status >= 300:
            raise PopulationProviderError(f"population adapter returned HTTP {status}")
        try:
            document = json.loads(raw)
        except ValueError as error:
            raise PopulationProviderError("population adapter returned invalid JSON") from error
        if not isinstance(document, dict):
            raise PopulationProviderError("population adapter response must be an object")
        allowed = {
            "output",
            "behavior_signature",
            "evidence_digest",
            "cost_usd",
            "duration_s",
            "state",
        }
        unknown = sorted(set(document) - allowed)
        if unknown:
            raise PopulationProviderError("population adapter returned unsupported fields: " + ", ".join(unknown))
        signature = document.get("behavior_signature", ())
        if not isinstance(signature, list) or any(not isinstance(value, str) for value in signature):
            raise PopulationProviderError("population adapter behavior_signature must be an array of strings")
        result = PopulationAdapterResult(
            output=str(document.get("output") or ""),
            behavior_signature=tuple(signature),
            evidence_digest=(str(document["evidence_digest"]) if document.get("evidence_digest") is not None else None),
            cost_usd=float(document.get("cost_usd", 0.0)),
            duration_s=float(document.get("duration_s", 0.0)),
            state=str(document.get("state", "answered")),
        )
        try:
            result.validate()
        except PopulationManifestError as error:
            raise PopulationProviderError(f"population adapter returned an invalid result: {error}") from error
        return result


def population_adapter_identity(adapter: PopulationAdapter) -> str:
    """Bind operation identity to the concrete trusted adapter configuration."""

    config = getattr(adapter, "config", None)
    if isinstance(config, HttpPopulationAdapterConfig):
        return config.digest()
    return _digest(
        {
            "provider": adapter.provider,
            "credential_capability": adapter.credential_capability,
            "implementation": f"{type(adapter).__module__}.{type(adapter).__qualname__}",
        }
    )


class PopulationProviderError(OSError):
    """A provider request was attempted but its usable outcome is not safely known."""


class PopulationArtifactStore:
    """Host-owned content-addressed provider output retention."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def retain(self, *, invocation_digest: str, output: str) -> tuple[str, Path]:
        _require_sha256(invocation_digest, field="invocation_digest")
        raw = output.encode()
        output_digest = "sha256:" + hashlib.sha256(raw).hexdigest()
        artifact_digest = _digest(
            {
                "invocation_digest": invocation_digest,
                "output_sha256": output_digest,
            }
        )
        path = self.root / (artifact_digest.removeprefix("sha256:") + ".json")
        document = {
            "schema_version": POPULATION_ADAPTER_SCHEMA_VERSION,
            "authority": POPULATION_ADAPTER_AUTHORITY,
            "artifact_digest": artifact_digest,
            "invocation_digest": invocation_digest,
            "output_sha256": output_digest,
            "output": output,
        }
        encoded = json.dumps(document, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing != document:
                raise PopulationManifestError("population artifact digest collision or corruption")
        else:
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(encoded, encoding="utf-8")
            tmp.replace(path)
        return artifact_digest, path

    def read(self, digest: str) -> dict[str, Any]:
        _require_sha256(digest, field="artifact_digest")
        path = self.root / (digest.removeprefix("sha256:") + ".json")
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("artifact_digest") != digest:
            raise PopulationManifestError("population artifact document is corrupt")
        output = raw.get("output")
        invocation_digest = raw.get("invocation_digest")
        output_digest = raw.get("output_sha256")
        if not isinstance(output, str) or not isinstance(invocation_digest, str) or not isinstance(output_digest, str):
            raise PopulationManifestError("population artifact document is invalid")
        actual_output = "sha256:" + hashlib.sha256(output.encode()).hexdigest()
        if actual_output != output_digest:
            raise PopulationManifestError("population artifact bytes do not match output digest")
        actual_artifact = _digest(
            {
                "invocation_digest": invocation_digest,
                "output_sha256": output_digest,
            }
        )
        if actual_artifact != digest:
            raise PopulationManifestError("population artifact provenance does not match digest")
        return raw


def http_population_adapters_from_document(
    document: Mapping[str, Any],
) -> dict[str, HttpPopulationAdapter]:
    """Parse backend adapter config containing only endpoints and secret names."""

    adapters: dict[str, HttpPopulationAdapter] = {}
    for provider, raw in document.items():
        if not isinstance(raw, Mapping):
            raise PopulationManifestError(f"population adapter {provider!r} must be an object")
        allowed = {
            "endpoint",
            "credential_capability",
            "credential_env",
            "timeout_s",
            "max_response_bytes",
            "auth_header",
            "auth_prefix",
        }
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise PopulationManifestError(f"population adapter {provider!r} has unknown fields: {', '.join(unknown)}")
        config = HttpPopulationAdapterConfig(
            provider=str(provider),
            endpoint=str(raw["endpoint"]),
            credential_capability=(
                str(raw["credential_capability"]) if raw.get("credential_capability") is not None else None
            ),
            credential_env=(str(raw["credential_env"]) if raw.get("credential_env") is not None else None),
            timeout_s=float(raw.get("timeout_s", 120.0)),
            max_response_bytes=int(raw.get("max_response_bytes", MAX_ADAPTER_RESPONSE_BYTES)),
            auth_header=str(raw.get("auth_header", "Authorization")),
            auth_prefix=str(raw.get("auth_prefix", "Bearer ")),
        )
        config.validate()
        if config.provider in adapters:
            raise PopulationManifestError(f"duplicate population adapter {config.provider!r}")
        adapters[config.provider] = HttpPopulationAdapter(config)
    return adapters
