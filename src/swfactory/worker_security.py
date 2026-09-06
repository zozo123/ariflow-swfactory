"""Least-privilege trust contract for the seven bounded worker roles."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

_SECRET = re.compile(r"(?i)(token|secret|password|authorization|cookie|private[_-]?key)")


class TrustZone(StrEnum):
    CONTROL = "control"
    SANDBOX = "sandbox"
    PUBLICATION = "publication"
    OPERATOR = "operator"


@dataclass(frozen=True)
class WorkerPolicy:
    role: str
    zone: TrustZone
    allowed_resources: tuple[str, ...]
    allowed_mutations: tuple[str, ...]
    secret_classes: tuple[str, ...] = ()

    def digest(self) -> str:
        payload = json.dumps(
            asdict(self),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return "worker-policy:" + hashlib.sha256(payload).hexdigest()


_POLICIES = {
    "authority": WorkerPolicy(
        "authority",
        TrustZone.CONTROL,
        ("cell", "operation", "generation"),
        ("validate", "fence"),
    ),
    "airflow": WorkerPolicy(
        "airflow",
        TrustZone.CONTROL,
        ("cell", "airflow_run"),
        ("bind", "observe"),
        ("airflow",),
    ),
    "workgraph": WorkerPolicy(
        "workgraph",
        TrustZone.SANDBOX,
        ("plan", "workspace", "artifact"),
        ("execute", "merge"),
        ("sandbox",),
    ),
    "recovery": WorkerPolicy(
        "recovery",
        TrustZone.CONTROL,
        ("cell", "operation", "cleanup"),
        ("observe", "retry", "repair"),
    ),
    "security": WorkerPolicy(
        "security",
        TrustZone.CONTROL,
        ("policy", "credential_metadata", "evidence"),
        ("validate", "redact"),
    ),
    "evidence": WorkerPolicy(
        "evidence",
        TrustZone.CONTROL,
        ("cell", "operation", "artifact", "metric"),
        ("append", "seal"),
    ),
    "operator": WorkerPolicy(
        "operator",
        TrustZone.OPERATOR,
        ("cell", "queue", "operation", "fleet", "evidence"),
        ("read",),
    ),
}


class WorkerPolicyViolation(PermissionError):
    """A worker attempted an operation outside its role contract."""


def policy_for(role: str) -> WorkerPolicy:
    try:
        return _POLICIES[role]
    except KeyError as error:
        raise WorkerPolicyViolation(f"unknown worker role: {role}") from error


def authorize(role: str, *, mutation: str, resource: str) -> WorkerPolicy:
    policy = policy_for(role)
    if mutation not in policy.allowed_mutations:
        raise WorkerPolicyViolation(f"{role} cannot perform mutation {mutation}")
    if resource not in policy.allowed_resources:
        raise WorkerPolicyViolation(f"{role} cannot access resource {resource}")
    return policy


def scoped_environment(role: str, env: dict[str, str]) -> dict[str, str]:
    """Return only secret classes explicitly allowed for the worker role.

    Non-secret configuration is preserved.  Secret-looking variables are retained only when their
    name contains one of the role's declared secret classes.
    """
    policy = policy_for(role)
    out: dict[str, str] = {}
    for key, value in env.items():
        if not _SECRET.search(key):
            out[key] = value
            continue
        lowered = key.casefold()
        if any(secret.casefold() in lowered for secret in policy.secret_classes):
            out[key] = value
    return out


def worker_policy_manifest() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "roles": {
            role: {
                **asdict(policy),
                "zone": policy.zone.value,
                "digest": policy.digest(),
            }
            for role, policy in sorted(_POLICIES.items())
        },
    }
