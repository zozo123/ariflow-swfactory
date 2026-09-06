"""Canonical control-plane authority map and offline invariant checks.

This module is intentionally declarative.  It does not schedule work.  Airflow remains the
lifecycle scheduler; these rules only answer who is allowed to mutate each durable resource.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class AuthorityViolation(RuntimeError):
    """A mutation or snapshot violates the single-authority contract."""


class ResourceKind(StrEnum):
    CELL = "cell"
    AIRFLOW_RUN = "airflow_run"
    SANDBOX = "sandbox"
    GITHUB_PUBLICATION = "github_publication"
    CLEANUP = "cleanup"
    ADMISSION = "admission"
    EVIDENCE = "evidence"
    GENERATION = "generation"


@dataclass(frozen=True)
class AuthorityRule:
    resource: ResourceKind
    owner: str
    fence: str
    truth: str


_RULES = (
    AuthorityRule(ResourceKind.CELL, "python-backend", "cell_epoch", "CellStore"),
    AuthorityRule(ResourceKind.AIRFLOW_RUN, "airflow", "dag_run_id", "Airflow metadata DB"),
    AuthorityRule(ResourceKind.SANDBOX, "python-backend", "cell_epoch", "provider receipt"),
    AuthorityRule(
        ResourceKind.GITHUB_PUBLICATION,
        "python-backend",
        "operation_key",
        "operation journal",
    ),
    AuthorityRule(ResourceKind.CLEANUP, "python-backend", "cell_epoch", "cleanup receipt"),
    AuthorityRule(ResourceKind.ADMISSION, "python-backend", "work_id", "admission store"),
    AuthorityRule(ResourceKind.EVIDENCE, "python-backend", "cell_epoch", "evidence bundle"),
    AuthorityRule(
        ResourceKind.GENERATION, "python-backend", "generation_id", "generation manifest"
    ),
)


@dataclass(frozen=True)
class MutationAuthority:
    resource: ResourceKind
    actor: str
    cell_id: str
    epoch: int
    operation_key: str

    def validate(self, *, current_epoch: int, allowed_actor: str | None = None) -> None:
        if self.epoch < 1:
            raise AuthorityViolation("mutation epoch must be positive")
        if self.epoch != current_epoch:
            raise AuthorityViolation(f"stale epoch {self.epoch}; current epoch is {current_epoch}")
        owner = allowed_actor or rule_for(self.resource).owner
        if self.actor != owner:
            raise AuthorityViolation(
                f"{self.resource.value} mutations belong to {owner}, not {self.actor}"
            )
        if not self.cell_id.startswith("cell_"):
            raise AuthorityViolation("mutation must carry a Factory Cell id")
        if not self.operation_key.strip():
            raise AuthorityViolation("mutation must carry an operation key")


def rule_for(resource: ResourceKind) -> AuthorityRule:
    for rule in _RULES:
        if rule.resource == resource:
            return rule
    raise KeyError(resource)


def authority_manifest() -> dict[str, Any]:
    """Return a stable machine-readable authority document."""
    return {
        "schema_version": 1,
        "scheduler": "airflow",
        "rules": [asdict(rule) for rule in _RULES],
    }


def validate_manifest() -> None:
    resources = [rule.resource for rule in _RULES]
    if len(resources) != len(set(resources)):
        raise AuthorityViolation("a mutable resource has more than one authority rule")
    if rule_for(ResourceKind.AIRFLOW_RUN).owner != "airflow":
        raise AuthorityViolation("Airflow must remain the lifecycle scheduling authority")


def check_snapshot(snapshot: dict[str, list[dict[str, Any]]]) -> tuple[str, ...]:
    """Return deterministic invariant failures for an operator snapshot.

    Supported rows are intentionally generic so the checker can consume SQLite exports, backend
    API documents, or retained evidence without becoming another persistence layer.
    """
    failures: list[str] = []
    seen_operations: dict[str, tuple[str, int]] = {}

    for cell in snapshot.get("cells", []):
        cell_id = str(cell.get("cell_id", ""))
        epoch = cell.get("epoch")
        if not cell_id.startswith("cell_"):
            failures.append(f"invalid_cell_id:{cell_id or '<missing>'}")
        if type(epoch) is not int or epoch < 1:
            failures.append(f"invalid_cell_epoch:{cell_id}")

    for operation in snapshot.get("operations", []):
        key = str(operation.get("operation_key", ""))
        cell_id = str(operation.get("cell_id", ""))
        epoch = operation.get("epoch")
        if not key:
            failures.append(f"missing_operation_key:{cell_id}")
            continue
        identity = (cell_id, epoch if type(epoch) is int else -1)
        previous = seen_operations.setdefault(key, identity)
        if previous != identity:
            failures.append(f"operation_identity_conflict:{key}")

    for resource in snapshot.get("resources", []):
        state = str(resource.get("state", ""))
        if state == "active" and resource.get("tombstoned") is True:
            failures.append(f"zombie_resource:{resource.get('id', '<unknown>')}")

    return tuple(sorted(set(failures)))


validate_manifest()
