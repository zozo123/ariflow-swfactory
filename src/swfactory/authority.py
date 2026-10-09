"""Canonical control-plane authority map and offline invariant checks.

This module is intentionally declarative.  It does not schedule work.  Airflow remains the
lifecycle scheduler; these rules only answer who is allowed to mutate each durable resource.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from swfactory.cells import is_cell_id


class AuthorityViolation(RuntimeError):
    """A mutation or the rule map violates the single-authority contract."""


class ResourceKind(StrEnum):
    CELL = "cell"
    AIRFLOW_RUN = "airflow_run"
    SANDBOX = "sandbox"
    GITHUB_PUBLICATION = "github_publication"
    MODEL_CALL = "model_call"
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
    AuthorityRule(ResourceKind.MODEL_CALL, "python-backend", "operation_key", "operation journal"),
    AuthorityRule(ResourceKind.CLEANUP, "python-backend", "cell_epoch", "cleanup receipt"),
    AuthorityRule(ResourceKind.ADMISSION, "python-backend", "work_id", "admission store"),
    AuthorityRule(ResourceKind.EVIDENCE, "python-backend", "cell_epoch", "evidence bundle"),
    AuthorityRule(ResourceKind.GENERATION, "python-backend", "generation_id", "generation manifest"),
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
            raise AuthorityViolation(f"{self.resource.value} mutations belong to {owner}, not {self.actor}")
        if not is_cell_id(self.cell_id):
            raise AuthorityViolation("mutation must carry a Factory Cell id")
        if not self.operation_key.strip():
            raise AuthorityViolation("mutation must carry an operation key")


def rule_for(resource: ResourceKind) -> AuthorityRule:
    for rule in _RULES:
        if rule.resource == resource:
            return rule
    raise KeyError(resource)


def validate_manifest() -> None:
    resources = [rule.resource for rule in _RULES]
    if len(resources) != len(set(resources)):
        raise AuthorityViolation("a mutable resource has more than one authority rule")
    if rule_for(ResourceKind.AIRFLOW_RUN).owner != "airflow":
        raise AuthorityViolation("Airflow must remain the lifecycle scheduling authority")


validate_manifest()
