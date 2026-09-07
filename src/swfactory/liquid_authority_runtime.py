"""Rough implementation layer for authority/fencing issue families.

This is intentionally thin over the canonical CellStore and authority contract. It adds no
scheduler and no second source of truth; it only turns the authority acceptance surface into
callable lifecycle operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from swfactory.authority import MutationAuthority, ResourceKind
from swfactory.cells import CellIdentity, CellStore, Mutation, operation_key


@dataclass(frozen=True)
class AuthorityDecision:
    cell_id: str
    epoch: int
    operation_key: str
    resource: ResourceKind
    actor: str


class LiquidAuthorityRuntime:
    def __init__(self, store: CellStore) -> None:
        self.store = store

    def activate(self, *, repo: str, target: str, issue: str, actor: str) -> dict[str, Any]:
        identity = CellIdentity(repo=repo, target=target, issue=issue)
        return self.store.activate(identity, actor)

    def takeover(self, *, cell_id: str, expected_epoch: int, actor: str) -> int:
        return self.store.take_epoch(cell_id, expected_epoch, actor)

    def authorize(
        self,
        *,
        resource: ResourceKind,
        actor: str,
        cell_id: str,
        epoch: int,
        kind: str,
        parts: tuple[str, ...] = (),
    ) -> AuthorityDecision:
        current = self.store.get(cell_id)
        key = operation_key(kind, cell_id, str(epoch), *parts)
        MutationAuthority(
            resource=resource,
            actor=actor,
            cell_id=cell_id,
            epoch=epoch,
            operation_key=key,
        ).validate(current_epoch=int(current["epoch"]))
        return AuthorityDecision(cell_id, epoch, key, resource, actor)

    def record_effect(
        self,
        *,
        decision: AuthorityDecision,
        kind: str,
        payload: dict[str, Any],
    ) -> None:
        self.store.record(
            Mutation(
                cell_id=decision.cell_id,
                epoch=decision.epoch,
                operation_key=decision.operation_key,
                kind=kind,
                payload=payload,
            )
        )

    def observe_before_retry(self, *, cell_id: str, operation_key_value: str) -> dict[str, Any] | None:
        for event in reversed(self.store.history(cell_id)):
            if event["operation_key"] == operation_key_value:
                return event
        return None
