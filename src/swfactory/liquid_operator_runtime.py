"""Rough typed operator control surface shared by CLI/TUI/API issue families."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class OperatorAction(StrEnum):
    INSPECT = "inspect"
    CANCEL = "cancel"
    RETRY = "retry"
    REPAIR = "repair"
    PROMOTE = "promote"
    ROLLBACK = "rollback"


DESTRUCTIVE = frozenset({OperatorAction.CANCEL, OperatorAction.REPAIR, OperatorAction.PROMOTE, OperatorAction.ROLLBACK})


@dataclass(frozen=True)
class OperatorRequest:
    action: OperatorAction
    cell_id: str | None = None
    generation_id: str | None = None
    expected_epoch: int | None = None
    operation_key: str | None = None
    approved: bool = False

    def validate(self) -> None:
        if self.action in {OperatorAction.INSPECT}:
            return
        if self.action in {OperatorAction.CANCEL, OperatorAction.RETRY, OperatorAction.REPAIR}:
            if not self.cell_id or not self.cell_id.startswith("cell_"):
                raise ValueError("cell action requires Factory Cell id")
            if self.expected_epoch is None or self.expected_epoch < 1:
                raise ValueError("cell mutation requires positive expected epoch")
            if not self.operation_key:
                raise ValueError("cell mutation requires operation key")
        if self.action in {OperatorAction.PROMOTE, OperatorAction.ROLLBACK}:
            if not self.generation_id:
                raise ValueError("generation action requires generation id")
            if not self.approved:
                raise ValueError("promotion/rollback requires explicit approval")

    def public(self) -> dict[str, Any]:
        self.validate()
        data = asdict(self)
        data["action"] = self.action.value
        data["destructive"] = self.action in DESTRUCTIVE
        return data


def render_operator_state(
    *, cells: list[dict[str, Any]], debt: list[dict[str, Any]], generations: list[dict[str, Any]]
) -> dict[str, Any]:
    """One stable view for CLI/TUI/API renderers; contains no business logic."""
    return {
        "schema_version": 1,
        "cells": cells,
        "cleanup_debt": debt,
        "generations": generations,
        "counts": {
            "cells": len(cells),
            "cleanup_debt": len(debt),
            "generations": len(generations),
        },
    }


def promotion_decision(*, candidate_evidence_ok: bool, parent_evidence_ok: bool, approved: bool) -> str:
    if not approved:
        return "await_approval"
    if not candidate_evidence_ok:
        return "refuse_candidate"
    if not parent_evidence_ok:
        return "refuse_unverified_parent"
    return "promote"
