"""Executable conformance scenarios for many AI harnesses driving one repository.

This module is deliberately scheduler-free.  It exercises the same admission and Factory Cell
authority primitives the backend uses while Apache Airflow remains the only lifecycle scheduler.
Scenario fixtures can therefore hammer session identity, repo pressure, duplicate work, epoch
takeover and stale writes without inventing a second orchestration state machine.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swfactory.admission import AdmissionController, Limits, Priority
from swfactory.cells import (
    TERMINAL_STATES,
    CellBusy,
    CellIdentity,
    CellStore,
    DuplicateOperation,
    StaleEpoch,
)


@dataclass(frozen=True)
class Session:
    harness: str
    factory_id: str

    @classmethod
    def parse(cls, value: str) -> Session:
        harness, sep, factory_id = value.partition(":")
        if not sep or not _component(harness, 48) or not _component(factory_id, 64):
            raise ValueError("session must be HARNESS:FACTORY_ID using letters, digits, dot, underscore or hyphen")
        return cls(harness.lower(), factory_id.lower())

    @property
    def actor(self) -> str:
        return f"harness:{self.harness}:{self.factory_id}"


@dataclass(frozen=True)
class Intent:
    work_id: str
    request_id: str
    actor: str
    repo: str
    target: str
    issue: str
    blueprint: str
    priority: Priority

    @property
    def identity(self) -> CellIdentity:
        return CellIdentity(self.repo, self.target, self.issue)


@dataclass(frozen=True)
class ScenarioResult:
    active_admissions: int
    queued_admissions: int
    cells: int
    active_cells: int
    busy: int
    stale: int
    duplicate_submissions: int
    duplicate_operations: int
    max_epoch: int

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


def load_scenario(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text())
    if document.get("schema_version") != 1:
        raise ValueError(f"{path}: schema_version must be 1")
    if not isinstance(document.get("name"), str) or not document["name"].strip():
        raise ValueError(f"{path}: name is required")
    actions = document.get("actions")
    if not isinstance(actions, list) or not actions:
        raise ValueError(f"{path}: actions must be a non-empty array")
    if not isinstance(document.get("expect"), dict):
        raise ValueError(f"{path}: expect must be an object")
    return document


def execute_scenario(document: dict[str, Any], root: Path) -> ScenarioResult:
    limits = Limits(**document.get("limits", {}))
    admission = AdmissionController(limits)
    store = CellStore(root / "cells.sqlite3")
    repo = str(document.get("repo") or "owner/repo")
    blueprint = str(document.get("blueprint") or "factory")

    pending: dict[str, Intent] = {}
    active: dict[str, tuple[Intent, str, int]] = {}
    by_request: dict[str, str] = {}
    busy = stale = duplicate_submissions = duplicate_operations = 0

    def activate(work_id: str) -> list[str]:
        nonlocal busy
        intent = pending[work_id]
        try:
            cell = store.activate(intent.identity, actor=intent.actor)
        except CellBusy:
            busy += 1
            pending.pop(work_id, None)
            return admission.complete(work_id)
        active[work_id] = (intent, cell["cell_id"], int(cell["epoch"]))
        pending.pop(work_id, None)
        return []

    def drain(work_ids: list[str]) -> None:
        queue = list(work_ids)
        while queue:
            queue.extend(activate(queue.pop(0)))

    try:
        for index, action in enumerate(document["actions"]):
            if not isinstance(action, dict):
                raise ValueError(f"action {index} must be an object")
            op = action.get("op")

            if op == "submit":
                session = Session.parse(str(action["session"]))
                issue = str(action["issue"])
                target = str(action.get("target") or "")
                request_id = str(action.get("request_id") or f"request-{index}")
                seed = "\0".join((repo, target, issue, session.actor, request_id))
                work_id = "scenario_" + hashlib.sha256(seed.encode()).hexdigest()[:24]
                priority_name = str(action.get("priority") or "normal").upper()
                try:
                    priority = Priority[priority_name]
                except KeyError as error:
                    raise ValueError(f"unknown priority {priority_name.lower()}") from error
                intent = Intent(work_id, request_id, session.actor, repo, target, issue, blueprint, priority)
                by_request[request_id] = work_id
                pending[work_id] = intent
                decision = admission.submit(work_id, repo, session.actor, blueprint, priority)
                if decision.reason == "duplicate":
                    duplicate_submissions += 1
                    pending.pop(work_id, None)
                elif decision.admitted:
                    drain([work_id])
                elif decision.reason != "queued":
                    pending.pop(work_id, None)

            elif op in {"finish", "cancel"}:
                request_id = str(action["request_id"])
                work_id = by_request[request_id]
                intent, cell_id, epoch = active.pop(work_id)
                state = "success" if op == "finish" else "cancelled"
                store.patch(cell_id, epoch, f"scenario:{op}:{request_id}", state=state)
                drain(admission.complete(work_id))

            elif op == "takeover":
                identity = CellIdentity(repo, str(action.get("target") or ""), str(action["issue"]))
                cell_id = identity.stable_id()
                expected = int(action["epoch"])
                actor = Session.parse(str(action["session"])).actor
                try:
                    store.take_epoch(cell_id, expected, actor)
                except StaleEpoch:
                    stale += 1

            elif op == "mutate":
                identity = CellIdentity(repo, str(action.get("target") or ""), str(action["issue"]))
                cell_id = identity.stable_id()
                epoch = int(action["epoch"])
                operation_key = str(action.get("operation_key") or f"scenario:mutate:{index}")
                fields: dict[str, Any] = {}
                if "state" in action:
                    fields["state"] = str(action["state"])
                try:
                    store.patch(cell_id, epoch, operation_key, **fields)
                except StaleEpoch:
                    stale += 1
                except DuplicateOperation:
                    duplicate_operations += 1

            else:
                raise ValueError(f"unknown scenario operation {op!r}")

        snapshot = admission.snapshot()
        cells = store.list(limit=1000)
        return ScenarioResult(
            active_admissions=len(snapshot["active"]),
            queued_admissions=len(snapshot["queued"]),
            cells=len(cells),
            active_cells=sum(str(cell["state"]) not in TERMINAL_STATES for cell in cells),
            busy=busy,
            stale=stale,
            duplicate_submissions=duplicate_submissions,
            duplicate_operations=duplicate_operations,
            max_epoch=max((int(cell["epoch"]) for cell in cells), default=0),
        )
    finally:
        store.close()


def assert_scenario(document: dict[str, Any], result: ScenarioResult) -> None:
    observed = result.as_dict()
    for key, expected in document["expect"].items():
        if key not in observed:
            raise AssertionError(f"{document['name']}: unknown expectation {key}")
        if observed[key] != expected:
            raise AssertionError(
                f"{document['name']}: expected {key}={expected!r}, observed {observed[key]!r}"
            )


def _component(value: str, limit: int) -> bool:
    return (
        1 <= len(value) <= limit
        and all(char.isascii() and (char.isalnum() or char in "._-") for char in value)
    )
