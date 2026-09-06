"""Deterministic execution planning for validated Plan.work DAGs.

Airflow remains the scheduler. This module plans bounded work *inside* the governed build stage.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkNode:
    id: str
    depends_on: tuple[str, ...] = ()
    files: tuple[str, ...] = ()
    parallel_safe: bool = False


@dataclass(frozen=True)
class WorkWave:
    index: int
    nodes: tuple[WorkNode, ...]


class WorkGraphError(ValueError):
    pass


def waves(nodes: Iterable[WorkNode]) -> tuple[WorkWave, ...]:
    ordered = tuple(nodes)
    by_id = {n.id: n for n in ordered}
    if len(by_id) != len(ordered):
        raise WorkGraphError("duplicate work node id")
    for node in ordered:
        missing = set(node.depends_on) - set(by_id)
        if missing:
            raise WorkGraphError(f"{node.id}: unknown dependencies {sorted(missing)}")
        if node.id in node.depends_on:
            raise WorkGraphError(f"{node.id}: self dependency")
    done: set[str] = set()
    remaining = set(by_id)
    result: list[WorkWave] = []
    while remaining:
        ready = tuple(n for n in ordered if n.id in remaining and set(n.depends_on) <= done)
        if not ready:
            raise WorkGraphError("cycle detected")
        result.append(WorkWave(len(result), ready))
        done.update(n.id for n in ready)
        remaining.difference_update(n.id for n in ready)
    return tuple(result)


def parallel_groups(nodes: Iterable[WorkNode]) -> tuple[tuple[str, ...], ...]:
    groups = []
    for wave in waves(nodes):
        safe = [node for node in wave.nodes if node.parallel_safe]
        if len(safe) < 2:
            continue
        groups.append(tuple(node.id for node in safe))
    return tuple(groups)


def conflict_set(nodes: Iterable[WorkNode]) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    ordered = tuple(nodes)
    conflicts = []
    for i, left in enumerate(ordered):
        for right in ordered[i + 1 :]:
            overlap = tuple(sorted(set(left.files).intersection(right.files)))
            if overlap:
                conflicts.append((left.id, right.id, overlap))
    return tuple(conflicts)


def deterministic_merge_order(completed_node_ids: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(completed_node_ids)))
