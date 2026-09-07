"""Rough bounded workgraph/runtime implementation for compute and repository issue families."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkNode:
    node_id: str
    deps: tuple[str, ...] = ()
    lane: str = "workgraph"


@dataclass(frozen=True)
class WorkPlan:
    layers: tuple[tuple[str, ...], ...]
    width: int


def compile_workgraph(nodes: Iterable[WorkNode], *, max_width: int = 7) -> WorkPlan:
    if max_width < 1 or max_width > 7:
        raise ValueError("inner workgraph width must be between 1 and 7")
    materialized = tuple(nodes)
    rows = {node.node_id: node for node in materialized}
    if not rows:
        return WorkPlan((), max_width)
    if len(rows) != len(materialized):
        raise ValueError("duplicate node id")
    remaining = set(rows)
    emitted: set[str] = set()
    layers: list[tuple[str, ...]] = []
    while remaining:
        ready = sorted(node_id for node_id in remaining if set(rows[node_id].deps) <= emitted)
        if not ready:
            unknown = {dep for node_id in remaining for dep in rows[node_id].deps if dep not in rows}
            if unknown:
                raise ValueError(f"unknown dependencies: {sorted(unknown)}")
            raise ValueError("workgraph contains a cycle")
        for offset in range(0, len(ready), max_width):
            layer = tuple(ready[offset : offset + max_width])
            layers.append(layer)
            emitted.update(layer)
            remaining.difference_update(layer)
    return WorkPlan(tuple(layers), max_width)


@dataclass(frozen=True)
class RepositoryPublication:
    repo: str
    base_sha: str
    observed_target_sha: str
    branch: str
    operation_key: str

    def validate(self) -> None:
        if not self.repo or not self.base_sha or not self.observed_target_sha:
            raise ValueError("repository publication requires pinned source identities")
        if not self.branch or not self.operation_key:
            raise ValueError("publication requires deterministic branch and operation key")


def publication_action(publication: RepositoryPublication, *, current_target_sha: str) -> str:
    publication.validate()
    if current_target_sha != publication.observed_target_sha:
        return "rebase_or_refuse"
    return "publish_once"


@dataclass(frozen=True)
class SandboxLease:
    cell_id: str
    epoch: int
    provider: str
    external_id: str

    def reclaimable(self, *, current_epoch: int, terminal: bool) -> bool:
        return terminal or self.epoch != current_epoch
