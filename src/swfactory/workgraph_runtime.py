"""Bounded execution and deterministic fan-in for validated Plan.work DAGs.

Airflow remains the only scheduler.  This executor lives *inside* one fixed build task and only
coordinates issue-specific work nodes.  Completion timing is deliberately excluded from merge
order: successful node outputs fan in by stable node id with an explicit receipt for every merge.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from typing import Callable, Iterable, Literal, Protocol

from swfactory.workgraph import WorkNode, WorkWave, conflict_set, waves

NodeStatus = Literal["success", "failed", "cancelled"]
ConflictKind = Literal["disjoint", "overlap", "stale_base", "protected_path"]


@dataclass(frozen=True)
class NodeAttempt:
    cell_id: str
    epoch: int
    node_id: str
    attempt: int
    input_head: str
    dependency_heads: tuple[str, ...]
    started_at: float


@dataclass(frozen=True)
class NodeResult:
    node_id: str
    status: NodeStatus
    input_head: str
    output_head: str | None = None
    touched_files: tuple[str, ...] = ()
    artifacts: tuple[str, ...] = ()
    attempt: int = 1
    retryable: bool = False
    detail: str | None = None
    sandbox_incarnation: str | None = None
    duration_s: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status == "success" and bool(self.output_head)


@dataclass(frozen=True)
class MergeReceipt:
    node_id: str
    source_head: str
    target_before: str
    target_after: str
    conflict: ConflictKind = "disjoint"
    transformed: bool = False
    requires_reverification: bool = False
    detail: str | None = None


@dataclass(frozen=True)
class ExecutionReceipt:
    cell_id: str
    epoch: int
    mode: Literal["parallel", "serial"]
    waves: tuple[tuple[str, ...], ...]
    node_results: tuple[NodeResult, ...]
    merges: tuple[MergeReceipt, ...]
    cancelled: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


class NodeRunner(Protocol):
    def __call__(self, node: WorkNode, attempt: NodeAttempt) -> NodeResult: ...


class NodeMerger(Protocol):
    def __call__(self, result: NodeResult, target_head: str) -> MergeReceipt: ...


def classify_file_conflict(
    *,
    declared_files: Iterable[str],
    touched_files: Iterable[str],
    sibling_files: Iterable[str],
    protected_files: Iterable[str] = (),
    stale_base: bool = False,
) -> ConflictKind:
    declared = set(declared_files)
    touched = set(touched_files)
    siblings = set(sibling_files)
    protected = set(protected_files)
    if touched & protected:
        return "protected_path"
    if stale_base:
        return "stale_base"
    # Actual touches are authoritative; declared files add a conservative pre-merge signal.
    if (touched | declared) & siblings:
        return "overlap"
    return "disjoint"


@dataclass
class BoundedWorkExecutor:
    max_parallel: int = 4
    max_retries: int = 1
    cancel: threading.Event = field(default_factory=threading.Event)

    def execute(
        self,
        *,
        cell_id: str,
        epoch: int,
        nodes: Iterable[WorkNode],
        input_head: str,
        run_node: NodeRunner,
        merge_node: NodeMerger,
        supports_parallel_lineage: bool,
    ) -> ExecutionReceipt:
        ordered = tuple(nodes)
        graph_waves = waves(ordered)
        declared_conflicts = conflict_set(ordered)
        conflict_nodes = {node_id for left, right, _ in declared_conflicts for node_id in (left, right)}
        can_parallel = supports_parallel_lineage and self.max_parallel > 1
        mode: Literal["parallel", "serial"] = "parallel" if can_parallel else "serial"
        current_head = input_head
        results: dict[str, NodeResult] = {}
        merges: list[MergeReceipt] = []
        wave_ids: list[tuple[str, ...]] = []

        for wave in graph_waves:
            if self.cancel.is_set():
                break
            stable_nodes = tuple(sorted(wave.nodes, key=lambda node: node.id))
            wave_ids.append(tuple(node.id for node in stable_nodes))
            dependency_heads = {
                node.id: tuple(
                    results[dep].output_head or ""
                    for dep in sorted(node.depends_on)
                    if dep in results
                )
                for node in stable_nodes
            }
            parallel_nodes = tuple(
                node
                for node in stable_nodes
                if can_parallel and node.parallel_safe and node.id not in conflict_nodes
            )
            serial_nodes = tuple(node for node in stable_nodes if node not in parallel_nodes)

            wave_results: dict[str, NodeResult] = {}
            if len(parallel_nodes) >= 2:
                wave_results.update(
                    self._run_parallel(
                        cell_id,
                        epoch,
                        parallel_nodes,
                        current_head,
                        dependency_heads,
                        run_node,
                    )
                )
            else:
                serial_nodes = tuple(sorted((*serial_nodes, *parallel_nodes), key=lambda node: node.id))

            for node in serial_nodes:
                if self.cancel.is_set():
                    wave_results[node.id] = NodeResult(
                        node_id=node.id,
                        status="cancelled",
                        input_head=current_head,
                        detail="parent build stage cancelled",
                    )
                    continue
                wave_results[node.id] = self._run_with_retry(
                    cell_id,
                    epoch,
                    node,
                    current_head,
                    dependency_heads[node.id],
                    run_node,
                )

            # Deterministic fan-in: never merge in future-completion order.
            for node_id in sorted(wave_results):
                result = wave_results[node_id]
                results[node_id] = result
                if not result.ok:
                    self.cancel.set()
                    continue
                receipt = merge_node(result, current_head)
                if receipt.node_id != node_id or receipt.source_head != result.output_head:
                    raise ValueError(f"merge receipt does not describe node {node_id}")
                if receipt.target_before != current_head:
                    raise ValueError(
                        f"merge receipt for {node_id} expected {current_head}, got {receipt.target_before}"
                    )
                current_head = receipt.target_after
                merges.append(receipt)

            if any(not result.ok for result in wave_results.values()):
                break

        # Preserve graph order in the receipt while keeping merge order explicit.
        result_tuple = tuple(results[node.id] for node in ordered if node.id in results)
        return ExecutionReceipt(
            cell_id=cell_id,
            epoch=epoch,
            mode=mode,
            waves=tuple(wave_ids),
            node_results=result_tuple,
            merges=tuple(merges),
            cancelled=self.cancel.is_set(),
        )

    def _run_parallel(
        self,
        cell_id: str,
        epoch: int,
        nodes: tuple[WorkNode, ...],
        input_head: str,
        dependency_heads: dict[str, tuple[str, ...]],
        run_node: NodeRunner,
    ) -> dict[str, NodeResult]:
        results: dict[str, NodeResult] = {}
        workers = min(max(1, self.max_parallel), len(nodes))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="swf-node") as pool:
            futures: dict[Future[NodeResult], str] = {
                pool.submit(
                    self._run_with_retry,
                    cell_id,
                    epoch,
                    node,
                    input_head,
                    dependency_heads[node.id],
                    run_node,
                ): node.id
                for node in nodes
            }
            for future in as_completed(futures):
                node_id = futures[future]
                if self.cancel.is_set():
                    future.cancel()
                try:
                    results[node_id] = future.result()
                except BaseException as exc:
                    results[node_id] = NodeResult(
                        node_id=node_id,
                        status="failed",
                        input_head=input_head,
                        detail=f"node runner raised: {exc}",
                    )
                    self.cancel.set()
        return results

    def _run_with_retry(
        self,
        cell_id: str,
        epoch: int,
        node: WorkNode,
        input_head: str,
        dependency_heads: tuple[str, ...],
        run_node: NodeRunner,
    ) -> NodeResult:
        last: NodeResult | None = None
        for attempt_number in range(1, max(0, self.max_retries) + 2):
            if self.cancel.is_set():
                return NodeResult(
                    node_id=node.id,
                    status="cancelled",
                    input_head=input_head,
                    attempt=attempt_number,
                    detail="parent build stage cancelled",
                )
            started = time.monotonic()
            attempt = NodeAttempt(
                cell_id=cell_id,
                epoch=epoch,
                node_id=node.id,
                attempt=attempt_number,
                input_head=input_head,
                dependency_heads=dependency_heads,
                started_at=time.time(),
            )
            result = run_node(node, attempt)
            if result.node_id != node.id:
                raise ValueError(f"runner returned {result.node_id!r} for node {node.id!r}")
            if result.input_head != input_head:
                raise ValueError(f"runner changed immutable input head for node {node.id}")
            # Runner-reported duration wins when present; otherwise keep a bounded local measure.
            if result.duration_s <= 0:
                result = NodeResult(
                    **{
                        **asdict(result),
                        "duration_s": round(time.monotonic() - started, 6),
                        "attempt": attempt_number,
                    }
                )
            last = result
            if result.ok or not result.retryable:
                return result
        assert last is not None
        return last
