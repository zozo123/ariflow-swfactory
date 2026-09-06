"""Bounded deterministic executor for validated ``Plan.work`` DAGs.

This executor lives *inside* the fixed Airflow build stage.  It does not schedule Airflow work and
it does not choose lifecycle transitions.  It fans out independent work nodes, captures immutable
receipts, and fans them back in through a stable merge order.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from typing import Literal, Protocol

from swfactory.workgraph import WorkNode, conflict_set, deterministic_merge_order, waves

NodeState = Literal["ok", "failed", "cancelled", "skipped"]
ConflictKind = Literal["disjoint", "overlap", "stale_base", "protected"]


@dataclass(frozen=True)
class NodeRequest:
    cell_id: str
    epoch: int
    node: WorkNode
    input_head: str
    dependency_results: tuple[str, ...]
    attempt: int = 1
    timeout_s: int = 1800

    @property
    def logical_id(self) -> str:
        raw = f"{self.cell_id}\0{self.epoch}\0{self.node.id}\0{self.input_head}".encode()
        return "wn_" + hashlib.sha256(raw).hexdigest()[:24]


@dataclass(frozen=True)
class NodeResult:
    logical_id: str
    node_id: str
    state: NodeState
    input_head: str
    output_head: str | None
    touched_files: tuple[str, ...] = ()
    artifacts: tuple[str, ...] = ()
    provider_incarnation: str | None = None
    attempt: int = 1
    duration_s: float = 0.0
    detail: str = ""


@dataclass(frozen=True)
class ConflictReceipt:
    left: str
    right: str
    kind: ConflictKind
    files: tuple[str, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class MergeReceipt:
    index: int
    node_id: str
    source_head: str
    target_before: str
    target_after: str
    transform: str
    requires_reverify: bool
    conflicts: tuple[ConflictReceipt, ...] = ()


@dataclass(frozen=True)
class ExecutionReport:
    cell_id: str
    epoch: int
    input_head: str
    parallel: bool
    results: tuple[NodeResult, ...]
    merges: tuple[MergeReceipt, ...]
    final_head: str
    cancelled: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


class NodeRunner(Protocol):
    def __call__(self, request: NodeRequest) -> NodeResult: ...


class NodeMerger(Protocol):
    def __call__(self, result: NodeResult, target_head: str, index: int) -> MergeReceipt: ...


class Cancellation:
    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


@dataclass(frozen=True)
class ExecutorPolicy:
    max_parallel: int = 4
    max_attempts: int = 2
    allow_parallel: bool = True
    protected_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.max_parallel < 1 or self.max_attempts < 1:
            raise ValueError("executor limits must be positive")


class WorkExecutor:
    def __init__(
        self, runner: NodeRunner, merger: NodeMerger, policy: ExecutorPolicy | None = None
    ):
        self.runner = runner
        self.merger = merger
        self.policy = policy or ExecutorPolicy()

    def execute(
        self,
        *,
        cell_id: str,
        epoch: int,
        input_head: str,
        nodes: Iterable[WorkNode],
        supports_fork: bool,
        cancellation: Cancellation | None = None,
    ) -> ExecutionReport:
        ordered = tuple(nodes)
        cancellation = cancellation or Cancellation()
        declared_conflicts = conflict_set(ordered)
        parallel = (
            self.policy.allow_parallel
            and supports_fork
            and self.policy.max_parallel > 1
            and not declared_conflicts
        )
        results: dict[str, NodeResult] = {}
        target_head = input_head
        merges: list[MergeReceipt] = []

        for wave in waves(ordered):
            if cancellation.cancelled:
                break
            requests = [
                NodeRequest(
                    cell_id=cell_id,
                    epoch=epoch,
                    node=node,
                    input_head=target_head,
                    dependency_results=tuple(
                        results[d].output_head or results[d].input_head for d in node.depends_on
                    ),
                )
                for node in wave.nodes
            ]
            wave_results = (
                self._parallel(requests, cancellation)
                if parallel and len(requests) > 1
                else self._serial(requests, cancellation)
            )
            for result in wave_results:
                results[result.node_id] = result
            if any(result.state != "ok" for result in wave_results):
                cancellation.cancel()
                break

            # Completion timing is irrelevant: fan-in is always node-id stable.
            for node_id in deterministic_merge_order(r.node_id for r in wave_results):
                result = results[node_id]
                if result.output_head is None:
                    cancellation.cancel()
                    break
                receipt = self.merger(result, target_head, len(merges))
                self._validate_merge(receipt, result, target_head)
                merges.append(receipt)
                target_head = receipt.target_after

        for node in ordered:
            if node.id not in results:
                results[node.id] = NodeResult(
                    logical_id=NodeRequest(cell_id, epoch, node, target_head, ()).logical_id,
                    node_id=node.id,
                    state="cancelled" if cancellation.cancelled else "skipped",
                    input_head=target_head,
                    output_head=None,
                    detail="not started after upstream cancellation/failure",
                )

        return ExecutionReport(
            cell_id=cell_id,
            epoch=epoch,
            input_head=input_head,
            parallel=parallel,
            results=tuple(results[node.id] for node in ordered),
            merges=tuple(merges),
            final_head=target_head,
            cancelled=cancellation.cancelled,
        )

    def _serial(self, requests: list[NodeRequest], cancellation: Cancellation) -> list[NodeResult]:
        out: list[NodeResult] = []
        for request in requests:
            if cancellation.cancelled:
                break
            result = self._run_with_retry(request, cancellation)
            out.append(result)
            if result.state != "ok":
                cancellation.cancel()
                break
        return out

    def _parallel(
        self, requests: list[NodeRequest], cancellation: Cancellation
    ) -> list[NodeResult]:
        by_id: dict[str, NodeResult] = {}
        with ThreadPoolExecutor(max_workers=min(self.policy.max_parallel, len(requests))) as pool:
            futures: dict[Future[NodeResult], NodeRequest] = {
                pool.submit(self._run_with_retry, request, cancellation): request
                for request in requests
            }
            for future in as_completed(futures):
                request = futures[future]
                if cancellation.cancelled and future.cancel():
                    continue
                try:
                    result = future.result()
                except BaseException as exc:
                    result = NodeResult(
                        logical_id=request.logical_id,
                        node_id=request.node.id,
                        state="failed",
                        input_head=request.input_head,
                        output_head=None,
                        attempt=request.attempt,
                        detail=f"{type(exc).__name__}: {exc}"[:2000],
                    )
                by_id[request.node.id] = result
                if result.state != "ok":
                    cancellation.cancel()
                    for pending in futures:
                        pending.cancel()
        return [by_id[r.node.id] for r in requests if r.node.id in by_id]

    def _run_with_retry(self, request: NodeRequest, cancellation: Cancellation) -> NodeResult:
        last: NodeResult | None = None
        for attempt in range(1, self.policy.max_attempts + 1):
            if cancellation.cancelled:
                return NodeResult(
                    logical_id=request.logical_id,
                    node_id=request.node.id,
                    state="cancelled",
                    input_head=request.input_head,
                    output_head=None,
                    attempt=attempt,
                )
            started = time.monotonic()
            req = NodeRequest(
                cell_id=request.cell_id,
                epoch=request.epoch,
                node=request.node,
                input_head=request.input_head,
                dependency_results=request.dependency_results,
                attempt=attempt,
                timeout_s=request.timeout_s,
            )
            result = self.runner(req)
            if result.logical_id != request.logical_id or result.node_id != request.node.id:
                raise ValueError("runner returned result for the wrong logical node")
            result = NodeResult(
                **{
                    **asdict(result),
                    "attempt": attempt,
                    "duration_s": max(result.duration_s, time.monotonic() - started),
                }
            )
            last = result
            if result.state == "ok":
                return result
            # Infrastructure-like retry is opt-in via the detail prefix; deterministic code/test
            # failures do not churn the same input repeatedly.
            if not result.detail.startswith("retryable:"):
                return result
        assert last is not None
        return last

    def classify_conflicts(
        self,
        results: Iterable[NodeResult],
        *,
        observed_target_head: str,
        expected_target_head: str,
    ) -> tuple[ConflictReceipt, ...]:
        ordered = tuple(results)
        found: list[ConflictReceipt] = []
        if observed_target_head != expected_target_head:
            found.append(
                ConflictReceipt(
                    left="target",
                    right="candidate",
                    kind="stale_base",
                    detail=f"expected {expected_target_head}, observed {observed_target_head}",
                )
            )
        protected = set(self.policy.protected_paths)
        for result in ordered:
            hit = tuple(sorted(protected.intersection(result.touched_files)))
            if hit:
                found.append(ConflictReceipt(result.node_id, "policy", "protected", hit))
        for i, left in enumerate(ordered):
            for right in ordered[i + 1 :]:
                overlap = tuple(sorted(set(left.touched_files).intersection(right.touched_files)))
                if overlap:
                    found.append(ConflictReceipt(left.node_id, right.node_id, "overlap", overlap))
        return tuple(found)

    @staticmethod
    def _validate_merge(receipt: MergeReceipt, result: NodeResult, target_head: str) -> None:
        if receipt.node_id != result.node_id or receipt.source_head != result.output_head:
            raise ValueError("merge receipt does not match node result")
        if receipt.target_before != target_head:
            raise ValueError("merge receipt target head is stale")
        if not receipt.target_after:
            raise ValueError("merge receipt has no target_after")
        if receipt.transform != "fast_forward" and not receipt.requires_reverify:
            raise ValueError("transformed merge must require re-verification")
