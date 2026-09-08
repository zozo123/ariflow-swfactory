"""The unit scenarios the `workgraph.provider-fork` claim rests on.

That claim is deliberately experimental: no provider has proved isolated candidate workspaces and
retained lineage yet.  What the executor *must* already guarantee, before any provider is trusted
with it, is the part that is ours rather than the provider's: fan-out only when the graph is
conflict-free, fan-in in node-id order regardless of completion order, no merge onto a stale head,
and a crash or cancellation that stops the wave instead of half-merging it.

The inventory used to cite these as prose ("WorkExecutor unit scenarios for ..."), which resolved
to nothing.  They are executable here, against ``swfactory.work_executor.WorkExecutor`` itself.
"""

from __future__ import annotations

import threading
import time

import pytest

from swfactory.work_executor import (
    Cancellation,
    ExecutorPolicy,
    MergeReceipt,
    NodeRequest,
    NodeResult,
    WorkExecutor,
)
from swfactory.workgraph import WorkNode

HEAD = "head0000"


def _merger(result: NodeResult, target_head: str, index: int) -> MergeReceipt:
    """A fast-forward merge that keeps the receipt honest about what it moved."""
    return MergeReceipt(
        index=index,
        node_id=result.node_id,
        source_head=result.output_head or "",
        target_before=target_head,
        target_after=f"{target_head}+{result.node_id}",
        transform="fast_forward",
        requires_reverify=False,
    )


def _runner(*, fail: str | None = None, detail: str = "node failed", record: list[str] | None = None):
    def run(request: NodeRequest) -> NodeResult:
        if record is not None:
            record.append(request.node.id)
        if fail == request.node.id:
            return NodeResult(
                logical_id=request.logical_id,
                node_id=request.node.id,
                state="failed",
                input_head=request.input_head,
                output_head=None,
                detail=detail,
            )
        return NodeResult(
            logical_id=request.logical_id,
            node_id=request.node.id,
            state="ok",
            input_head=request.input_head,
            output_head=f"{request.input_head}/{request.node.id}",
            touched_files=request.node.files,
        )

    return run


def _execute(executor: WorkExecutor, nodes: tuple[WorkNode, ...], *, supports_fork: bool = True, **kwargs):
    return executor.execute(
        cell_id="cell_1",
        epoch=1,
        input_head=HEAD,
        nodes=nodes,
        supports_fork=supports_fork,
        **kwargs,
    )


def test_independent_nodes_fan_out_and_fan_in_in_node_id_order() -> None:
    """Completion order is provider timing; merge order has to be a property of the graph."""
    slow_first = WorkNode(id="b", files=("b.py",), parallel_safe=True)
    fast_second = WorkNode(id="a", files=("a.py",), parallel_safe=True)
    completed: list[str] = []

    def run(request: NodeRequest) -> NodeResult:
        # "b" is dispatched first and answers last, so a completion-ordered fan-in would invert.
        time.sleep(0.05 if request.node.id == "b" else 0.0)
        completed.append(request.node.id)
        return NodeResult(
            logical_id=request.logical_id,
            node_id=request.node.id,
            state="ok",
            input_head=request.input_head,
            output_head=f"{request.input_head}/{request.node.id}",
            touched_files=request.node.files,
        )

    report = _execute(WorkExecutor(run, _merger), (slow_first, fast_second))

    assert report.parallel is True
    assert completed == ["a", "b"]
    assert [merge.node_id for merge in report.merges] == ["a", "b"]
    assert report.final_head == f"{HEAD}+a+b"
    assert all(result.state == "ok" for result in report.results)


def test_a_declared_file_conflict_refuses_to_fan_out_at_all() -> None:
    """Two nodes over the same file are serialized: parallelism is never worth a lost edit."""
    nodes = (
        WorkNode(id="a", files=("shared.py",), parallel_safe=True),
        WorkNode(id="b", files=("shared.py",), parallel_safe=True),
    )
    order: list[str] = []

    report = _execute(WorkExecutor(_runner(record=order), _merger), nodes)

    assert report.parallel is False
    assert order == ["a", "b"]


def test_a_provider_without_fork_support_never_runs_in_parallel() -> None:
    nodes = (
        WorkNode(id="a", files=("a.py",), parallel_safe=True),
        WorkNode(id="b", files=("b.py",), parallel_safe=True),
    )

    report = _execute(WorkExecutor(_runner(), _merger), nodes, supports_fork=False)

    assert report.parallel is False
    assert report.final_head == f"{HEAD}+a+b"


def test_a_crashing_node_cancels_the_wave_and_merges_nothing() -> None:
    """A raised exception is a node result, not an executor crash — and it stops the fan-in."""

    def run(request: NodeRequest) -> NodeResult:
        if request.node.id == "b":
            raise RuntimeError("provider sandbox vanished")
        return NodeResult(
            logical_id=request.logical_id,
            node_id=request.node.id,
            state="ok",
            input_head=request.input_head,
            output_head=f"{request.input_head}/{request.node.id}",
        )

    nodes = (
        WorkNode(id="a", files=("a.py",), parallel_safe=True),
        WorkNode(id="b", files=("b.py",), parallel_safe=True),
    )
    report = _execute(WorkExecutor(run, _merger), nodes)

    failed = {result.node_id: result for result in report.results}["b"]
    assert failed.state == "failed"
    assert "provider sandbox vanished" in failed.detail
    assert report.cancelled is True
    assert report.merges == ()
    assert report.final_head == HEAD


def test_a_failed_node_leaves_the_next_wave_unstarted_rather_than_skipped_silently() -> None:
    nodes = (
        WorkNode(id="a", files=("a.py",)),
        WorkNode(id="b", depends_on=("a",), files=("b.py",)),
    )
    started: list[str] = []

    report = _execute(WorkExecutor(_runner(fail="a", record=started), _merger), nodes)

    assert started == ["a"]
    states = {result.node_id: result.state for result in report.results}
    assert states == {"a": "failed", "b": "cancelled"}
    assert report.merges == ()


def test_cancellation_between_waves_stops_the_run_and_says_so() -> None:
    """Operator cancellation must be visible in the report, not inferred from missing receipts."""
    nodes = (
        WorkNode(id="a", files=("a.py",)),
        WorkNode(id="b", depends_on=("a",), files=("b.py",)),
    )
    cancellation = Cancellation()
    ran: list[str] = []

    def run(request: NodeRequest) -> NodeResult:
        ran.append(request.node.id)
        cancellation.cancel()  # the operator cancels while the first node is in flight
        return NodeResult(
            logical_id=request.logical_id,
            node_id=request.node.id,
            state="ok",
            input_head=request.input_head,
            output_head=f"{request.input_head}/{request.node.id}",
        )

    report = _execute(WorkExecutor(run, _merger), nodes, cancellation=cancellation)

    assert ran == ["a"]
    assert report.cancelled is True
    assert {result.node_id: result.state for result in report.results}["b"] == "cancelled"


def test_only_an_infrastructure_failure_is_retried() -> None:
    """Retrying a deterministic test failure burns budget on the same answer."""
    attempts: list[int] = []

    def run(request: NodeRequest) -> NodeResult:
        attempts.append(request.attempt)
        return NodeResult(
            logical_id=request.logical_id,
            node_id=request.node.id,
            state="failed",
            input_head=request.input_head,
            output_head=None,
            detail="retryable: sandbox lease expired" if len(attempts) == 1 else "assertion failed",
        )

    executor = WorkExecutor(run, _merger, ExecutorPolicy(max_attempts=3))
    report = _execute(executor, (WorkNode(id="a", files=("a.py",)),))

    assert attempts == [1, 2]
    assert report.results[0].attempt == 2


def test_a_result_for_the_wrong_node_is_refused_before_it_can_be_merged() -> None:
    """A provider that answers with someone else's work must not become a merge receipt."""

    def run(request: NodeRequest) -> NodeResult:
        return NodeResult(
            logical_id="wn_somebody_else",
            node_id=request.node.id,
            state="ok",
            input_head=request.input_head,
            output_head="other",
        )

    with pytest.raises(ValueError, match="wrong logical node"):
        _execute(WorkExecutor(run, _merger), (WorkNode(id="a", files=("a.py",)),))


def test_a_merge_receipt_that_moved_a_stale_head_is_refused() -> None:
    def stale_merger(result: NodeResult, target_head: str, index: int) -> MergeReceipt:
        del target_head
        return MergeReceipt(
            index=index,
            node_id=result.node_id,
            source_head=result.output_head or "",
            target_before="somewhere-else",
            target_after="merged",
            transform="fast_forward",
            requires_reverify=False,
        )

    with pytest.raises(ValueError, match="target head is stale"):
        _execute(WorkExecutor(_runner(), stale_merger), (WorkNode(id="a", files=("a.py",)),))


def test_a_rewritten_merge_must_demand_re_verification() -> None:
    """Anything but a fast-forward produces a tree no node ever tested."""

    def rebasing_merger(result: NodeResult, target_head: str, index: int) -> MergeReceipt:
        return MergeReceipt(
            index=index,
            node_id=result.node_id,
            source_head=result.output_head or "",
            target_before=target_head,
            target_after=f"{target_head}+{result.node_id}",
            transform="rebase",
            requires_reverify=False,
        )

    with pytest.raises(ValueError, match="require re-verification"):
        _execute(WorkExecutor(_runner(), rebasing_merger), (WorkNode(id="a", files=("a.py",)),))


def test_conflicts_are_classified_against_the_head_the_candidates_actually_left() -> None:
    results = (
        NodeResult("wn_a", "a", "ok", HEAD, f"{HEAD}/a", touched_files=("src/app.py", "factory.toml")),
        NodeResult("wn_b", "b", "ok", HEAD, f"{HEAD}/b", touched_files=("src/app.py",)),
    )
    executor = WorkExecutor(_runner(), _merger, ExecutorPolicy(protected_paths=("factory.toml",)))

    receipts = executor.classify_conflicts(results, observed_target_head="moved", expected_target_head=HEAD)

    kinds = {(receipt.left, receipt.right, receipt.kind) for receipt in receipts}
    assert ("target", "candidate", "stale_base") in kinds
    assert ("a", "policy", "protected") in kinds
    assert ("a", "b", "overlap") in kinds


def test_the_executor_never_starts_a_thread_when_it_is_not_parallel() -> None:
    """The serial path is the supported one; it must not depend on a pool being available."""
    before = threading.active_count()
    nodes = (WorkNode(id="a", files=("a.py",)), WorkNode(id="b", depends_on=("a",), files=("b.py",)))

    _execute(WorkExecutor(_runner(), _merger), nodes, supports_fork=False)

    assert threading.active_count() == before
