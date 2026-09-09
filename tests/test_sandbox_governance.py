from __future__ import annotations

import pytest

from swfactory.sandbox_governance import (
    CleanupDebt,
    CleanupDecision,
    ReplayCandidate,
    ReplayStrategy,
    ResourceObservation,
    SandboxIdentity,
    WorkNodeResult,
    authorize_cleanup,
    authorize_fan_in,
    choose_replay_winner,
    classify_workgraph_conflicts,
)


def identity() -> SandboxIdentity:
    return SandboxIdentity("docker", "cell_0123456789abcdef01234567", 2, "attempt-1")


def observation(item: SandboxIdentity, *, running: bool = False) -> ResourceObservation:
    return ResourceObservation("container-1", item.labels, running)


def test_owned_resource_cleanup_is_epoch_and_activity_fenced() -> None:
    item = identity()
    assert authorize_cleanup(item, observation(item), current_epoch=2, active=True) == CleanupDecision.KEEP
    assert authorize_cleanup(item, observation(item), current_epoch=2, active=False) == CleanupDecision.REMOVE
    assert authorize_cleanup(item, observation(item), current_epoch=3, active=False) == CleanupDecision.REMOVE
    assert (
        authorize_cleanup(item, ResourceObservation("x", {}, False), current_epoch=2, active=False)
        == CleanupDecision.REFUSE
    )


def test_cleanup_debt_cannot_be_settled_by_another_cell() -> None:
    debt = CleanupDebt()
    item = identity()
    debt.record("container-1", item)
    with pytest.raises(RuntimeError, match="does not match"):
        debt.settle("container-1", SandboxIdentity("docker", "cell_other", 2, "attempt-1"))
    debt.settle("container-1", item)
    assert not debt.outstanding


def test_workgraph_parallel_safety_and_observed_paths_are_enforced() -> None:
    left = WorkNodeResult("a", True, frozenset({"a.py"}), frozenset({"a.py"}), "h0", "h1", "cell", 1)
    right = WorkNodeResult("b", True, frozenset({"b.py"}), frozenset({"b.py"}), "h0", "h2", "cell", 1)
    authorize_fan_in([left, right])
    overlap = WorkNodeResult("b", True, frozenset({"a.py"}), frozenset({"a.py"}), "h0", "h2", "cell", 1)
    assert any(row.kind == "sibling-overlap" for row in classify_workgraph_conflicts([left, overlap]))
    with pytest.raises(RuntimeError, match="fan-in refused"):
        authorize_fan_in([left, overlap])


def test_parallel_unsafe_node_never_joins_a_parallel_wave() -> None:
    unsafe = WorkNodeResult("a", False, frozenset({"a.py"}), frozenset({"a.py"}), "h0", "h1", "cell", 1)
    safe = WorkNodeResult("b", True, frozenset({"b.py"}), frozenset({"b.py"}), "h0", "h2", "cell", 1)
    assert any(row.kind == "parallel-unsafe" for row in classify_workgraph_conflicts([unsafe, safe]))


def test_time_machine_uses_same_frozen_input_and_prefers_correct_low_cost_future() -> None:
    rows = [
        ReplayCandidate(ReplayStrategy.REPAIR, "input", "a", True, 100, 2.0, "a" * 64),
        ReplayCandidate(ReplayStrategy.REPLAN, "input", "b", True, 200, 1.0, "b" * 64),
        ReplayCandidate(ReplayStrategy.RESTART, "input", "c", False, 10, 0.1, "c" * 64),
    ]
    result = choose_replay_winner(rows)
    assert result.winner.strategy == ReplayStrategy.REPLAN
    bad = list(rows)
    bad[2] = ReplayCandidate(ReplayStrategy.RESTART, "different", "c", False, 10, 0.1, "c" * 64)
    with pytest.raises(ValueError, match="frozen input"):
        choose_replay_winner(bad)
