from __future__ import annotations

import random

import pytest

from swfactory.idempotency import MutationOutcome, OperationJournal
from swfactory.mutation_contract import (
    ExternalMutation,
    StaleMutation,
    external_mutation_kinds,
    mutation_ref,
    require_current_epoch,
)

SEEDS = (3, 17, 41, 73, 101, 211, 509, 997)


def test_registry_covers_the_external_side_effect_surface() -> None:
    assert {kind.value for kind in external_mutation_kinds()} == {
        "sandbox_allocate",
        "sandbox_reclaim",
        "github_publish",
        "evidence_seal",
        "approval_answer",
        "cleanup_reconcile",
        "generation_promote",
    }


@pytest.mark.parametrize("seed", SEEDS)
def test_epoch_and_operation_identity_properties(seed: int) -> None:
    rng = random.Random(seed)
    trace: list[str] = []
    for _index in range(64):
        kind = rng.choice(external_mutation_kinds())
        epoch = rng.randint(1, 25)
        cell_id = f"cell_{rng.getrandbits(96):024x}"
        logical_key = f"logical-{rng.randint(0, 9)}"
        trace.append(f"{kind.value}:{cell_id}:{epoch}:{logical_key}")
        a = mutation_ref(kind, cell_id, epoch, logical_key)
        b = mutation_ref(kind, cell_id, epoch, logical_key)
        newer = mutation_ref(kind, cell_id, epoch + 1, logical_key)
        assert a == b, f"seed={seed} trace={trace}"
        assert a.key != newer.key, f"seed={seed} trace={trace}"
        require_current_epoch(a, epoch)
        with pytest.raises(StaleMutation):
            require_current_epoch(a, epoch + 1)
        with pytest.raises(StaleMutation):
            require_current_epoch(newer, epoch)


def test_committed_replay_never_calls_external_side_effect_twice(tmp_path) -> None:
    journal = OperationJournal(tmp_path / "ops.sqlite3")
    calls: list[str] = []
    try:
        for kind in external_mutation_kinds():
            ref = mutation_ref(kind, "cell_property", 7, "same-logical-op")

            def apply(kind: ExternalMutation = kind) -> dict[str, str]:
                calls.append(kind.value)
                return {"kind": kind.value}

            assert journal.execute(ref, apply) == {"kind": kind.value}
            assert journal.execute(ref, apply) == {"kind": kind.value}
        assert calls == [kind.value for kind in external_mutation_kinds()]
    finally:
        journal.close()


def test_ambiguous_previous_write_is_observed_before_replay(tmp_path) -> None:
    journal = OperationJournal(tmp_path / "ops.sqlite3")
    ref = mutation_ref(ExternalMutation.GITHUB_PUBLISH, "cell_ambiguous", 2, "pr-42")
    journal.begin(ref)
    observations: list[str] = []
    writes: list[str] = []
    try:
        result = journal.execute(
            ref,
            lambda: writes.append("write") or {"pr": 42},
            replay_safe=True,
            reconcile=lambda: (
                observations.append("observe") or MutationOutcome("definitely_absent", evidence={"pr": 42})
            ),
        )
        assert result == {"pr": 42}
        assert observations == ["observe"]
        assert writes == ["write"]
    finally:
        journal.close()


@pytest.mark.parametrize(
    ("cell_id", "epoch", "parts"),
    [("wrong", 1, ("x",)), ("cell_x", 0, ("x",)), ("cell_x", -1, ("x",)), ("cell_x", 1, ()), ("cell_x", 1, ("",))],
)
def test_invalid_mutation_identity_fails_closed(cell_id: str, epoch: int, parts: tuple[str, ...]) -> None:
    with pytest.raises((ValueError, TypeError)):
        mutation_ref(ExternalMutation.EVIDENCE_SEAL, cell_id, epoch, *parts)
