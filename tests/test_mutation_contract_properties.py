from __future__ import annotations

import random
from pathlib import Path

import pytest

from swfactory.cells import CellIdentity, CellStore, DuplicateOperation, StaleEpoch
from swfactory.idempotency import OperationJournal
from swfactory.mutation_contract import EXTERNAL_MUTATION_KINDS, cell_mutation, mutation_ref

SEEDS = (7, 17, 29, 43, 71, 101, 137, 211)


def _identity(seed: int) -> CellIdentity:
    return CellIdentity(repo="owner/repo", target="owner/repo", issue=str(10_000 + seed))


def test_every_external_mutation_kind_has_epoch_sensitive_deterministic_identity() -> None:
    cell_id = _identity(1).stable_id()
    for kind in EXTERNAL_MUTATION_KINDS:
        first = mutation_ref(cell_id, 3, kind, "logical-op")
        replay = mutation_ref(cell_id, 3, kind, "logical-op")
        next_epoch = mutation_ref(cell_id, 4, kind, "logical-op")
        assert first == replay
        assert first.key != next_epoch.key
        assert first.cell_id == cell_id
        assert first.epoch == 3
        assert first.kind == kind.value


def test_randomized_cell_traces_refuse_stale_and_duplicate_writers(tmp_path: Path) -> None:
    for seed in SEEDS:
        rng = random.Random(seed)
        store = CellStore(tmp_path / f"cells-{seed}.sqlite3")
        try:
            identity = _identity(seed)
            cell = store.ensure(identity)
            cell_id = cell["cell_id"]
            epoch = int(cell["epoch"])

            for step in range(60):
                kind = rng.choice(EXTERNAL_MUTATION_KINDS)
                logical = f"seed-{seed}-step-{step}"
                mutation = cell_mutation(cell_id, epoch, kind, logical, payload={"seed": seed, "step": step})
                store.record(mutation)

                with pytest.raises(DuplicateOperation):
                    store.record(mutation)

                if rng.random() < 0.2:
                    stale_epoch = epoch
                    epoch = store.take_epoch(cell_id, epoch, actor=f"property-seed-{seed}")
                    stale = cell_mutation(cell_id, stale_epoch, kind, logical + "-stale")
                    with pytest.raises(StaleEpoch):
                        store.record(stale)
        finally:
            store.close()


def test_operation_journal_replays_committed_result_without_second_side_effect(tmp_path: Path) -> None:
    for seed in SEEDS:
        journal = OperationJournal(tmp_path / f"operations-{seed}.sqlite3")
        try:
            cell_id = _identity(seed).stable_id()
            calls: dict[str, int] = {kind.value: 0 for kind in EXTERNAL_MUTATION_KINDS}

            for kind in EXTERNAL_MUTATION_KINDS:
                ref = mutation_ref(cell_id, 1, kind, f"seed-{seed}")

                def side_effect(
                    kind_value: str = kind.value,
                    calls_ref: dict[str, int] = calls,
                    seed_value: int = seed,
                ) -> dict[str, object]:
                    calls_ref[kind_value] += 1
                    return {"kind": kind_value, "seed": seed_value}

                first = journal.execute(ref, side_effect)
                replay = journal.execute(ref, side_effect)
                assert replay == first
                assert calls[kind.value] == 1
        finally:
            journal.close()


def test_external_mutation_identity_rejects_invalid_cell_epoch_and_key_parts() -> None:
    kind = EXTERNAL_MUTATION_KINDS[0]
    cell_id = _identity(5).stable_id()
    with pytest.raises(ValueError):
        mutation_ref("not-a-cell", 1, kind, "op")
    with pytest.raises(ValueError):
        mutation_ref(cell_id, 0, kind, "op")
    with pytest.raises(ValueError):
        mutation_ref(cell_id, 1, kind)
    with pytest.raises(ValueError):
        mutation_ref(cell_id, 1, kind, "")
