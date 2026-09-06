from __future__ import annotations

import threading
from pathlib import Path

import pytest

from swfactory.cell_runtime import bind_jobs, identity_for_job
from swfactory.cells import CellBusy, CellStore, StaleEpoch


def _job(index: int = 0) -> dict[str, object]:
    return {
        "issue": "42",
        "repo": "acme/widgets",
        "dir": "services/api",
        "base_branch": "main",
        "job_idx": index,
    }


def test_cell_identity_and_binding_are_deterministic(tmp_path: Path) -> None:
    store = CellStore(tmp_path / "cells.sqlite3")
    try:
        identity = identity_for_job(_job())
        cell = store.activate(identity, actor="test")
        rows = bind_jobs(
            [_job()],
            [{"job_idx": 0, "cell_id": cell["cell_id"], "epoch": cell["epoch"]}],
        )
        assert rows == [
            {
                **_job(),
                "cell_id": identity.stable_id(),
                "cell_epoch": 1,
                "cell_managed": True,
            }
        ]
        assert store.history(cell["cell_id"])[0]["kind"] == "activated"
    finally:
        store.close()


def test_direct_binding_is_descriptive_not_managed() -> None:
    row = bind_jobs([_job()])[0]
    assert row["cell_id"] == identity_for_job(_job()).stable_id()
    assert row["cell_epoch"] == 1
    assert row["cell_managed"] is False


def test_binding_rejects_wrong_cell_or_epoch() -> None:
    with pytest.raises(ValueError, match="binding mismatch"):
        bind_jobs([_job()], [{"job_idx": 0, "cell_id": "cell_wrong", "epoch": 1}])
    with pytest.raises(ValueError, match="epoch must be positive"):
        bind_jobs(
            [_job()],
            [{"job_idx": 0, "cell_id": identity_for_job(_job()).stable_id(), "epoch": 0}],
        )


def test_active_cell_cannot_be_double_dispatched(tmp_path: Path) -> None:
    store = CellStore(tmp_path / "cells.sqlite3")
    identity = identity_for_job(_job())
    try:
        first = store.activate(identity, actor="one")
        assert first["state"] == "dispatching"
        with pytest.raises(CellBusy):
            store.activate(identity, actor="two")
    finally:
        store.close()


def test_terminal_reactivation_advances_epoch_and_fences_old_writer(tmp_path: Path) -> None:
    store = CellStore(tmp_path / "cells.sqlite3")
    identity = identity_for_job(_job())
    try:
        first = store.activate(identity, actor="one")
        store.patch(first["cell_id"], 1, "finish:1", state="success")
        second = store.activate(identity, actor="two")
        assert second["epoch"] == 2
        assert second["state"] == "dispatching"
        with pytest.raises(StaleEpoch):
            store.patch(first["cell_id"], 1, "stale-write", state="failed")
    finally:
        store.close()


def test_two_threads_cannot_claim_the_same_fresh_cell(tmp_path: Path) -> None:
    store = CellStore(tmp_path / "cells.sqlite3")
    identity = identity_for_job(_job())
    barrier = threading.Barrier(3)
    outcomes: list[str] = []

    def claim(actor: str) -> None:
        barrier.wait()
        try:
            store.activate(identity, actor=actor)
            outcomes.append("won")
        except CellBusy:
            outcomes.append("busy")

    threads = [threading.Thread(target=claim, args=(name,)) for name in ("one", "two")]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()
    try:
        assert sorted(outcomes) == ["busy", "won"]
        assert store.get(identity.stable_id())["epoch"] == 1
    finally:
        store.close()
