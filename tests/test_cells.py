from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from swfactory.cell_runtime import bind_jobs, identity_for_job
from swfactory.cells import CellBusy, CellStore, SCHEMA_VERSION, StaleEpoch


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


def test_python_accepts_the_shared_factory_cell_v1_wire_fixture() -> None:
    path = Path(__file__).parent / "fixtures" / "cells" / "factory_cell_v1.json"
    golden = json.loads(path.read_text(encoding="utf-8"))
    cell = golden["cell"]
    history = golden["history"]

    assert cell["schema_version"] == SCHEMA_VERSION == 1
    assert cell["cell_id"].startswith("cell_") and len(cell["cell_id"]) == 29
    assert cell["epoch"] >= 1
    assert cell["airflow_dag_id"] == "factory"
    assert cell["map_index"] == 7
    assert set(cell) == {
        "cell_id",
        "schema_version",
        "repo",
        "target",
        "issue",
        "epoch",
        "state",
        "airflow_dag_id",
        "airflow_run_id",
        "map_index",
        "factory_generation",
        "policy_digest",
        "base_sha",
        "observed_target_sha",
        "compute",
        "cleanup",
        "created_at",
        "updated_at",
    }
    assert [event["seq"] for event in history] == [1, 2]
    assert all(event["epoch"] == cell["epoch"] for event in history)
    assert history[0]["operation_key"] == "activation:3"
