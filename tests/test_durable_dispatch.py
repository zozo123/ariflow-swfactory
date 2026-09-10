"""Queued work must actually reach Airflow, exactly once, and survive a restart at any boundary.

The failure this module exists for (#2058): with capacity one, submitting A then B queued B, and
completing A drained B into an "active" admission with no Factory Cell behind it. Nothing ever
dispatched B, retrying it answered 409, and the operator watched a queue that claimed to be moving.

Everything here is hermetic. ``FakeAirflow`` answers the four calls the backend really makes and
counts the POSTs, which is the only honest way to say "exactly once". A restart is a real restart:
the ``Factory`` is closed and a new one is opened on the same state directory, so anything the tests
observe afterwards came out of SQLite rather than out of a live object.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any

import pytest

from swfactory import cell_callback
from swfactory.admission import Limits, Priority
from swfactory.backend.service import Factory, Refused
from swfactory.cells import CellIdentity, CellStore
from swfactory.durable_admission import (
    MAX_DISPATCH_ATTEMPTS,
    DispatchLeaseLost,
    DurableAdmission,
    MemberSpec,
    WorkOrderConflict,
)

TOKEN = "t" * 40
AIRFLOW_URL = "https://airflow.invalid:8080"

LINE = """
[blueprint]
name = "line"
version = 1
description = "test line"

[trigger]
kind = "manual"

[[targets]]
repo = "owner/one"
dir = "a"
base_branch = "main"

[stages]
order = ["intent", "spec", "plan", "build_and_test", "review", "deliver"]

[[gates]]
after = "intent"
artifact = "intent.md"
timeout_h = 1
assigned = []
auto = true

[limits]
max_build_iterations = 3
max_review_fixes = 1
max_turns = 40
budget_usd_per_stage = 2.0
budget_usd = 8.0
stage_timeout_h = 1
max_parallel_jobs = 2

[review]
policy = "REVIEW.md"
nit_cap = 3

[sandbox]
kind = "local"
ttl_s = 7200
idle_s = 900

[deliver]
labels = ["factory"]
"""

SECOND_TARGET = """
[[targets]]
repo = "owner/two"
dir = "b"
base_branch = "main"
"""

SAME_REPO_TARGET = """
[[targets]]
repo = "owner/one"
dir = "b"
base_branch = "main"
"""


class FakeAirflow:
    """The four calls the backend makes, plus a truthful record of which runs really exist.

    ``posts`` counts attempts and ``runs`` holds the runs a POST actually created; a refused POST
    creates nothing, which is what makes the next attempt's reconcile answer ``definitely_absent``.
    """

    def __init__(self) -> None:
        self.posts: list[dict[str, Any]] = []
        self.created: list[dict[str, Any]] = []
        self.runs: dict[str, dict[str, Any]] = {}
        self.post_hook = None
        self.post_status = 200

    def __call__(self, method: str, path: str, body: dict | None) -> tuple[int, Any]:
        if path.count("/") == 2 and method in {"GET", "PATCH"}:
            return 200, {"is_paused": False}
        if method == "POST" and path.endswith("/dagRuns"):
            run_id = str(body["dag_run_id"])
            work_id = str(body["conf"]["_factory_submission_id"])
            self.posts.append({"dag_run_id": run_id, "work_id": work_id, "conf": body["conf"]})
            if self.post_hook is not None:
                self.post_hook(run_id)
            if self.post_status >= 300:
                return self.post_status, {"detail": "airflow refused"}
            if run_id in self.runs:
                # Airflow refuses a duplicate deterministic run id; a second dispatch must be
                # visible as a conflict rather than as a silently repeated run.
                return 409, {"detail": "duplicate dag_run_id"}
            self.runs[run_id] = {"dag_run_id": run_id, "state": "queued"}
            self.created.append({"dag_run_id": run_id, "work_id": work_id})
            return 200, self.runs[run_id]
        if method == "GET" and "/dagRuns/" in path:
            run_id = urllib.parse.unquote(path.rsplit("/", 1)[1])
            if run_id in self.runs:
                return 200, self.runs[run_id]
            return 404, {"detail": "absent"}
        raise AssertionError(f"unexpected Airflow call {method} {path}")

    def created_for(self, work_id: str) -> list[str]:
        return [run["dag_run_id"] for run in self.created if run["work_id"] == work_id]


class Backend:
    """One backend on one state directory, restartable in place."""

    def __init__(self, root: Path, limits: Limits) -> None:
        self.root = root
        self.limits = limits
        self.airflow = FakeAirflow()
        self.factory = self._open()

    def _open(self) -> Factory:
        factory = Factory(token=TOKEN, airflow_url=AIRFLOW_URL, root=self.root, state_root=self.root / ".factory")
        factory.control.admission.limits = self.limits
        # A crashed process cannot hand its dispatch lease back, so redelivery waits for the lease
        # to expire. These tests restart instantly, so the lease has to expire instantly too.
        factory.control.admission.dispatch_lease_s = 0.0
        # Likewise a lost report is only looked for once the Cell's last write is older than the
        # read floor; these tests lose it and restart within the same second.
        factory.reconcile_interval_s = 0.0
        factory.airflow = self.airflow  # type: ignore[method-assign]
        return factory

    def restart(self) -> Factory:
        self.factory.close()
        self.factory = self._open()
        return self.factory

    def close(self) -> None:
        self.factory.close()

    # ---------------------------------------------------------------- helpers

    def submit(self, *issues: str, actor: str = "op", targets: list[str] | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"line": "line", "issues": list(issues), "actor": actor}
        if targets is not None:
            body["targets"] = targets
        return self.factory.submit(body)

    def finish(self, cell_id: str, state: str = "success") -> dict[str, Any]:
        epoch = int(self.factory.cell_store.get(cell_id)["epoch"])
        return self.factory._transition(
            {"cell_id": cell_id, "epoch": epoch, "state": state, "operation_key": f"airflow:{state}:{cell_id}"}
        )

    def admission_state(self, work_id: str) -> str | None:
        return self.factory.control.admission.state_of(work_id)


def _line(root: Path, *extra: str) -> None:
    (root / "blueprints").mkdir(parents=True, exist_ok=True)
    (root / "blueprints" / "line.toml").write_text(LINE + "".join(extra))


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A factory whose blueprint lives in the cwd, since that is how blueprints resolve."""

    def build(limits: Limits, *extra: str) -> Backend:
        _line(tmp_path, *extra)
        monkeypatch.chdir(tmp_path)
        return Backend(tmp_path, limits)

    return build


# --------------------------------------------------------------- the reported failure


def test_capacity_one_dispatches_the_queued_work_when_the_first_run_finishes(backend) -> None:
    """The four reproduction steps from the issue, end to end."""
    box = backend(Limits(global_active=1))
    first = box.submit("1")
    assert first["state"] == "submitted"
    second = box.submit("2")
    assert second["state"] == "queued"
    assert len(box.airflow.created) == 1

    box.finish(first["cells"][0])

    assert len(box.airflow.created) == 2, "B was admitted but never dispatched"
    work_id = second["submission_id"]
    assert box.admission_state(work_id) == "bound"
    members = box.factory.control.admission.members(work_id)
    assert [m.cell_epoch for m in members] == [1], "a bound admission must carry its real Cell epoch"
    cell = box.factory.cell_store.get(members[0].cell_id)
    assert cell["state"] == "queued" and cell["airflow_run_id"] == box.airflow.created[1]["dag_run_id"]


def test_resubmitting_the_drained_work_answers_with_its_run_instead_of_409(backend) -> None:
    """The operator's retry used to hit "active admission has no corresponding Factory Cell"."""
    box = backend(Limits(global_active=1))
    first = box.submit("1")
    box.submit("2")
    box.finish(first["cells"][0])
    again = box.submit("2")
    assert again["state"] == "submitted"
    assert again["run_id"] == box.airflow.created[1]["dag_run_id"]
    assert len(box.airflow.created) == 2, "a retry must not create a second run"


BOUNDARIES = ("after_first_submit", "after_queueing", "after_release", "mid_activation", "after_airflow_post")


@pytest.mark.parametrize("boundary", BOUNDARIES)
def test_the_queued_work_is_dispatched_exactly_once_with_a_restart_at_every_boundary(backend, boundary: str) -> None:
    """Whatever the crash point, B ends up with one Airflow run and one bound Cell."""
    box = backend(Limits(global_active=1))
    first = box.submit("1")
    if boundary == "after_first_submit":
        box.restart()
    second = box.submit("2")
    assert second["state"] == "queued"
    if boundary == "after_queueing":
        box.restart()

    if boundary == "mid_activation":
        # Crash between admitting B and delivering its command: the drain lands, the delivery does
        # not, and only the restarted backend's resume may finish it. A's transition still succeeds,
        # because one work order's Airflow call must not fail another's lifecycle report.
        box.airflow.post_status = 503
        report = box.finish(first["cells"][0])
        assert report["released_work"] == [second["submission_id"]], "A's terminal Cell admits B"
        assert report["resumed_dispatch"] == [
            {"work_id": second["submission_id"], "dispatched": False, "detail": report["resumed_dispatch"][0]["detail"]}
        ]
        box.airflow.post_status = 200
        box.restart()
        box.factory.resume_dispatch()
    else:
        box.finish(first["cells"][0])

    if boundary == "after_release":
        box.restart()
        box.factory.resume_dispatch()
    if boundary == "after_airflow_post":
        box.restart()
        box.factory.resume_dispatch()

    work_id = second["submission_id"]
    runs = box.airflow.created_for(work_id)
    assert len(runs) == 1, f"B ended up with {len(runs)} Airflow runs across a restart at {boundary}"
    assert box.admission_state(work_id) == "bound"
    cells = [member.cell_id for member in box.factory.control.admission.members(work_id)]
    assert box.factory.cell_store.get(cells[0])["airflow_run_id"] == runs[0]


def test_a_restart_keeps_the_queued_payload_and_separates_waiting_from_running(backend) -> None:
    box = backend(Limits(global_active=2))
    box.submit("1")
    queued = box.submit("2", "3")
    box.restart()

    order = box.factory.control.admission.work_order(queued["submission_id"]).payload
    assert order["issues"] == ["2", "3"]
    assert [job["issue"] for job in order["jobs"]] == ["2", "3"]
    assert all(job["policy_digest"].startswith("policy:") for job in order["jobs"])
    assert order["blueprint"]["identity"].startswith("policy:")
    assert order["request_digest"].startswith("sha256:")

    snapshot = box.factory.control.admission.snapshot()
    assert snapshot["capacity_unit"] == "factory_cell_activation"
    assert snapshot["pressure"]["queued"] == 1
    assert snapshot["pressure"]["bound"] == 1
    assert snapshot["pressure"]["awaiting_dispatch"] == 0
    assert snapshot["queued"][0]["state"] == "queued"
    assert snapshot["active"][0]["state"] == "bound"
    assert snapshot["active"][0]["dispatch"]["state"] == "delivered"


# --------------------------------------------------------------- capacity accounting


def test_a_multi_job_submission_holds_every_unit_until_its_last_member_finishes(backend) -> None:
    """One Cell going terminal used to release the whole reservation and strand its siblings."""
    box = backend(Limits(global_active=2, per_repo_active=2), SECOND_TARGET)
    multi = box.submit("1")
    assert len(multi["cells"]) == 2
    queued = box.submit("2")
    assert queued["state"] == "queued"

    box.finish(multi["cells"][0])
    assert box.admission_state(multi["submission_id"]) in {"bound", "dispatching"}
    assert box.admission_state(queued["submission_id"]) == "queued", "a sibling was still running"
    assert len(box.airflow.created) == 1

    box.finish(multi["cells"][1])
    assert box.admission_state(multi["submission_id"]) == "success"
    assert box.admission_state(queued["submission_id"]) == "bound"
    assert len(box.airflow.created) == 2


def test_every_repository_counts_against_its_own_quota(backend) -> None:
    box = backend(Limits(global_active=8, per_repo_active=1), SECOND_TARGET)
    spread = box.submit("1")
    assert spread["state"] == "submitted", "one unit in each of two repositories fits a per-repo limit of one"
    blocked = box.submit("2")
    assert blocked["state"] == "queued"
    assert blocked["limiting"].dimension == "repo"


def test_two_jobs_in_one_repository_need_two_units_of_that_repository(backend) -> None:
    box = backend(Limits(global_active=8, per_repo_active=1), SAME_REPO_TARGET)
    decision = box.submit("1")
    assert decision["state"] == "rejected"
    assert decision["reason"] == "capacity_impossible"
    assert decision["limiting"].dimension == "repo"
    assert not box.airflow.posts


# --------------------------------------------------------------- duplicates and races


def test_a_duplicate_immutable_request_reuses_the_one_reservation(backend) -> None:
    box = backend(Limits(global_active=1))
    box.submit("1")
    queued = box.submit("2")
    again = box.submit("2")
    assert again["submission_id"] == queued["submission_id"]
    assert again["reason"] == "duplicate_queued"
    admission = box.factory.control.admission
    assert admission.snapshot()["pressure"]["queued"] == 1
    assert len(admission.members(queued["submission_id"])) == 1


def test_concurrent_admissions_cannot_exceed_the_declared_capacity(tmp_path: Path) -> None:
    """Capacity is inspected and reserved inside one write transaction, so only one request wins."""
    admission = DurableAdmission(tmp_path / "admission.sqlite3", Limits(global_active=1))
    started = threading.Barrier(8)
    outcomes: list[str] = []
    lock = threading.Lock()

    def run(index: int) -> None:
        order = {"schema_version": 1, "line": "line", "actor": "op", "n": index}
        started.wait()
        decision = admission.submit(
            work_id=f"work-{index}",
            actor="op",
            blueprint="line",
            order=order,
            members=[MemberSpec(0, "owner/one", f"cell_{index}")],
        )
        with lock:
            outcomes.append(decision.state)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert outcomes.count("admitted") == 1
    assert outcomes.count("queued") == 7
    admission.close()


def test_one_work_id_cannot_be_reused_for_a_different_work_order(tmp_path: Path) -> None:
    admission = DurableAdmission(tmp_path / "admission.sqlite3")
    member = [MemberSpec(0, "owner/one", "cell_a")]
    admission.submit(
        work_id="work", actor="op", blueprint="line", order={"schema_version": 1, "issue": "1"}, members=member
    )
    with pytest.raises(WorkOrderConflict):
        admission.submit(
            work_id="work", actor="op", blueprint="line", order={"schema_version": 1, "issue": "2"}, members=member
        )
    admission.close()


# --------------------------------------------------------------- partial failure


def test_a_batch_that_cannot_activate_every_cell_compensates_its_own_activations(backend) -> None:
    """No invisible Cell, no leaked unit: the half-built batch is undone and the queue moves on."""
    box = backend(Limits(global_active=4, per_repo_active=4), SECOND_TARGET)
    # A sibling harness owns the second target's Cell, so the batch can never complete.
    stolen = CellStore(box.root / ".factory" / "cells.sqlite3")
    stolen.activate(CellIdentity("owner/two", "b@main", "1"), actor="other:harness")
    stolen.close()

    with pytest.raises(Refused):
        box.submit("1")
    admission = box.factory.control.admission
    held = admission.snapshot()["active"]
    assert len(held) == 1, "the half-activated order is the only thing holding capacity"
    work_id = held[0]["work_id"]
    for _ in range(MAX_DISPATCH_ATTEMPTS):
        box.factory.resume_dispatch()

    assert admission.state_of(work_id) == "failed"
    mine, theirs = admission.members(work_id)
    assert box.factory.cell_store.get(mine.cell_id)["state"] == "cancelled", (
        "the Cell this order activated must not stay live with no work order owning it"
    )
    assert box.factory.cell_store.get(theirs.cell_id)["state"] == "dispatching", (
        "compensation must never touch a Cell another harness owns"
    )
    assert admission.snapshot()["pressure"]["held_units"] == 0
    assert not box.airflow.posts, "nothing may be sent to Airflow for a batch that never assembled"


# --------------------------------------------------------------- cancellation


def test_cancelling_queued_work_releases_the_queue_without_ever_dispatching_it(backend) -> None:
    box = backend(Limits(global_active=1))
    first = box.submit("1")
    queued = box.submit("2")
    result = box.factory.operation("/queue/cancel", {"work_id": queued["submission_id"], "reason": "operator"})
    assert result["was"] == "queued" and result["state"] == "cancelled"

    box.finish(first["cells"][0])
    assert len(box.airflow.created) == 1, "cancelled work must not be dispatched by a later drain"
    assert box.admission_state(queued["submission_id"]) == "cancelled"


def test_cancelling_admitted_work_prevents_the_later_stale_dispatch(backend) -> None:
    """Admitted-awaiting-dispatch is a real state, and cancelling it really stops the delivery."""
    box = backend(Limits(global_active=1))
    first = box.submit("1")
    queued = box.submit("2")
    box.airflow.post_status = 503
    assert box.finish(first["cells"][0])["resumed_dispatch"][0]["dispatched"] is False
    box.airflow.post_status = 200
    assert box.admission_state(queued["submission_id"]) == "admitted"

    box.factory.operation("/queue/cancel", {"work_id": queued["submission_id"], "reason": "operator"})
    box.restart()
    assert box.factory.resume_dispatch() == []
    assert box.airflow.created_for(queued["submission_id"]) == []
    assert len(box.airflow.runs) == 1
    assert box.admission_state(queued["submission_id"]) == "cancelled"


def test_cancelling_while_dispatching_keeps_the_unproven_remote_outcome(backend) -> None:
    """A withdrawal during an in-flight POST must not be recorded as if it were clean."""
    box = backend(Limits(global_active=1))
    admission = box.factory.control.admission
    holder: dict[str, str] = {}
    original = box.factory._dispatch_intent

    def spy(intent):
        # The work id is only known once the order is durable, and the cancel has to land while the
        # POST is in flight, so the delivery itself hands the id to the hook below.
        holder["work_id"] = intent.work_id
        return original(intent)

    def cancel_midflight(run_id: str) -> None:
        box.factory.control.cancel_reservation(holder["work_id"], reason="operator cancelled mid-dispatch")

    box.factory._dispatch_intent = spy  # type: ignore[method-assign]
    box.airflow.post_hook = cancel_midflight
    box.airflow.post_status = 500
    with pytest.raises(Refused):
        box.submit("1")

    work_id = holder["work_id"]
    assert admission.state_of(work_id) == "cancelled"
    dispatch = admission.dispatch_row(work_id)
    assert dispatch["state"] == "abandoned"
    assert dispatch["observation"]["remote"] == "unknown", "an unproven Airflow outcome must stay on the record"
    assert "unknown" in dispatch["last_error"]
    assert admission.snapshot()["pressure"]["held_units"] == 0
    assert box.airflow.created_for(work_id) == []


def test_a_delivery_that_lost_its_lease_cannot_bind_cells_afterwards(tmp_path: Path) -> None:
    admission = DurableAdmission(tmp_path / "admission.sqlite3")
    decision = admission.submit(
        work_id="work",
        actor="op",
        blueprint="line",
        order={"schema_version": 1, "issue": "1"},
        members=[MemberSpec(0, "owner/one", "cell_a")],
    )
    # A fresh admission hands its delivery to the submitter inside the admission transaction, so
    # nothing else can claim it -- claim_dispatch here would (correctly) answer None.
    intent = decision.intent
    assert intent is not None
    assert admission.claim_dispatch("work") is None
    admission.cancel("work", reason="operator")
    with pytest.raises(DispatchLeaseLost):
        admission.record_member_epoch("work", 0, 1, token=intent.lease_token)
    with pytest.raises(DispatchLeaseLost):
        admission.record_dispatch("work", token=intent.lease_token, dag_run_id="swf__x")
    assert admission.pending_dispatch() == []
    admission.close()


# --------------------------------------------------------------- legacy state


def test_a_legacy_active_row_is_repaired_instead_of_holding_capacity_forever(tmp_path: Path) -> None:
    """The rows this bug already created must not keep a restarted backend's queue blocked."""
    path = tmp_path / "admission.sqlite3"
    seed = DurableAdmission(path, Limits(global_active=1))
    seed.close()
    db = sqlite3.connect(path)
    now = time.time()
    db.execute(
        """INSERT INTO admission_work(work_id,repo,actor,blueprint,priority,sequence,state,reason,
           cell_id,cell_epoch,enqueued_at,admitted_at,updated_at)
           VALUES('stranded','owner/one','op','line',?,900,'active','drained',NULL,NULL,?,?,?)""",
        (int(Priority.NORMAL), now, now, now),
    )
    db.execute(
        """INSERT INTO admission_work(work_id,repo,actor,blueprint,priority,sequence,state,reason,
           cell_id,cell_epoch,enqueued_at,admitted_at,updated_at)
           VALUES('running','owner/one','op','line',?,901,'active','admitted','cell_x',3,?,?,?)""",
        (int(Priority.NORMAL), now, now, now),
    )
    db.commit()
    db.close()

    admission = DurableAdmission(path, Limits(global_active=1))
    assert admission.state_of("stranded") == "cancelled"
    assert admission.state_of("running") == "bound"
    assert admission.members_for_cell("cell_x", 3) == ["running"]
    assert admission.snapshot()["pressure"]["held_units"] == 1
    admission.close()


# --------------------------------------------------------------- the lifecycle callback


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._raw = json.dumps(payload).encode()
        self.status = 200

    def read(self, _limit: int | None = None) -> bytes:
        return self._raw

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def _managed_job() -> dict[str, Any]:
    return {"cell_managed": True, "cell_id": "cell_abc", "cell_epoch": 2}


def test_the_callback_reports_the_resumed_dispatch_and_does_not_perform_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """An Airflow worker may carry the news that queued work resumed; it may not be what resumes it."""
    monkeypatch.setenv("SWF_BACKEND_URL", "https://backend.invalid")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "b" * 40)
    body = {
        "cell": {"cell_id": "cell_abc", "epoch": 2, "state": "success"},
        "released_work": ["submit_b"],
        "resumed_dispatch": [{"work_id": "submit_b", "dispatched": True, "run_id": "swf__b"}],
    }
    monkeypatch.setattr(cell_callback.urllib.request, "urlopen", lambda *a, **k: _Response(body))
    result = cell_callback.transition(_managed_job(), "success", operation_key="airflow:success")
    assert result == {
        "cell": body["cell"],
        "released_work": ["submit_b"],
        "resumed_dispatch": body["resumed_dispatch"],
    }


def test_the_callback_refuses_an_answer_about_a_different_cell_epoch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWF_BACKEND_URL", "https://backend.invalid")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "b" * 40)
    body = {"cell": {"cell_id": "cell_abc", "epoch": 3, "state": "success"}, "released_work": []}
    monkeypatch.setattr(cell_callback.urllib.request, "urlopen", lambda *a, **k: _Response(body))
    with pytest.raises(cell_callback.CellCallbackError):
        cell_callback.transition(_managed_job(), "success", operation_key="airflow:success")


def test_a_dispatched_work_order_is_not_cancellable_behind_its_running_cells(backend) -> None:
    """Releasing a bound reservation from the queue side would free capacity the run still uses."""
    box = backend(Limits(global_active=1))
    first = box.submit("1")
    with pytest.raises(Refused) as refusal:
        box.factory.operation("/queue/cancel", {"work_id": first["submission_id"], "reason": "operator"})
    assert refusal.value.status == 409
    assert box.admission_state(first["submission_id"]) == "bound"
    assert box.factory.control.admission.snapshot()["pressure"]["held_units"] == 1
