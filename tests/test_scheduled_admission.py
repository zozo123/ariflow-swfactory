"""A cron tick admits nothing: the scheduled run must obtain its Factory Cells from the backend.

#2067: Airflow creates a scheduled run before anyone has admitted the work, so ``fan_out`` used to
hand every job ``cell_managed=False`` at epoch 1 and the line executed with none of the durable
admission, capacity accounting or epoch fencing a managed submission has -- and a partial bindings
list bound half a run. The run now submits itself through ``POST /v1/work-orders`` with
``airflow_run_id`` naming the run that already exists, and the backend binds the Cells to *that* run
instead of dispatching a second copy of the schedule.

Hermetic, in the style of ``tests/test_durable_dispatch.py``: the backend is a real ``Factory`` over
``FakeAirflow``, which records every POST so "no second run" is an assertion, and the worker client
speaks to it through a monkeypatched ``urlopen`` so the HTTP contract is the one exercised.
"""

from __future__ import annotations

import io
import json
import urllib.request
from pathlib import Path
from typing import Any

import pytest
from test_durable_dispatch import Backend, _line  # noqa: F401

from swfactory.admission import Limits
from swfactory.backend.server import _json_default
from swfactory.backend.service import Refused
from swfactory.blueprint import load
from swfactory.cell_callback import CellCallbackError
from swfactory.cell_runtime import SCHEDULE_ACTOR, admit_scheduled_run, bind_jobs

SCHEDULED = "scheduled__2026-09-10T06:17:00+00:00"


@pytest.fixture
def box(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    _line(tmp_path)
    monkeypatch.chdir(tmp_path)
    built = Backend(tmp_path, Limits(global_active=1))
    try:
        yield built
    finally:
        built.close()


def _run(box: Backend, run_id: str = SCHEDULED, *, state: str = "running", run_type: str = "scheduled") -> None:
    """A run Airflow's scheduler created on its own, the way ``FakeAirflow`` answers a GET for it."""
    box.airflow.runs[run_id] = {"dag_run_id": run_id, "dag_id": "line", "run_type": run_type, "state": state}


def _attach(box: Backend, *issues: str, run_id: str = SCHEDULED, actor: str = SCHEDULE_ACTOR) -> dict[str, Any]:
    order = {"line": "line", "actor": actor, "issues": list(issues or ("1",)), "airflow_run_id": run_id}
    return box.factory.submit(order)


# ---------------------------------------------------------------- the backend binds the run to itself


def test_a_scheduled_run_is_bound_to_itself_and_no_second_run_is_dispatched(box) -> None:
    _run(box)
    document = _attach(box)

    assert document["state"] == "submitted" and document["run_id"] == SCHEDULED
    assert box.airflow.posts == [], "attaching to the run Airflow already created must not dispatch another"
    cell = box.factory.cell_store.get(document["cells"][0])
    assert cell["state"] == "queued" and cell["airflow_run_id"] == SCHEDULED and cell["airflow_dag_id"] == "line"

    # The answer carries the complete bindings the run needs, and they bind every job managed.
    bound = bind_jobs(load("line").jobs({"issues": ["1"]}), document["bindings"])
    assert [(job["cell_managed"], job["cell_epoch"], job["cell_id"]) for job in bound] == [(True, 1, cell["cell_id"])]
    assert all(row["repo"] == "owner/one" and len(row["snapshot_digest"]) == 64 for row in document["bindings"])

    # A retried fan_out is the same trigger identity: same order, same Cells, still no dispatch.
    again = _attach(box)
    assert (again["submission_id"], again["bindings"]) == (document["submission_id"], document["bindings"])
    assert box.airflow.posts == []


def test_two_ticks_are_two_work_orders(box) -> None:
    """Yesterday's bound order must not be replayed as today's answer: the run id is part of the request."""
    _run(box)
    _run(box, "scheduled__2026-09-11T06:17:00+00:00")
    first = _attach(box)
    box.finish(first["cells"][0])
    second = _attach(box, run_id="scheduled__2026-09-11T06:17:00+00:00")
    assert second["submission_id"] != first["submission_id"]
    assert second["run_id"] == "scheduled__2026-09-11T06:17:00+00:00"
    assert second["bindings"][0]["epoch"] == 2, "the same Cell, re-armed for the new tick"


@pytest.mark.parametrize(
    ("prepare", "detail"),
    [
        (lambda box: None, "does not exist"),
        (lambda box: _run(box, run_type="manual"), "only an Airflow-scheduled run"),
        (lambda box: _run(box, state="failed"), "already failed"),
    ],
)
def test_attaching_needs_a_live_scheduled_run_airflow_itself_vouches_for(box, prepare, detail: str) -> None:
    """The caller's claim confers nothing: the backend reads the run from Airflow before activating a Cell."""
    prepare(box)
    with pytest.raises(Refused, match=detail) as info:
        _attach(box)
    assert info.value.status == 409
    assert box.factory.cell_store.list(limit=10) == [], "a refused attach activates no Cell"
    assert box.airflow.posts == []


def test_airflow_run_id_is_reserved_for_the_schedule_actor(box) -> None:
    _run(box)
    with pytest.raises(ValueError, match="airflow-schedule"):
        _attach(box, actor="operator")


def test_a_scheduled_run_behind_capacity_is_deferred_and_retired_once_the_run_is_gone(box) -> None:
    """Capacity one, held by a manual order: the tick is queued, not executed. When the manual work
    finishes but the scheduled run has already failed (fan_out refused to run unmanaged), the queued
    order is retired on Airflow's evidence and its unit released -- no Cell is bound to a dead run."""
    manual = box.submit("1")
    _run(box)
    deferred = _attach(box, "2")
    assert deferred["state"] == "queued" and "bindings" not in deferred
    assert deferred["limiting"] is not None

    box.airflow.runs[SCHEDULED]["state"] = "failed"
    box.finish(manual["cells"][0])

    assert box.admission_state(deferred["submission_id"]) == "failed"
    live = [c for c in box.factory.cell_store.list(limit=10) if c["state"] not in {"success", "failed", "cancelled"}]
    assert live == [], live
    assert len(box.airflow.posts) == 1, "only the manual order ever reached Airflow's dagRuns endpoint"


def test_a_deferred_scheduled_run_that_is_still_alive_is_bound_when_capacity_frees(box) -> None:
    manual = box.submit("1")
    _run(box)
    deferred = _attach(box, "2")
    assert deferred["state"] == "queued"

    box.finish(manual["cells"][0])

    assert box.admission_state(deferred["submission_id"]) == "bound"
    again = _attach(box, "2")
    assert again["state"] == "submitted" and again["run_id"] == SCHEDULED
    assert box.factory.cell_store.get(again["cells"][0])["airflow_run_id"] == SCHEDULED
    assert len(box.airflow.posts) == 1


# ---------------------------------------------------------------- the worker side of the contract


class _Response(io.BytesIO):
    def __init__(self, data: bytes, status: int = 200) -> None:
        super().__init__(data)
        self.status = status

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


@pytest.fixture
def worker(box, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """An Airflow worker configured for the backend, whose ``urlopen`` reaches the real ``Factory``."""
    calls: list[dict[str, Any]] = []

    def urlopen(request: urllib.request.Request, timeout: float | None = None) -> _Response:
        body = json.loads(request.data.decode())
        calls.append({"url": request.full_url, "body": body, "auth": request.get_header("Authorization")})
        assert request.full_url == "http://backend.invalid:8082/v1/work-orders"
        try:
            document = box.factory.submit(body)
        except Refused as error:
            raise urllib.error.HTTPError(request.full_url, error.status, str(error), {}, io.BytesIO(b"{}")) from error
        # The same encoder the real transport uses, so a dataclass in a queued answer (its
        # ``limiting`` block) is not a difference between this fake and ``swfactory.backend.server``.
        return _Response(json.dumps(document, default=_json_default).encode())

    monkeypatch.setenv("SWF_BACKEND_URL", "http://backend.invalid:8082")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "t" * 40)
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return calls


def test_the_worker_refuses_to_run_a_scheduled_line_without_a_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWF_BACKEND_URL", raising=False)
    monkeypatch.delenv("SWF_BACKEND_TOKEN", raising=False)
    with pytest.raises(CellCallbackError, match="SWF_BACKEND_URL"):
        admit_scheduled_run("line", SCHEDULED, [{"issue": "1", "repo": "owner/one", "job_idx": 0}])


def test_the_worker_admits_through_the_work_order_route_and_binds_every_job(box, worker) -> None:
    box.factory.control.admission.limits = Limits(global_active=4)  # room for both jobs of one tick
    _run(box)
    jobs = load("line").jobs({"issues": ["1", "2"]})

    bindings = admit_scheduled_run("line", SCHEDULED, jobs)

    (call,) = worker
    assert call["auth"] == "Bearer " + "t" * 40
    assert call["body"] == {
        "line": "line",
        "actor": SCHEDULE_ACTOR,
        "airflow_run_id": SCHEDULED,
        "issues": ["1", "2"],
        "targets": ["owner/one"],
    }
    bound = bind_jobs(jobs, bindings)
    assert [job["cell_managed"] for job in bound] == [True, True]
    assert {job["cell_epoch"] for job in bound} == {1}
    assert box.airflow.posts == []


def test_the_worker_treats_a_queued_order_as_a_deferral_not_a_run(box, worker) -> None:
    box.submit("1")
    _run(box)
    with pytest.raises(CellCallbackError, match="deferred"):
        admit_scheduled_run("line", SCHEDULED, load("line").jobs({"issues": ["2"]}))


def test_the_worker_refuses_bindings_for_a_different_run(box, worker, monkeypatch: pytest.MonkeyPatch) -> None:
    _run(box)
    real = box.factory.submit
    monkeypatch.setattr(box.factory, "submit", lambda body: {**real(body), "run_id": "swf__other"})
    with pytest.raises(CellCallbackError, match="different Airflow run"):
        admit_scheduled_run("line", SCHEDULED, load("line").jobs({"issues": ["1"]}))
