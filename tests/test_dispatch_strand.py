"""Adversarial companions to test_durable_dispatch: try to strand a work order, or run it twice.

Every test here was written to break the durable outbox rather than to describe it, and each one
failed before the fix it guards. Three ways to strand an admitted work order are covered:

  * spending the bounded delivery budget, which used to leave an ``admitted`` row holding a unit
    that nothing could ever claim again, with ``resume_dispatch()`` answering ``[]`` forever;
  * cancelling an order that had already activated its Cells, which used to release the unit but
    leave the Cells live -- and because a Cell's epoch is part of the deterministic work id, every
    later resubmission of that request answered 409 forever;
  * a superseded delivery attempt retiring the intent it no longer owned, which could cancel a
    Factory Cell while a newer attempt was binding a real Airflow run to it.

``Crash`` is deliberately a BaseException: it bypasses every in-process recovery handler, so the
only thing that can repair the damage is durable state read back by a restarted backend.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
from test_durable_dispatch import LINE, SECOND_TARGET, Backend, _line  # noqa: F401

from swfactory.admission import Limits
from swfactory.durable_admission import MAX_DISPATCH_ATTEMPTS


class Crash(BaseException):
    """Not an Exception: bypasses every in-process recovery handler, like a real SIGKILL."""


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def build(limits: Limits, *extra: str) -> Backend:
        _line(tmp_path, *extra)
        monkeypatch.chdir(tmp_path)
        return Backend(tmp_path, limits)

    return build


def _state(box, work_id):
    return box.factory.control.admission.state_of(work_id)


# ---------------------------------------------------------------- 1. plain A/B


def test_plain_ab(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    assert b["state"] == "queued"
    box.finish(a["cells"][0])
    assert len(box.airflow.created_for(b["submission_id"])) == 1
    assert _state(box, b["submission_id"]) == "bound"


# ------------------------------- 2. crash after admission, before any delivery


def test_crash_after_admission_before_delivery(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    box.factory.resume_dispatch = lambda **_: []  # type: ignore[method-assign]
    box.finish(a["cells"][0])
    assert _state(box, b["submission_id"]) == "admitted"
    assert box.airflow.created_for(b["submission_id"]) == []

    box.restart()
    box.factory.resume_dispatch()
    assert len(box.airflow.created_for(b["submission_id"])) == 1
    assert _state(box, b["submission_id"]) == "bound"


# ------------------------------- 3. crash mid-activation (epoch never recorded)


def test_crash_between_activation_and_recording_the_epoch(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")

    admission = box.factory.control.admission
    real = admission.record_member_epoch

    def boom(*args, **kwargs):
        raise Crash("died right after activating the Cell")

    admission.record_member_epoch = boom  # type: ignore[method-assign]
    with pytest.raises(Crash):
        box.finish(a["cells"][0])
    admission.record_member_epoch = real  # type: ignore[method-assign]

    box.restart()
    box.factory.resume_dispatch()

    work_id = b["submission_id"]
    runs = box.airflow.created_for(work_id)
    assert len(runs) == 1, f"{len(runs)} runs after a crash mid-activation"
    assert _state(box, work_id) == "bound"
    members = box.factory.control.admission.members(work_id)
    assert [m.cell_epoch for m in members] == [1], "the orphaned activation must be adopted, not re-activated"
    assert box.factory.cell_store.get(members[0].cell_id)["airflow_run_id"] == runs[0]


# ------------------- 4. crash after Airflow accepted, before anything recorded


def test_crash_after_airflow_accepted_before_binding(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")

    def create_then_die(run_id: str) -> None:
        box.airflow.runs[run_id] = {"dag_run_id": run_id, "state": "queued"}
        box.airflow.created.append({"dag_run_id": run_id, "work_id": b["submission_id"]})
        raise Crash("died with the run already created")

    box.airflow.post_hook = create_then_die
    with pytest.raises(Crash):
        box.finish(a["cells"][0])
    box.airflow.post_hook = None

    work_id = b["submission_id"]
    assert len(box.airflow.created_for(work_id)) == 1
    box.restart()
    box.factory.resume_dispatch()

    runs = box.airflow.created_for(work_id)
    assert len(runs) == 1, f"{len(runs)} runs: the accepted-but-unrecorded POST was repeated"
    assert len(box.airflow.posts) == 2, "the resume must reconcile, not blindly re-POST"
    assert _state(box, work_id) == "bound"
    members = box.factory.control.admission.members(work_id)
    assert box.factory.cell_store.get(members[0].cell_id)["airflow_run_id"] == runs[0]


# ----------------------------- 5. crash after the binding, before record_dispatch


def test_crash_after_binding_before_record_dispatch(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")

    admission = box.factory.control.admission
    real = admission.record_dispatch

    def boom(*args, **kwargs):
        raise Crash("died between binding the Cell and closing the intent")

    admission.record_dispatch = boom  # type: ignore[method-assign]
    with pytest.raises(Crash):
        box.finish(a["cells"][0])
    admission.record_dispatch = real  # type: ignore[method-assign]

    work_id = b["submission_id"]
    box.restart()
    box.factory.resume_dispatch()
    runs = box.airflow.created_for(work_id)
    assert len(runs) == 1, f"{len(runs)} runs after a crash before the intent was closed"
    assert _state(box, work_id) == "bound"


# ------------------------------------------- 6. repeated resume never re-dispatches


def test_resume_is_idempotent_after_success(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    box.finish(a["cells"][0])
    for _ in range(5):
        box.restart()
        assert box.factory.resume_dispatch() == []
    assert len(box.airflow.created_for(b["submission_id"])) == 1


# ------------------------------------------- 7. concurrent resume must not double


def test_concurrent_resume_dispatches_once(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    box.factory.resume_dispatch = lambda **_: []  # type: ignore[method-assign]
    box.finish(a["cells"][0])
    box.restart()

    barrier = threading.Barrier(6)
    errors: list[BaseException] = []

    def pump() -> None:
        barrier.wait()
        try:
            box.factory.resume_dispatch()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=pump) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    runs = box.airflow.created_for(b["submission_id"])
    assert len(runs) == 1, f"{len(runs)} runs from concurrent resume"
    assert _state(box, b["submission_id"]) == "bound"


# ------------------------------------------- 8. persistent Airflow outage


def test_persistent_airflow_outage_does_not_strand_the_reservation(backend) -> None:
    """An outage that outlives the delivery budget must end loudly and stay recoverable."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    work_id = b["submission_id"]

    box.airflow.post_status = 503
    box.finish(a["cells"][0])
    for _ in range(MAX_DISPATCH_ATTEMPTS + 3):
        box.factory.resume_dispatch()
    assert box.airflow.created_for(work_id) == []
    assert box.airflow.posts, "every attempt of the budget must actually have reached Airflow"

    box.restart()
    assert _state(box, work_id) == "failed", "a spent delivery budget must not stay 'admitted' forever"
    pressure = box.factory.control.admission.snapshot()["pressure"]
    assert pressure["held_units"] == 0 and pressure["undeliverable"] == 0

    # Airflow comes back: the same request is submittable again, because compensation moved the
    # Cell's epoch on and the identity is therefore no longer a permanent 409.
    box.airflow.post_status = 200
    again = box.submit("2")
    assert again["state"] == "submitted"
    assert len(box.airflow.created_for(again["submission_id"])) == 1


def test_a_permanently_undeliverable_order_does_not_hold_capacity_forever(backend) -> None:
    """Whatever the give-up rule is, it must not be 'hold a unit and go quiet'."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    c = box.submit("3")
    assert c["state"] == "queued"
    box.airflow.post_status = 503
    box.finish(a["cells"][0])
    for _ in range(MAX_DISPATCH_ATTEMPTS + 5):
        box.factory.resume_dispatch()

    snapshot = box.factory.control.admission.snapshot()
    state = _state(box, b["submission_id"])
    assert state != "admitted" or snapshot["pressure"]["held_units"] == 0, (
        f"{b['submission_id']} still holds capacity in state {state!r} with no way to move; "
        f"pressure={snapshot['pressure']}"
    )
    del c


# ------------------------------------------- 9. multi-job sibling release


def test_multi_job_sibling_release(backend) -> None:
    box = backend(Limits(global_active=2, per_repo_active=2), SECOND_TARGET)
    multi = box.submit("1")
    assert len(multi["cells"]) == 2
    queued = box.submit("2")
    assert queued["state"] == "queued"

    box.finish(multi["cells"][0])
    assert box.factory.control.admission.snapshot()["pressure"]["held_units"] == 1
    assert _state(box, queued["submission_id"]) == "queued"

    box.restart()
    assert _state(box, queued["submission_id"]) == "queued", "a restart must not free the sibling's unit"
    assert box.factory.control.admission.snapshot()["pressure"]["held_units"] == 1

    box.finish(multi["cells"][1])
    assert _state(box, multi["submission_id"]) == "success"
    assert len(box.airflow.created_for(queued["submission_id"])) == 1


def test_multi_job_sibling_release_out_of_order_and_mixed_outcomes(backend) -> None:
    box = backend(Limits(global_active=2, per_repo_active=2), SECOND_TARGET)
    multi = box.submit("1")
    box.finish(multi["cells"][1], state="failed")
    assert _state(box, multi["submission_id"]) in {"bound", "dispatching"}
    box.finish(multi["cells"][0], state="success")
    assert _state(box, multi["submission_id"]) == "failed", "the worst member outcome must win"
    assert box.factory.control.admission.snapshot()["pressure"]["held_units"] == 0


def test_double_terminal_report_for_one_cell_releases_one_unit_only(backend) -> None:
    box = backend(Limits(global_active=2, per_repo_active=2), SECOND_TARGET)
    multi = box.submit("1")
    queued = box.submit("2")
    box.finish(multi["cells"][0])
    box.finish(multi["cells"][0], state="success")
    assert _state(box, queued["submission_id"]) == "queued", "a repeated report released a sibling's unit"
    assert box.factory.control.admission.snapshot()["pressure"]["held_units"] == 1


# ------------------------------------------- 10. cancelling an order that already activated


def test_cancelling_an_admitted_order_does_not_leave_its_cells_live(backend) -> None:
    """The mirror of #2058: a live Factory Cell that no work order owns."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    box.airflow.post_status = 503
    box.finish(a["cells"][0])
    box.airflow.post_status = 200
    work_id = b["submission_id"]
    assert _state(box, work_id) == "admitted"
    members = box.factory.control.admission.members(work_id)
    assert [m.cell_epoch for m in members] == [1], "the Cell was activated before the POST failed"

    box.factory.operation("/queue/cancel", {"work_id": work_id, "reason": "operator"})
    cell = box.factory.cell_store.get(members[0].cell_id)
    assert cell["state"] not in {"dispatching", "queued", "running"}, (
        f"cancelling {work_id} left Cell {members[0].cell_id} live in state {cell['state']!r} with no owner"
    )


def test_after_cancelling_an_admitted_order_the_operator_can_resubmit_it(backend) -> None:
    """#2058's actual symptom: the retry answered 409 and the work could never be re-run."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    box.airflow.post_status = 503
    box.finish(a["cells"][0])
    box.airflow.post_status = 200
    box.factory.operation("/queue/cancel", {"work_id": b["submission_id"], "reason": "operator"})

    again = box.submit("2")
    assert again["state"] == "submitted", f"resubmission after a cancel answered {again}"
    assert len(box.airflow.created_for(again["submission_id"])) == 1


def test_a_total_airflow_outage_leaves_a_visible_stuck_unit_not_a_silent_one(backend) -> None:
    """When even the reconcile cannot answer, the unit stays held -- but it is counted as stuck."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    work_id = b["submission_id"]
    real = box.airflow.__call__

    def blackhole(method: str, path: str, body: dict | None):
        if "/dagRuns" in path:
            return 503, {"detail": "airflow is down"}
        return real(method, path, body)

    box.factory.airflow = blackhole  # type: ignore[method-assign,assignment]
    box.finish(a["cells"][0])
    for _ in range(MAX_DISPATCH_ATTEMPTS + 3):
        box.factory.resume_dispatch()

    admission = box.factory.control.admission
    pressure = admission.snapshot()["pressure"]
    assert admission.dispatch_row(work_id)["state"] == "undeliverable"
    assert pressure["undeliverable"] == 1, f"a stuck unit must be counted, not hidden: {pressure}"
    assert admission.pending_dispatch() == [], "an undeliverable intent must leave the redelivery queue"
    assert box.factory.resume_dispatch() == []

    # And the operator handle actually frees it, Cells included.
    result = box.factory.operation("/queue/cancel", {"work_id": work_id, "reason": "airflow outage"})
    assert result["cancelled_cells"], "cancelling must close the Cells the stuck order activated"
    assert admission.snapshot()["pressure"]["held_units"] == 0


# ------------------------------------------- 11. two backends on one state directory


def test_two_replicas_pumping_the_same_outbox_dispatch_once(backend) -> None:
    """The in-process guard cannot help here; the journal and the deterministic run id must."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    box.factory.resume_dispatch = lambda **_: []  # type: ignore[method-assign]
    box.finish(a["cells"][0])
    box.factory.close()

    replicas = [box._open() for _ in range(4)]
    barrier = threading.Barrier(len(replicas))
    errors: list[BaseException] = []

    def pump(factory) -> None:
        barrier.wait()
        try:
            factory.resume_dispatch()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=pump, args=(r,)) for r in replicas]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    runs = box.airflow.created_for(b["submission_id"])
    assert len(runs) == 1, f"{len(runs)} runs from {len(replicas)} replicas"
    for r in replicas:
        r.close()

    box.factory = box._open()
    box.factory.resume_dispatch()
    assert box.admission_state(b["submission_id"]) == "bound"
    assert len(box.airflow.created_for(b["submission_id"])) == 1
    cell = box.factory.cell_store.get(box.factory.control.admission.members(b["submission_id"])[0].cell_id)
    assert cell["state"] not in {"cancelled", "failed"}, "a live run's Cell was cancelled by a losing attempt"
    assert cell["airflow_run_id"] == runs[0]


def test_a_superseded_attempt_cannot_cancel_the_winning_attempts_cell(backend) -> None:
    """A stale delivery attempt has no authority over the intent it no longer owns."""
    box = backend(Limits(global_active=1))
    box.factory.control.admission.dispatch_lease_s = 0.0
    a = box.submit("1")
    b = box.submit("2")
    box.factory.resume_dispatch = lambda **_: []  # type: ignore[method-assign]
    box.finish(a["cells"][0])
    work_id = b["submission_id"]
    admission = box.factory.control.admission

    stale = admission.claim_dispatch(work_id)
    assert stale is not None
    admission._release_inflight(work_id)  # pretend this attempt's thread is still running
    winner = admission.claim_dispatch(work_id)
    assert winner is not None and winner.lease_token != stale.lease_token

    with pytest.raises(Exception):  # noqa: B017 - the stale attempt must be refused, however
        box.factory._dispatch_intent(stale)
    assert admission.state_of(work_id) != "failed", "a superseded attempt retired the reservation"

    admission._release_inflight(work_id)
    box.factory._dispatch_intent(winner)
    assert admission.state_of(work_id) == "bound"
    assert len(box.airflow.created_for(work_id)) == 1


# ------------------------------------------- 12. nothing is left waiting for a timer


def test_work_drained_by_a_failing_submission_is_delivered_by_the_next_request(backend) -> None:
    """No timer exists, so every drain must be picked up by some later request path."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    queued = box.submit("2")
    assert queued["state"] == "queued"

    # A's Cell ends, B is admitted, but this transition's own resume is prevented.
    box.factory.resume_dispatch = lambda **_: []  # type: ignore[method-assign]
    box.finish(a["cells"][0])
    assert box.admission_state(queued["submission_id"]) == "admitted"
    box.restart()

    # An unrelated submission is enough: the outbox is pumped by the request paths themselves.
    third = box.submit("3")
    assert third["state"] == "queued"
    assert len(box.airflow.created_for(queued["submission_id"])) == 1
    assert box.admission_state(queued["submission_id"]) == "bound"


# ------------------------------------------- 13. a unit held by an already-finished Cell


def test_crash_between_cancelling_the_cells_and_closing_the_reservation(backend) -> None:
    """A unit whose Cell is already terminal has no report coming; it must still be repaired."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    box.airflow.post_status = 503
    box.finish(a["cells"][0])
    box.airflow.post_status = 200
    work_id = b["submission_id"]
    assert box.admission_state(work_id) == "admitted"

    real = box.factory.control.cancel_reservation

    def boom(*args, **kwargs):
        raise Crash("died after cancelling the Cells, before closing the reservation")

    box.factory.control.cancel_reservation = boom  # type: ignore[method-assign]
    with pytest.raises(Crash):
        box.factory.operation("/queue/cancel", {"work_id": work_id, "reason": "operator"})
    box.factory.control.cancel_reservation = real  # type: ignore[method-assign]

    box.restart()
    box.factory.resume_dispatch()
    pressure = box.factory.control.admission.snapshot()["pressure"]
    members = box.factory.control.admission.members(work_id)
    cell = box.factory.cell_store.get(members[0].cell_id)
    assert cell["state"] == "cancelled"
    assert pressure["held_units"] == 0, (
        f"a cancelled Cell's unit is still held: state={box.admission_state(work_id)!r} pressure={pressure}"
    )


# ------------------------------------------- 14. racing the submit path itself


def test_identical_submissions_racing_produce_one_reservation_and_one_run(backend) -> None:
    """The deterministic work id is only half of it; the delivery must not race either."""
    box = backend(Limits(global_active=4, per_repo_active=4))
    barrier = threading.Barrier(6)
    results: list[dict] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def submit_same() -> None:
        barrier.wait()
        try:
            answer = box.submit("1")
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)
        else:
            with lock:
                results.append(answer)

    threads = [threading.Thread(target=submit_same) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    ids = {answer["submission_id"] for answer in results}
    assert len(ids) == 1, f"one immutable request produced {len(ids)} reservations"
    work_id = ids.pop()
    assert len(box.airflow.created_for(work_id)) == 1, "a raced duplicate submission created a second run"
    admission = box.factory.control.admission
    assert admission.snapshot()["pressure"]["held_units"] == 1
    assert len(admission.members(work_id)) == 1

    box.restart()
    box.factory.resume_dispatch()
    assert box.admission_state(work_id) == "bound"
    assert len(box.airflow.created_for(work_id)) == 1


def test_racing_distinct_submissions_never_exceed_capacity(backend) -> None:
    box = backend(Limits(global_active=2, per_repo_active=2))
    barrier = threading.Barrier(8)
    states: list[str] = []
    lock = threading.Lock()

    def submit(index: int) -> None:
        barrier.wait()
        answer = box.submit(str(index))
        with lock:
            states.append(answer["state"])

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert states.count("submitted") == 2, states
    assert states.count("queued") == 6, states
    pressure = box.factory.control.admission.snapshot()["pressure"]
    assert pressure["held_units"] == 2
    assert len(box.airflow.created) == 2


# ------------------------------------------- 12. the lost terminal callback (#2071)


def _lose_the_terminal_report(box, work: dict, run_state: str | None = "failed") -> str:
    """Airflow finishes the run; the worker's report never reaches the backend."""
    cell_id = work["cells"][0]
    box.finish(cell_id, state="running")
    run_id = box.factory.cell_store.get(cell_id)["airflow_run_id"]
    if run_state is None:
        del box.airflow.runs[run_id]
    else:
        box.airflow.runs[run_id]["state"] = run_state
    return cell_id


def test_a_lost_terminal_callback_is_reconciled_against_airflow_on_restart(backend) -> None:
    """The issue's probe: running Cell, finished run, no callback, queued order, restart."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    cell_id = _lose_the_terminal_report(box, a, "failed")
    assert _state(box, b["submission_id"]) == "queued"

    box.restart()
    assert box.factory._reconcile_held_units() == [a["submission_id"]], "the held unit was never reconciled"
    cell = box.factory.cell_store.get(cell_id)
    assert cell["state"] == "failed", f"Cell stayed {cell['state']!r} after its run finished"
    box.factory.resume_dispatch()
    assert len(box.airflow.created_for(b["submission_id"])) == 1, "the queued order never dispatched"
    assert _state(box, b["submission_id"]) == "bound"
    assert box.factory.control.admission.snapshot()["pressure"]["held_units"] == 1
    history = box.factory.cell_store.history(cell_id)
    assert any(e["kind"] == "patch" and e["payload"].get("state") == "failed" for e in history)


def test_a_lost_success_report_adopts_the_run_outcome(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    cell_id = _lose_the_terminal_report(box, a, "success")
    box.restart()
    box.factory.resume_dispatch()
    assert box.factory.cell_store.get(cell_id)["state"] == "success"
    assert _state(box, b["submission_id"]) == "bound"


def test_an_absent_airflow_run_releases_its_unit_as_cancelled(backend) -> None:
    """A run deleted under the backend certainly is not computing; the unit is held for nothing."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    cell_id = _lose_the_terminal_report(box, a, None)
    box.restart()
    box.factory.resume_dispatch()
    assert box.factory.cell_store.get(cell_id)["state"] == "cancelled"
    assert _state(box, b["submission_id"]) == "bound"


def test_a_live_airflow_run_is_not_mistaken_for_a_lost_callback(backend) -> None:
    """Only Airflow's verdict ends a Cell; a run still going keeps its unit."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    cell_id = _lose_the_terminal_report(box, a, "running")
    box.restart()
    assert box.factory.resume_dispatch() == []
    assert box.factory.cell_store.get(cell_id)["state"] == "running"
    assert _state(box, b["submission_id"]) == "queued"
    assert box.factory.fleet()["callback_debt"] == 0


def test_an_unreachable_airflow_keeps_the_unit_held_and_surfaces_the_debt(backend) -> None:
    """When the run cannot be read, nothing is released -- but the doubt is counted, not hidden."""
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    cell_id = _lose_the_terminal_report(box, a, "failed")
    real = box.airflow.__call__

    def blackhole(method: str, path: str, body: dict | None):
        if method == "GET" and "/dagRuns/" in path:
            raise OSError("airflow is down")
        return real(method, path, body)

    box.restart()
    box.factory.airflow = blackhole  # type: ignore[method-assign,assignment]
    assert box.factory.resume_dispatch() == []
    assert box.factory.cell_store.get(cell_id)["state"] == "running"
    assert _state(box, b["submission_id"]) == "queued"
    fleet = box.factory.fleet()
    assert fleet["callback_debt"] == 1 and "callback_debt:1" in fleet["bottlenecks"], fleet

    box.factory.airflow = box.airflow  # type: ignore[method-assign]
    box.factory.resume_dispatch()
    assert box.factory.cell_store.get(cell_id)["state"] == "failed"
    assert _state(box, b["submission_id"]) == "bound"
    assert box.factory.fleet()["callback_debt"] == 0


def test_reconciling_a_lost_callback_twice_releases_one_unit_only(backend) -> None:
    box = backend(Limits(global_active=1))
    a = box.submit("1")
    b = box.submit("2")
    _lose_the_terminal_report(box, a, "failed")
    box.restart()
    assert box.factory._reconcile_held_units() == [a["submission_id"]]
    assert box.factory._reconcile_held_units() == []
    box.factory.resume_dispatch()
    assert _state(box, b["submission_id"]) == "bound"
    assert box.factory.control.admission.snapshot()["pressure"]["held_units"] == 1


def test_a_multi_job_order_releases_when_its_last_lost_report_is_reconciled(backend) -> None:
    """One sibling reported; the other's report was lost. The order holds both units until then."""
    box = backend(Limits(global_active=2, per_repo_active=2), SECOND_TARGET)
    multi = box.submit("1")
    queued = box.submit("2")
    reported, lost = multi["cells"]
    box.finish(reported, state="success")
    box.finish(lost, state="running")
    run_id = box.factory.cell_store.get(lost)["airflow_run_id"]
    box.airflow.runs[run_id]["state"] = "failed"
    assert _state(box, queued["submission_id"]) == "queued"

    box.restart()
    box.factory.resume_dispatch()
    assert box.factory.cell_store.get(reported)["state"] == "success", "a reconcile must not rewrite a reported Cell"
    assert box.factory.cell_store.get(lost)["state"] == "failed"
    assert _state(box, multi["submission_id"]) == "failed", "the worst member outcome must win"
    assert _state(box, queued["submission_id"]) == "bound"
