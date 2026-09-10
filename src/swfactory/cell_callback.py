"""Managed Airflow -> backend Factory Cell lifecycle callbacks.

The callback is intentionally tiny and bounded. Airflow still schedules the work; it merely reports
managed cell lifecycle transitions to the backend that owns epoch fencing, admission release and
durable evidence. Direct/unmanaged Airflow runs remain backward-compatible and do not call it.

A transition can free capacity, and the backend answers with the work that released
(``released_work``) and the admitted commands it re-delivered as a result (``resumed_dispatch``).
The worker reports both and acts on neither: redelivery happens inside the backend request that
released the capacity, because a worker that triggered runs of its own would be a second lifecycle
scheduler standing next to Airflow.

A report that cannot be delivered is not forgotten (#2071). It is recorded as a ``CallbackDebt`` in
the task's own XCom -- Airflow's store, the one thing worker and backend both reach -- and the next
report the same job makes settles it first, fenced by ``reconcile_callback`` against the Cell the
backend actually holds. The backend independently reads a live Cell's run back from Airflow, so a
job whose every report was lost is still released; the debt is what makes the *worker's* side of
that story visible and replayable rather than swallowed.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from swfactory.recovery_accounting import CallbackDebt, RecoveryAction, reconcile_callback

log = logging.getLogger(__name__)

# One JSON list per task instance: the lifecycle reports it could not deliver, oldest first.
DEBT_XCOM_KEY = "factory_callback_debt"
_DEBT_FIELDS = ("cell_id", "epoch", "dag_run_id", "task_id", "desired_state", "attempt")


class CellCallbackError(RuntimeError):
    pass


def _binding(job: dict[str, Any]) -> tuple[str, int]:
    cell_id = job.get("cell_id")
    epoch = job.get("cell_epoch")
    if not isinstance(cell_id, str) or type(epoch) is not int or epoch < 1:
        raise CellCallbackError("managed job has invalid Factory Cell binding")
    return cell_id, epoch


def _post(
    path: str, body: dict[str, Any], *, env: Mapping[str, str] | None = None, timeout: float = 10.0
) -> tuple[int, Any]:
    """One bounded POST to the backend. Transport failure raises; an HTTP status is returned as-is."""
    env = os.environ if env is None else env
    base = (env.get("SWF_BACKEND_URL") or "").rstrip("/")
    token = env.get("SWF_BACKEND_TOKEN") or ""
    if not base or len(token) < 32:
        raise CellCallbackError("managed Airflow workers require SWF_BACKEND_URL and SWF_BACKEND_TOKEN")
    request = urllib.request.Request(
        base + path,
        data=json.dumps(body, sort_keys=True, separators=(",", ":")).encode(),
        method="POST",
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(1024 * 1024 + 1)
            status = response.status
    except urllib.error.HTTPError as error:
        raw = error.read(8192)
        status = error.code
    except OSError as error:
        raise CellCallbackError(f"Factory Cell callback unavailable: {error}") from error
    if len(raw) > 1024 * 1024:
        raise CellCallbackError("Factory Cell callback response exceeds limit")
    try:
        return status, json.loads(raw) if raw else {}
    except ValueError as error:
        if status >= 300:
            return status, {}
        raise CellCallbackError("Factory Cell callback returned invalid JSON") from error


def post(path: str, body: dict[str, Any], *, env: Mapping[str, str] | None = None, timeout: float = 10.0) -> Any:
    """One authenticated ``POST`` to the backend's ``/v1`` surface; the decoded JSON answer.

    The shared fail-closed exchange every non-lifecycle caller uses (the maintenance sweep, a
    scheduled run admitting itself): a refusal raises here, because those callers have no debt to
    record -- a worker that guessed the control plane's answer would advance while the durable
    state stayed put. Lifecycle reports use ``_post`` directly, since a refused report is a fact
    they must keep rather than an error to raise.
    """
    status, document = _post("/v1" + path, body, env=env, timeout=timeout)
    if status >= 300:
        detail = document.get("detail", f"HTTP {status}") if isinstance(document, dict) else f"HTTP {status}"
        raise CellCallbackError(f"Factory Cell callback refused: {detail}")
    return document


def transition(job: dict[str, Any], state: str, *, operation_key: str) -> dict[str, Any] | None:
    """Report one authoritative lifecycle transition for a backend-managed mapped job.

    A managed job fails closed when the callback endpoint/credential is missing or unavailable:
    continuing would let compute advance while the durable control plane believes the old state and
    would leak admission capacity. Unmanaged/direct runs intentionally return ``None``.
    """
    if not bool(job.get("cell_managed")):
        return None
    cell_id, epoch = _binding(job)
    status, result = _post(
        "/v1/cells/transition",
        {"cell_id": cell_id, "epoch": epoch, "state": state, "operation_key": operation_key},
    )
    if status >= 300:
        detail = result.get("detail", f"HTTP {status}") if isinstance(result, dict) else f"HTTP {status}"
        raise CellCallbackError(f"Factory Cell callback refused: {detail}")
    if not isinstance(result, dict):
        raise CellCallbackError("Factory Cell callback returned invalid document")
    cell = result.get("cell")
    if not isinstance(cell, dict) or cell.get("cell_id") != cell_id or cell.get("epoch") != epoch:
        # A reply about some other cell or epoch must not be read as an acknowledgement of this
        # transition: the job would carry on believing the control plane had moved with it.
        raise CellCallbackError("Factory Cell callback acknowledged a different cell epoch")
    released = result.get("released_work")
    resumed = result.get("resumed_dispatch")
    return {
        "cell": cell,
        "released_work": list(released) if isinstance(released, list) else [],
        "resumed_dispatch": list(resumed) if isinstance(resumed, list) else [],
    }


# ---------------------------------------------------------------------------- the DAG-facing entry


def report(job: dict[str, Any], state: str, context: dict[str, Any], suffix: str) -> dict[str, Any] | None:
    """Settle what this job still owes, then report ``state``; ``dags/blueprints.py`` delegates here.

    The operation key is derived from the task instance so a retried task repeats the same report
    instead of minting a new one. A report that cannot be delivered is recorded as debt *before* it
    fails the task closed -- the failure is the same, the difference is that it is now written down
    where the next report of this job, or an operator, can find it.
    """
    if not bool(job.get("cell_managed")):
        return None
    cell_id, epoch = _binding(job)
    ti = context["ti"]
    run_id = str(context["dag_run"].run_id)
    task_id = str(getattr(ti, "task_id", suffix))
    try_number = int(getattr(ti, "try_number", 0) or 0)
    debt = CallbackDebt(cell_id, epoch, run_id, task_id, state, attempt=try_number)
    try:
        settle(job, context)
        return transition(
            job,
            state,
            operation_key=f"airflow:{run_id}:{int(job['job_idx'])}:{task_id}:{try_number}:{suffix}",
        )
    except CellCallbackError as error:
        _record(ti, debt)
        log.error("Factory Cell %s@%s owes a %r report (debt %s): %s", cell_id, epoch, state, debt.key, error)
        raise


def report_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback`` body: mark the managed Cell failed without touching Airflow's own verdict.

    Only a delivery failure is caught, and only because ``report`` has already recorded it as debt
    and logged it: raising here would add a second stack trace to the task log and change nothing
    else. Anything else propagates to Airflow's callback log instead of vanishing.
    """
    ti = context["ti"]
    jobs = ti.xcom_pull(task_ids="fan_out") or []
    index = int(ti.map_index)
    if not isinstance(jobs, list) or not 0 <= index < len(jobs):
        return
    try:
        report(jobs[index], "failed", context, "failed")
    except CellCallbackError as error:
        log.error("Factory Cell failure report for job %s was not delivered: %s", index, error)


def resume_backend() -> dict[str, Any] | None:
    """Ask the backend to reconcile lost reports and redeliver what that frees (the ``maintain`` DAG).

    The backend decides and redelivers; this only asks, so Airflow remains the one scheduler. An
    install with no backend has nothing to ask and returns ``None``.
    """
    if not os.getenv("SWF_BACKEND_URL"):
        return None
    status, result = _post("/v1/queue/resume", {})
    if status >= 300:
        raise CellCallbackError(f"Factory backend refused queue resume: HTTP {status}")
    return result if isinstance(result, dict) else {}


# ------------------------------------------------------------------------------------ callback debt


def _record(ti: Any, debt: CallbackDebt) -> None:
    """Append ``debt`` to this task instance's ledger. Failing to write it must not hide the report failure."""
    try:
        rows = ti.xcom_pull(key=DEBT_XCOM_KEY, map_indexes=ti.map_index) or []
        rows = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        if not any(_debt_from(row) == debt for row in rows):
            rows.append(asdict(debt))
            ti.xcom_push(key=DEBT_XCOM_KEY, value=rows)
    except Exception:  # noqa: BLE001 - the report failure being raised is the primary fact
        log.exception("Factory Cell callback debt %s could not be recorded", debt.key)


def _debt_from(row: Any) -> CallbackDebt | None:
    if not isinstance(row, dict) or set(row) != set(_DEBT_FIELDS):
        return None
    try:
        return CallbackDebt(
            str(row["cell_id"]),
            int(row["epoch"]),
            str(row["dag_run_id"]),
            str(row["task_id"]),
            str(row["desired_state"]),
            int(row["attempt"]),
        )
    except (TypeError, ValueError):
        return None


def outstanding(context: dict[str, Any]) -> list[CallbackDebt]:
    """Every debt this job's tasks have recorded so far -- same task group, same map index."""
    ti = context["ti"]
    task_id = str(ti.task_id)
    prefix = task_id.rsplit(".", 1)[0] + "." if "." in task_id else ""
    task_ids = [t for t in getattr(context.get("dag"), "task_ids", []) if str(t).startswith(prefix)] or [task_id]
    pulled = ti.xcom_pull(task_ids=task_ids, key=DEBT_XCOM_KEY, map_indexes=ti.map_index)
    debts: list[CallbackDebt] = []
    for rows in pulled if isinstance(pulled, list) else [pulled]:
        for row in rows if isinstance(rows, list) else []:
            debt = _debt_from(row)
            if debt is not None and debt not in debts:
                debts.append(debt)
    return debts


def settle(job: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    """Replay the reports this job could not deliver, each fenced by what the backend holds now.

    ``reconcile_callback`` decides per debt from the Cell the backend actually has: ADOPT when the
    report already landed (or the run ended the same way), REFUSE when the epoch moved on or the
    Cell ended differently -- the backend is the authority and the debt is void -- and RETRY when
    the Cell is still live, which is the one case the report is re-sent. A backend that cannot be
    asked settles nothing and fails this report closed too, as it would have anyway.
    """
    settled: list[dict[str, Any]] = []
    for debt in outstanding(context):
        status, cell = _post("/v1/cells/inspect", {"cell_id": debt.cell_id})
        if status == 404:
            action = RecoveryAction.REFUSE
        elif status >= 300 or not isinstance(cell, dict):
            raise CellCallbackError(f"Factory Cell {debt.cell_id} cannot be inspected: HTTP {status}")
        else:
            action = reconcile_callback(int(cell.get("epoch") or 0), debt, str(cell.get("state") or ""))
        if action is RecoveryAction.RETRY:
            transition(job, debt.desired_state, operation_key=f"airflow-debt:{debt.key}")
        settled.append({"debt": debt.key, "desired_state": debt.desired_state, "action": str(action)})
        log.warning("Factory Cell callback debt %s (%s) settled: %s", debt.key, debt.desired_state, action)
    return settled
