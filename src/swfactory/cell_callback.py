"""Managed Airflow -> backend Factory Cell lifecycle callbacks.

The callback is intentionally tiny and bounded. Airflow still schedules the work; it merely reports
managed cell lifecycle transitions to the backend that owns epoch fencing, admission release and
durable evidence. Direct/unmanaged Airflow runs remain backward-compatible and do not call it.

A transition can free capacity, and the backend answers with the work that released
(``released_work``) and the admitted commands it re-delivered as a result (``resumed_dispatch``).
The worker reports both and acts on neither: redelivery happens inside the backend request that
released the capacity, because a worker that triggered runs of its own would be a second lifecycle
scheduler standing next to Airflow.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any


class CellCallbackError(RuntimeError):
    pass


def post(path: str, body: dict[str, Any], *, env: Mapping[str, str] | None = None, timeout: float = 10.0) -> Any:
    """One authenticated ``POST`` to the backend's ``/v1`` surface; the decoded JSON answer.

    Fails closed when ``SWF_BACKEND_URL``/``SWF_BACKEND_TOKEN`` are missing or the backend is
    unreachable or refuses: a worker that guessed the control plane's answer would advance while
    the durable state stayed put.
    """
    env = os.environ if env is None else env
    base = (env.get("SWF_BACKEND_URL") or "").rstrip("/")
    token = env.get("SWF_BACKEND_TOKEN") or ""
    if not base or len(token) < 32:
        raise CellCallbackError("managed Airflow workers require SWF_BACKEND_URL and SWF_BACKEND_TOKEN")
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    request = urllib.request.Request(
        base + "/v1" + path,
        data=payload,
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
    if status >= 300:
        try:
            detail = json.loads(raw or b"{}").get("detail", f"HTTP {status}")
        except ValueError:
            detail = f"HTTP {status}"
        raise CellCallbackError(f"Factory Cell callback refused: {detail}")
    try:
        return json.loads(raw) if raw else {}
    except ValueError as error:
        raise CellCallbackError("Factory Cell callback returned invalid JSON") from error


def transition(job: dict[str, Any], state: str, *, operation_key: str) -> dict[str, Any] | None:
    """Report one authoritative lifecycle transition for a backend-managed mapped job.

    A managed job fails closed when the callback endpoint/credential is missing or unavailable:
    continuing would let compute advance while the durable control plane believes the old state and
    would leak admission capacity. Unmanaged/direct runs intentionally return ``None``.
    """
    if not bool(job.get("cell_managed")):
        return None
    cell_id = job.get("cell_id")
    epoch = job.get("cell_epoch")
    if not isinstance(cell_id, str) or type(epoch) is not int or epoch < 1:
        raise CellCallbackError("managed job has invalid Factory Cell binding")
    payload = {"cell_id": cell_id, "epoch": epoch, "state": state, "operation_key": operation_key}
    result = post("/cells/transition", payload)
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
