"""Both GitHub intakes must cross the managed work-order boundary, and say so truthfully.

#2068: the webhook receiver and ``.github/workflows/dispatch.yml`` posted straight at Airflow's
``dagRuns`` endpoint. Runs created that way carry no ``_factory_cells``, so they get none of the
durable admission, capacity accounting or Factory Cell fencing every other intake path has -- and
the compatibility mount answered a queued order with a 429, so a caller could not tell an order
that is durably waiting for capacity from one that was thrown away.

Hermetic: the backend answers from an in-process fake, and ``FakeAirflow`` records every request
that reaches Airflow so "no bypassing Airflow write" is an assertion rather than a hope.
"""

from __future__ import annotations

import io
import json
import time
import urllib.error
from pathlib import Path
from typing import Any

import pytest
from test_durable_dispatch import Backend, _line  # noqa: F401

from swfactory import dispatch, webhook
from swfactory.admission import Limits
from swfactory.backend.server import _json_default
from swfactory.backend.service import Refused
from swfactory.dispatch import DeliveryConflict, DeliveryInbox, Dispatcher
from swfactory.webhook import Trigger, WorkOrders

BACKEND = "http://127.0.0.1:8082"
_real_time = time.time
WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "dispatch.yml"


# ---------------------------------------------------------------- a backend on a fake socket


class FakeBackend:
    """Answers ``POST /v1/work-orders`` from a real :class:`Factory`, over the receiver's opener.

    Everything the receiver believes about admission therefore comes from the same code the
    canonical route runs, not from a hand-written stub.
    """

    def __init__(self, box: Backend) -> None:
        self.box = box
        self.calls: list[dict[str, Any]] = []
        self.fail: Exception | None = None

    def __call__(self, request, timeout=None):  # noqa: ANN001 - urlopen signature
        body = json.loads(request.data.decode()) if request.data else None
        self.calls.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "headers": {k.lower(): v for k, v in request.header_items()},
                "body": body,
            }
        )
        if self.fail is not None:
            raise self.fail
        assert request.full_url == BACKEND + "/v1/work-orders"
        document = self.box.factory.submit(body)
        # The same encoder the real transport uses, so a dataclass in the answer is not a
        # difference between this fake and `swfactory.backend.server`.
        return _Response(json.dumps(document, default=_json_default).encode())


class _Response(io.BytesIO):
    def __init__(self, data: bytes, status: int = 200) -> None:
        super().__init__(data)
        self.status = status

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _http_error(code: int, detail: str = "no") -> urllib.error.HTTPError:
    return urllib.error.HTTPError(BACKEND, code, detail, {}, io.BytesIO(b'{"detail":"x"}'))


@pytest.fixture
def box(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    _line(tmp_path)
    monkeypatch.chdir(tmp_path)
    built = Backend(tmp_path, Limits(global_active=1))
    try:
        yield built
    finally:
        built.close()


@pytest.fixture
def receiver(box, tmp_path: Path):
    """A durable inbox plus a dispatcher pointed at the managed boundary, not at Airflow."""

    def build() -> tuple[DeliveryInbox, Dispatcher, FakeBackend]:
        backend = FakeBackend(box)
        inbox = DeliveryInbox(tmp_path / "inbox.sqlite3")
        dispatcher = Dispatcher(
            inbox,
            airflow_url="http://127.0.0.1:8080",
            token_provider=None,
            opener=backend,
            log=lambda _line: None,
            work_orders=WorkOrders(BACKEND, token="backend-token"),
        )
        return inbox, dispatcher, backend

    return build


def _enqueue(inbox: DeliveryInbox, delivery_id: str, issue: str = "42") -> None:
    payload = json.dumps({"issue": issue}).encode()
    inbox.enqueue(delivery_id, "issues", payload, "owner/one", Trigger("line", {"issues": [issue]}))


# ---------------------------------------------------------------- box 1: reach the backend


def test_the_receiver_submits_a_work_order_and_the_run_carries_its_cells(box, receiver) -> None:
    """The one outbound call the receiver makes is the work order. Everything else -- unpausing the
    line, creating the run, binding the Cells -- is the backend's, which is the whole point."""
    inbox, dispatcher, backend = receiver()
    _enqueue(inbox, "d-1")

    assert dispatcher.dispatch_one() is True

    (call,) = backend.calls
    assert call["url"] == BACKEND + "/v1/work-orders"
    assert call["headers"]["authorization"] == "Bearer backend-token"
    assert call["body"] == {"line": "line", "actor": webhook.MANAGED_ACTOR, "issues": ["42"]}

    # The run exists because the backend admitted it, so it carries the bindings a run created
    # straight from the receiver never had.
    assert len(box.airflow.created) == 1
    conf = box.airflow.posts[0]["conf"]
    assert conf["_factory_cells"], "a managed dispatch binds Factory Cells into the run conf"
    assert box.factory.cell_store.list(limit=10), "and the Cells themselves exist"

    receipt = inbox.get("d-1")
    assert receipt.state == "dispatched"
    assert receipt.work_order_id == conf["_factory_submission_id"]
    assert receipt.admission_state == "submitted"


# ---------------------------------------------------------------- box 2: refusals and queueing


def test_a_queued_order_is_a_receipt_the_operator_can_tell_from_a_run(box, receiver) -> None:
    """Capacity one: the second delivery is durably queued, not executed, and says so."""
    inbox, dispatcher, backend = receiver()
    _enqueue(inbox, "d-1", "1")
    _enqueue(inbox, "d-2", "2")

    assert dispatcher.dispatch_one() and dispatcher.dispatch_one()

    second = inbox.get("d-2")
    assert second.state == "dispatched", "the backend holds it now; the receiver is done"
    assert second.admission_state == "queued", "queued is not executed and must not read as executed"
    assert len(box.airflow.created) == 1, "the queued order must not have reached Airflow"
    snapshot = box.factory.control.admission.snapshot(limit=10)
    assert [row["work_id"] for row in snapshot["queued"]] == [second.work_order_id]


@pytest.mark.parametrize(
    ("code", "retryable"),
    [(503, True), (429, True), (409, False), (422, False)],
)
def test_a_backend_refusal_never_falls_back_to_airflow(box, receiver, code: int, retryable: bool) -> None:
    """A drain or a capacity refusal must not be answered by writing to Airflow behind its back."""
    inbox, dispatcher, backend = receiver()
    _enqueue(inbox, "d-1")
    backend.fail = _http_error(code)

    assert dispatcher.dispatch_one() is True

    receipt = inbox.get("d-1")
    assert receipt.state == ("pending" if retryable else "dead")
    assert receipt.last_error == f"work_order_http_{code}"
    assert box.airflow.posts == [], "no bypassing Airflow write"


def test_the_compat_mount_answers_a_queued_order_with_a_receipt_not_a_refusal(box) -> None:
    """429 told the console to resend work the backend was already durably holding, and a caller
    could not tell an admitted order from a queued one. Neither can be a 201 either: no run yet."""
    status, first = box.factory.compatibility("POST", "/dags/line/dagRuns", {"conf": {"issues": ["1"]}})
    assert (status, first) == (201, {"dag_run_id": box.airflow.created[0]["dag_run_id"]})

    status, second = box.factory.compatibility("POST", "/dags/line/dagRuns", {"conf": {"issues": ["2"]}})
    assert status == 202, "a queued order is accepted, not refused"
    assert second["state"] == "queued" and second["submission_id"]
    assert "dag_run_id" not in second, "there is no run to name yet"
    assert second["position"] == 1 and second["limiting"] is not None

    # The same order, seen through the canonical routes, agrees with what the mount said.
    queued = box.factory.control.admission.snapshot(limit=10)["queued"]
    assert [row["work_id"] for row in queued] == [second["submission_id"]]
    assert len(box.airflow.created) == 1


def test_the_compat_mount_still_refuses_work_that_will_never_fit(box) -> None:
    """A rejection is not a queue position, and must stay a refusal the caller has to act on."""
    box.factory.control.admission.limits = Limits(global_active=0)
    with pytest.raises(Refused) as caught:
        box.factory.compatibility("POST", "/dags/line/dagRuns", {"conf": {"issues": ["9"]}})
    assert caught.value.status == 429 and "rejected" in str(caught.value)
    assert box.airflow.created == []


# ---------------------------------------------------------------- box 3: lose the response


class _Clock:
    """A movable stand-in for ``time`` inside the inbox, so a retry backoff can actually elapse."""

    def __init__(self) -> None:
        self.offset = 0.0

    def time(self) -> float:
        return _real_time() + self.offset


def test_losing_the_backend_response_redelivers_into_the_same_work_order(
    box, receiver, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lose the answer, restart the receiver, redeliver: one work order, admitted and run once."""
    inbox, dispatcher, backend = receiver()
    _enqueue(inbox, "d-1")

    # The backend admits and dispatches; the answer never makes it back to the receiver.
    def submit_then_lose(request, timeout=None):  # noqa: ANN001 - urlopen signature
        backend(request, timeout)
        raise _http_error(504, "gateway timeout")

    dispatcher.opener = submit_then_lose
    assert dispatcher.dispatch_one() is True
    assert inbox.get("d-1").state == "pending", "an unanswered submission must stay retryable"
    admitted = [row["work_id"] for row in box.factory.control.admission.snapshot(limit=10)["active"]]
    assert len(admitted) == 1 and len(box.airflow.created) == 1

    # A restarted receiver reads the same durable envelope out of SQLite and asks again.
    reopened = DeliveryInbox(tmp_path / "inbox.sqlite3")
    resumed = Dispatcher(
        reopened,
        airflow_url="http://127.0.0.1:8080",
        token_provider=None,
        opener=FakeBackend(box),
        log=lambda _line: None,
        work_orders=WorkOrders(BACKEND, token="backend-token"),
    )
    # The lost attempt is due again once its backoff elapses; nothing else may claim it before then.
    assert resumed.dispatch_one() is False
    clock = _Clock()
    monkeypatch.setattr(dispatch, "time", clock)
    clock.offset = 3600
    assert resumed.dispatch_one() is True

    receipt = reopened.get("d-1")
    assert receipt.state == "dispatched"
    assert receipt.work_order_id == admitted[0], "the redelivery observed the same work order"
    assert len(box.airflow.created) == 1, "one execution, not two"
    assert len(box.factory.control.admission.snapshot(limit=10)["active"]) == 1


def test_an_inbox_refuses_to_replay_its_receipts_at_a_different_backend(box, receiver, tmp_path: Path) -> None:
    """These receipts are unadmitted work. Re-pointing them at another factory would admit work
    there that nobody sent it, so the endpoint is fenced the way the Airflow URL always was."""
    inbox, _dispatcher, _backend = receiver()
    _enqueue(inbox, "d-1")

    with pytest.raises(DeliveryConflict, match="work_order_url"):
        Dispatcher(
            DeliveryInbox(tmp_path / "inbox.sqlite3"),
            airflow_url="http://127.0.0.1:8080",
            token_provider=None,
            opener=FakeBackend(box),
            log=lambda _line: None,
            work_orders=WorkOrders("http://127.0.0.1:9999", token="backend-token"),
        )


def test_both_channels_submit_the_same_work_order_for_the_same_label(box, receiver) -> None:
    """Cross-channel duplicates meet one immutable work order, not two admissions."""
    inbox, dispatcher, backend = receiver()
    _enqueue(inbox, "d-1")
    dispatcher.dispatch_one()
    first = inbox.get("d-1").work_order_id

    # The label workflow submits the same line/issue/actor through the same canonical route.
    again = box.factory.operation("/work-orders", {"line": "line", "issues": ["42"], "actor": webhook.MANAGED_ACTOR})
    assert again["submission_id"] == first
    assert len(box.airflow.created) == 1


# ---------------------------------------------------------------- the label workflow


def test_the_label_workflow_submits_work_orders_and_never_curls_airflow() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "/v1/work-orders" in text
    assert "/api/v2/dags/" not in text, "the label intake must not write to Airflow directly"
    assert "SWF_BACKEND_URL" in text and "SWF_BACKEND_TOKEN" in text
    # The submitted actor is the one that makes a webhook duplicate the *same* work order.
    assert f'"actor": "{webhook.MANAGED_ACTOR}"' in text


# ---------------------------------------------------------------- the shipped wiring


def test_managed_serve_needs_a_backend_token_and_no_airflow_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The receiver holds one credential, for one boundary. Missing it must be a refusal at
    startup, not a receiver that silently falls back to writing DAG runs."""
    from typer.testing import CliRunner

    from swfactory.cli import app

    for var in ("AIRFLOW_TOKEN", "AIRFLOW_USER", "AIRFLOW_PASSWORD", "SWF_BACKEND_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["webhook", "serve", "--port", "0", "--backend-url", BACKEND])
    assert result.exit_code == 2
    assert "SWF_BACKEND_TOKEN" in result.output
    assert "AIRFLOW_TOKEN" not in result.output, "managed mode must not ask for an Airflow login"


def test_the_docker_receiver_is_wired_to_the_backend_not_to_airflow() -> None:
    """The reviewer's "the shipped wiring never reaches it": the stack itself has to be managed."""
    root = Path(__file__).resolve().parents[1]
    entrypoint = (root / "deploy" / "docker" / "webhook.sh").read_text(encoding="utf-8")
    assert "--backend-url" in entrypoint and "--airflow-url" not in entrypoint
    compose = (root / "deploy" / "docker" / "compose.yml").read_text(encoding="utf-8")
    service = compose[compose.index("\n  webhook:") : compose.index("\n  backend:")]
    assert "SWF_BACKEND_URL" in service
    assert "AIRFLOW_TOKEN" not in service and "AIRFLOW_PASSWORD" not in service
