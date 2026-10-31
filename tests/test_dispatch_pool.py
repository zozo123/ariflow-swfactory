"""Slow upstream requests must not serialize independent durable deliveries."""

import threading
import time
from pathlib import Path

import pytest

from swfactory.dispatch import DeliveryInbox, Dispatcher
from swfactory.webhook import Trigger, WorkOrders


def _dispatcher(tmp_path: Path, workers: int) -> Dispatcher:
    return Dispatcher(
        DeliveryInbox(tmp_path / "inbox.sqlite3"),
        airflow_url="http://localhost:8080",
        token_provider=None,
        opener=lambda *args, **kwargs: None,
        log=lambda line: None,
        work_orders=WorkOrders("http://localhost:8082", "test-token"),
        workers=workers,
    )


def _wait_dispatched(dispatcher: Dispatcher, delivery_id: str) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if dispatcher.inbox.get(delivery_id).state == "dispatched":
            return
        time.sleep(0.01)
    pytest.fail(f"{delivery_id} was not dispatched")


def test_slow_submission_does_not_block_other_receipts(tmp_path: Path, monkeypatch) -> None:
    dispatcher = _dispatcher(tmp_path, 2)
    blocked, release = threading.Event(), threading.Event()
    calls = []

    def submit(delivery):
        calls.append(delivery.delivery_id)
        if delivery.delivery_id == "slow":
            blocked.set()
            assert release.wait(3)
        return {"submission_id": delivery.delivery_id, "state": "queued"}

    monkeypatch.setattr(dispatcher, "_submit", submit)
    dispatcher.inbox.enqueue("slow", "issues", b"slow", "owner/repo", Trigger("line", {}))
    dispatcher.start()
    try:
        assert blocked.wait(3)
        dispatcher.inbox.enqueue("fast", "issues", b"fast", "owner/repo", Trigger("line", {}))
        dispatcher.notify()
        _wait_dispatched(dispatcher, "fast")
        assert dispatcher.inbox.get("slow").state == "dispatching"
        assert dispatcher.inbox.get("fast").admission_state == "queued"
        assert dispatcher.is_alive()
        release.set()
        _wait_dispatched(dispatcher, "slow")
        assert sorted(calls) == ["fast", "slow"]
    finally:
        release.set()
        dispatcher.close()
    assert not any(thread.is_alive() for thread in dispatcher.threads)


def test_pool_bounds_concurrency_and_claims_each_delivery_once(tmp_path: Path, monkeypatch) -> None:
    dispatcher = _dispatcher(tmp_path, 2)
    release, both_started = threading.Event(), threading.Event()
    lock = threading.Lock()
    calls = []
    active = peak = 0

    def submit(delivery):
        nonlocal active, peak
        with lock:
            calls.append(delivery.delivery_id)
            active += 1
            peak = max(peak, active)
            if active == 2:
                both_started.set()
        assert release.wait(3)
        with lock:
            active -= 1
        return {"submission_id": delivery.delivery_id, "state": "queued"}

    monkeypatch.setattr(dispatcher, "_submit", submit)
    for i in range(8):
        dispatcher.inbox.enqueue(str(i), "issues", str(i).encode(), "owner/repo", Trigger("line", {}))
    dispatcher.start()
    try:
        assert both_started.wait(3)
        with lock:
            assert len(calls) == 2
        release.set()
        for i in range(8):
            _wait_dispatched(dispatcher, str(i))
        assert sorted(calls) == [str(i) for i in range(8)]
        assert peak == 2
    finally:
        release.set()
        dispatcher.close()


@pytest.mark.parametrize("workers", [0, 33])
def test_rejects_unbounded_or_empty_pool(tmp_path: Path, workers: int) -> None:
    with pytest.raises(ValueError, match="workers must be between"):
        _dispatcher(tmp_path, workers)


def test_close_before_start_is_safe(tmp_path: Path) -> None:
    dispatcher = _dispatcher(tmp_path, 4)
    assert not dispatcher.is_alive()
    dispatcher.close()


def test_cli_passes_worker_setting_from_environment(tmp_path: Path, monkeypatch) -> None:
    from typer.testing import CliRunner

    from swfactory import webhook
    from swfactory.cli import app

    received = {}
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "test-token")
    monkeypatch.setenv("SWF_WEBHOOK_DISPATCH_WORKERS", "7")
    monkeypatch.setattr(webhook, "serve", lambda port, **kwargs: received.update(kwargs))
    result = CliRunner().invoke(
        app,
        ["webhook", "serve", "--backend-url", "http://localhost:8082", "--inbox", str(tmp_path / "cli.sqlite3")],
    )
    assert result.exit_code == 0, result.output
    assert received["dispatch_workers"] == 7


def test_http_readiness_detects_a_failed_worker(tmp_path: Path, monkeypatch) -> None:
    import http.client

    from swfactory import webhook

    dispatcher = _dispatcher(tmp_path, 2)
    monkeypatch.setattr(webhook, "Dispatcher", lambda *args, **kwargs: dispatcher)
    server = webhook.make_server(
        0,
        host="127.0.0.1",
        airflow_url="http://localhost:8080",
        inbox=dispatcher.inbox,
        work_orders=WorkOrders("http://localhost:8082", "test-token"),
        log=lambda line: None,
    )
    serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    serving.start()

    def status():
        connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
        try:
            connection.request("GET", "/readyz")
            response = connection.getresponse()
            response.read()
            return response.status
        finally:
            connection.close()

    try:
        assert status() == 200
        # One dead worker must fail readiness even while another remains alive.
        monkeypatch.setattr(dispatcher.threads[0], "is_alive", lambda: False)
        assert status() == 503
    finally:
        monkeypatch.undo()
        server.shutdown()
        server.server_close()
        serving.join(3)
