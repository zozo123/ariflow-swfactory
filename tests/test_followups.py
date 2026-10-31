"""Follow-up intake is durable context, never a way around admission or approvals."""

import copy
import json

import pytest
from backend_support import REPO_SLUG

from swfactory.backend import followups
from swfactory.cells import CellIdentity
from swfactory.dispatch import DeliveryInbox
from swfactory.idempotency import OperationRef
from swfactory.webhook import WorkOrders, followup_trigger, repository_trigger, submit_work_order


@pytest.fixture
def published(factory, monkeypatch):
    cell = factory.cell_store.activate(CellIdentity(REPO_SLUG, "", "42"), "operator")
    ref = OperationRef.build(cell["cell_id"], cell["epoch"], "github_publish", "test")
    factory.control.operations.execute(ref, lambda: {"repository": REPO_SLUG, "pr_number": 7})
    comment = {
        "id": 55,
        "issue_url": f"https://api.github.com/repos/{REPO_SLUG}/issues/7",
        "body": "@factory revise\nPlease fix the edge case.",
        "user": {"type": "User", "login": "maintainer"},
        "author_association": "OWNER",
    }
    monkeypatch.setattr(followups.GitHubClient, "issue_comment", lambda self, number: copy.deepcopy(comment))
    return cell, comment, {"repository": REPO_SLUG, "pr_number": 7, "comment_id": 55}


def test_duplicate_and_restart_retain_one_request_without_launch(factory, published):
    from swfactory.cells import CellStore

    cell, comment, body = published
    before = factory.cell_store.get(cell["cell_id"])
    first = followups.receive(factory, body)
    comment["body"] = "edited after receipt"
    assert followups.receive(factory, body) == first
    assert factory.cell_store.get(cell["cell_id"]) == before
    reopened = CellStore(factory.cell_store.path)
    try:
        assert reopened.followup_attention() == [
            {key: value for key, value in first["followup"].items() if key != "body"}
        ]
    finally:
        reopened.close()
    assert len(followups.attention(factory, {})) == 1
    assert first["followup"]["disposition"] == "needs_admission"
    assert not factory.control.admission.snapshot()["active"]


@pytest.mark.parametrize(
    "mutation,disposition",
    [
        ({"author_association": "NONE"}, "refused"),
        ({"body": "Thanks for the change"}, "context"),
        ({"body": "@factory revise extra"}, "context"),
    ],
)
def test_only_exact_trusted_command_needs_attention(factory, published, mutation, disposition):
    _, comment, body = published
    comment.update(mutation)
    assert followups.receive(factory, body)["followup"]["disposition"] == disposition
    assert followups.attention(factory, {}) == []


def test_bot_cannot_create_a_revision(factory, published):
    _, comment, body = published
    comment["user"]["type"] = "Bot"
    assert followups.receive(factory, body)["state"] == "ignored"


@pytest.mark.parametrize(
    "change",
    [
        {"repository": "other/repo"},
        {"pr_number": 8},
        {"comment_id": True},
    ],
)
def test_unrelated_or_invalid_source_does_not_attach(factory, published, change):
    _, _, body = published
    body.update(change)
    if change == {"pr_number": 8}:
        assert followups.receive(factory, body)["state"] == "ignored"
    else:
        with pytest.raises(ValueError):
            followups.receive(factory, body)
    assert followups.attention(factory, {}) == []


def test_remote_comment_must_belong_to_the_recorded_pr(factory, published):
    _, comment, body = published
    comment["issue_url"] = f"https://api.github.com/repos/{REPO_SLUG}/issues/999"
    with pytest.raises(ValueError, match="does not belong"):
        followups.receive(factory, body)


def test_dismissal_is_epoch_fenced_and_does_not_approve(factory, published):
    cell, _, body = published
    row = followups.receive(factory, body)["followup"]
    with pytest.raises(ValueError, match="Cell moved"):
        followups.dismiss(factory, {**row, "epoch": 999, "reason": "reviewed"})
    assert len(followups.attention(factory, {})) == 1
    assert followups.dismiss(factory, {**row, "reason": "handled in separate work order"})["state"] == "dismissed"
    assert followups.attention(factory, {}) == []
    assert not factory.control.admission.snapshot()["active"]
    assert factory.cell_store.get(cell["cell_id"])["epoch"] == cell["epoch"]


def test_authenticated_backend_routes(client, published):
    _, _, body = published
    assert client.call("POST", "/v1/followups/receive", body, token=None)[0] == 401
    assert client.call("POST", "/v1/followups/receive", body)[0] == 200
    status, rows = client.call("POST", "/v1/followups/attention", {})
    assert status == 200 and len(rows) == 1


def test_queue_reopens_and_deduplicates_comment_identity(tmp_path):
    payload = {
        "action": "created",
        "repository": {"full_name": REPO_SLUG},
        "issue": {"number": 7, "pull_request": {}},
        "comment": {"id": 55, "user": {"type": "User"}},
    }
    trigger = followup_trigger("issue_comment", payload)
    assert trigger is not None
    trigger = repository_trigger(trigger, REPO_SLUG)
    path = tmp_path / "inbox.sqlite3"
    inbox = DeliveryInbox(path)
    raw = json.dumps(payload).encode()
    row, created = inbox.enqueue("delivery-1", "issue_comment", raw, REPO_SLUG, trigger)
    assert created
    again, created = DeliveryInbox(path).enqueue("delivery-1", "issue_comment", raw, REPO_SLUG, trigger)
    assert not created and row == again


def test_submission_uses_only_backend_followup_boundary():
    from backend_support import FakeResponse

    payload = {
        "action": "created",
        "repository": {"full_name": REPO_SLUG},
        "issue": {"number": 7, "pull_request": {}},
        "comment": {"id": 55, "user": {"type": "User"}},
    }
    calls = []

    def opener(request, timeout=0):
        calls.append(request)
        return FakeResponse({"submission_id": "github-comment:55", "state": "recorded"})

    submit_work_order(
        followup_trigger("issue_comment", payload), WorkOrders("http://localhost:8082", "t" * 40), opener=opener
    )
    assert calls[0].full_url.endswith("/v1/followups/receive")
    assert set(json.loads(calls[0].data)) == {"repository", "pr_number", "comment_id"}


def test_cancelled_work_is_not_resumed(factory, published):
    cell, _, body = published
    factory.cell_store.patch(cell["cell_id"], cell["epoch"], "cancel", state="cancelled")
    assert followups.receive(factory, body)["followup"]["disposition"] == "stopped"
    assert followups.attention(factory, {}) == []
    assert factory.cell_store.get(cell["cell_id"])["state"] == "cancelled"


def test_new_epoch_keeps_original_publication_lineage(factory, published):
    cell, _, body = published
    factory.cell_store.take_epoch(cell["cell_id"], cell["epoch"], "replacement")
    record = followups.receive(factory, body)["followup"]
    assert record["epoch"] == cell["epoch"] + 1
    assert record["publication_epoch"] == cell["epoch"]
    assert not factory.control.admission.snapshot()["active"]


def test_drain_refuses_new_records_and_dismissals(factory, published, monkeypatch):
    _, _, body = published
    row = followups.receive(factory, body)["followup"]
    monkeypatch.setenv("SWF_DRAIN", "1")
    with pytest.raises(ValueError, match="draining"):
        followups.receive(factory, body)
    with pytest.raises(ValueError, match="draining"):
        followups.dismiss(factory, {**row, "reason": "handled"})
    assert len(followups.attention(factory, {})) == 1


def test_multiple_delivery_ids_dispatch_one_comment_without_airflow(factory, published, tmp_path):
    from backend_support import FakeResponse

    from swfactory.dispatch import Dispatcher
    from swfactory.webhook import Trigger

    _, _, body = published
    inbox = DeliveryInbox(tmp_path / "inbox.sqlite3")
    trigger = Trigger("followup", {"_followup": body})
    for delivery_id in ("first", "redelivery"):
        inbox.enqueue(delivery_id, "issue_comment", b"same payload", REPO_SLUG, trigger)
    calls = []

    def opener(request, timeout=0):
        calls.append(request.full_url)
        assert request.full_url.endswith("/v1/followups/receive")
        return FakeResponse(followups.receive(factory, json.loads(request.data)))

    dispatcher = Dispatcher(
        DeliveryInbox(inbox.path),
        airflow_url="http://localhost:8080",
        token_provider=None,
        work_orders=WorkOrders("http://localhost:8082", "t" * 40),
        opener=opener,
        log=lambda _: None,
    )
    assert dispatcher.dispatch_one()
    assert dispatcher.dispatch_one()
    assert not dispatcher.dispatch_one()
    assert len(calls) == 2
    assert len(followups.attention(factory, {})) == 1
    assert inbox.get("redelivery").work_order_id == "github-comment:55"


@pytest.mark.parametrize("user", [None, [], "bot", {"type": "Bot"}])
def test_malformed_and_bot_webhooks_are_ignored(user):
    assert (
        followup_trigger(
            "issue_comment",
            {
                "action": "created",
                "issue": {"number": 7, "pull_request": {}},
                "comment": {"id": 55, "user": user},
            },
        )
        is None
    )


def test_signed_receiver_queues_pr_context_only_in_managed_mode(tmp_path):
    import hashlib
    import hmac
    import threading
    from http.server import ThreadingHTTPServer

    from test_webhook import post

    from swfactory.webhook import make_handler

    raw = json.dumps(
        {
            "action": "created",
            "repository": {"full_name": REPO_SLUG},
            "issue": {"number": 7, "pull_request": {}},
            "comment": {"id": 55, "user": {"type": "User"}},
        }
    ).encode()
    headers = {
        "X-GitHub-Event": "issue_comment",
        "X-GitHub-Delivery": "source-1",
        "X-Hub-Signature-256": "sha256=" + hmac.new(b"secret", raw, hashlib.sha256).hexdigest(),
    }
    for managed in (False, True):
        inbox = DeliveryInbox(tmp_path / str(managed) / "inbox.sqlite3")
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            make_handler(
                airflow_url="http://localhost:8080",
                token_provider=lambda: "unused",
                secret="secret",
                inbox=inbox,
                work_orders=WorkOrders("http://localhost:8082", "t" * 40) if managed else None,
                log=lambda _: None,
            ),
        )
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            port = server.server_address[1]
            assert post(port, raw, {**headers, "X-Hub-Signature-256": "bad"})[0] == 401
            status, result = post(port, raw, headers)
            assert status == (202 if managed else 200)
            if managed:
                assert inbox.get("source-1").conf["_followup"]["comment_id"] == 55
                assert post(port, raw, headers)[1]["duplicate"]
            else:
                assert result["routed"] is False
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


def test_invalid_backend_receipt_keeps_comment_retryable(tmp_path):
    from backend_support import FakeResponse

    from swfactory.dispatch import Dispatcher
    from swfactory.webhook import Trigger

    inbox = DeliveryInbox(tmp_path / "inbox.sqlite3")
    inbox.enqueue(
        "retry-me",
        "issue_comment",
        b"payload",
        REPO_SLUG,
        Trigger(
            "followup",
            {"_followup": {"repository": REPO_SLUG, "pr_number": 7, "comment_id": 55}},
        ),
    )
    dispatcher = Dispatcher(
        inbox,
        airflow_url="http://localhost:8080",
        token_provider=None,
        work_orders=WorkOrders("http://localhost:8082", "t" * 40),
        opener=lambda request, timeout=0: FakeResponse(b"temporary proxy response"),
        log=lambda _: None,
    )
    assert dispatcher.dispatch_one()
    receipt = inbox.get("retry-me")
    assert receipt.state == "pending"
    assert receipt.last_error == "work_order_invalid_response"
