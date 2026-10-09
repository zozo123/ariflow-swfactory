"""Operator status crosses real CLI/HTTP/SQLite boundaries without changing gate authority."""

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from backend_support import TOKEN
from typer.testing import CliRunner

from swfactory.autonomy import AutonomyStore, record_merge_timing
from swfactory.autonomy_status import remote_status, status
from swfactory.cli import app
from swfactory.inspection import inspect_run
from swfactory.models import Issue
from swfactory.state import RunState


def test_blocked_triage_is_visible_through_authenticated_http_and_cli(client, factory, monkeypatch):
    from swfactory.scm import GitHubScm

    monkeypatch.setattr(GitHubScm, "fetch_issue", lambda self, ref: Issue(id=ref, title="Fix", body="Useful work"))
    code, result = client.call("POST", "/v1/scm/triage", {"issue": "101"})
    assert code == 200 and result["state"] == "blocked"
    assert client.call("POST", "/v1/scm/autonomy-status", {}, token=None)[0] == 401
    monkeypatch.setenv("SWF_BACKEND_TOKEN", TOKEN)
    output = CliRunner().invoke(
        app, ["state", "autonomy", "--backend-url", f"http://{client.host}:{client.port}", "--json"]
    )
    assert output.exit_code == 0, output.output
    row = json.loads(output.output)["decisions"][0]
    assert row["issue"] == "101" and row["reason"] == "required_labels_missing"
    assert "required labels" in row["next_action"] and row["recorded_at"]
    assert TOKEN not in output.output
    assert not factory.cell_store.list(), "blocked triage must not admit a Cell"
    before = row["recorded_at"]
    client.call("POST", "/v1/scm/triage", {"issue": "101"})
    assert status(factory.state_root)["decisions"][0]["recorded_at"] == before


def test_status_reads_old_schema_and_never_creates_missing_state(tmp_path):
    assert status(tmp_path)["decisions"] == []
    assert not (tmp_path / "autonomy.sqlite3").exists()
    with sqlite3.connect(tmp_path / "autonomy.sqlite3") as db:
        db.execute("CREATE TABLE decisions (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.execute(
            "INSERT INTO decisions VALUES (?,?)",
            (
                "triage:acme/repo:1:revision:digest",
                json.dumps({"state": "blocked", "reason": "denied_label"}, sort_keys=True, separators=(",", ":")),
            ),
        )
    row = status(tmp_path)["decisions"][0]
    assert row["recorded_at"] is None and "blocking condition" in row["next_action"]
    with sqlite3.connect(tmp_path / "autonomy.sqlite3") as db:
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='decision_times'").fetchone()
    AutonomyStore(tmp_path / "autonomy.sqlite3").bind(
        "triage:acme/repo:1:revision:digest", {"state": "blocked", "reason": "denied_label"}
    )
    assert status(tmp_path)["decisions"][0]["recorded_at"] is None


@pytest.mark.parametrize("limit", [0, 1001, -1])
def test_status_bounds(limit, tmp_path):
    with pytest.raises(ValueError):
        status(tmp_path, limit=limit)


def test_status_response_contains_only_operator_fields(tmp_path):
    AutonomyStore(tmp_path / "autonomy.sqlite3").bind(
        "cell_test:1:publication", {"evidence": {"secret": "do-not-expose"}}
    )
    assert "do-not-expose" not in json.dumps(status(tmp_path))


def test_local_status_ignores_backend_environment_and_preserves_recorded_revision(tmp_path, monkeypatch):
    AutonomyStore(tmp_path / "autonomy.sqlite3").bind(
        "triage:acme/repo:1:old:digest", {"state": "eligible", "revision": "old"}
    )
    monkeypatch.setenv("SWF_BACKEND_URL", "http://invalid.example")
    output = CliRunner().invoke(app, ["state", "autonomy", "--local", "--root", str(tmp_path), "--json"])
    assert output.exit_code == 0, output.output
    result = json.loads(output.output)
    assert result["decisions"][0]["policy_revision"] == "old"
    assert result["policy_revision"] != "old"


@pytest.mark.parametrize(
    "url", ["http://example.com", "https://user:secret@example.com", "https://example.com?token=secret"]
)
def test_remote_status_rejects_unsafe_urls(url):
    with pytest.raises(ValueError):
        remote_status(url, "token")


def test_merge_wait_timings_survive_reschedule_and_remain_read_only(tmp_path, monkeypatch):
    import swfactory.autonomy as module
    from swfactory.call_accounting import CallLedger

    current = datetime(2026, 10, 3, tzinfo=UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return current

    monkeypatch.setattr(module, "datetime", Clock)
    state = RunState(tmp_path / "run1")
    ctx = SimpleNamespace(state=state)
    state.write_control("setup-timing.json", json.dumps({"duration_s": 2.5}))
    call = CallLedger(state).reserve(stage="plan", iteration=0, reserved_usd=1)
    CallLedger(state).settle(call, cost_usd=0.1, receipt={"duration_ms": 1500, "agent": "claude"})
    record_merge_timing(ctx)
    record_merge_timing(ctx, {"state": "pending", "reason": "check running: candidate-readiness"})
    current += timedelta(seconds=30)
    record_merge_timing(ctx)
    record_merge_timing(ctx, {"state": "merged"})
    current += timedelta(seconds=30)
    record_merge_timing(ctx)
    record_merge_timing(ctx, {"state": "merged"})
    result = inspect_run(tmp_path, "run1")
    assert result["timings"] == {
        "queue_s": None,
        "setup_s": 2.5,
        "provider_reported_s": 1.5,
        "merge_wait_s": 30,
        "ci_checks_wait_s": 30,
    }
    assert result["recorded_cost_usd"] == 0
