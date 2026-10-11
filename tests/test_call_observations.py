"""Trusted operator imports release only completed, exactly bound factory calls."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_external_launch_recovery import Fleet, context

from swfactory import accepted_inputs
from swfactory.call_accounting import CallLedger, ReconciliationRefused
from swfactory.call_observations import list_calls, reconcile_call
from swfactory.models import StageError
from swfactory.stages import _agent, seed_budget
from swfactory.state import RunBusyError, RunState


@pytest.fixture
def paid(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    ctx.cfg = ctx.cfg.model_copy(update={"run_id": "recover01"})
    ctx.run_dir = tmp_path / ".factory" / ctx.cfg.run_id
    ctx.sb.state = ctx.state
    accepted_inputs.admit(ctx.state, accepted_inputs.snapshot(ctx.cfg, None, ctx.issue))
    fleet = Fleet(monkeypatch)
    _agent(ctx, "spec", 1, "prompt", None)
    (record,) = CallLedger(ctx.state).records()
    key = hashlib.sha256(record.call_id.encode()).hexdigest()
    descriptor = json.loads(ctx.state.read_control(f"external-calls/{key}.json"))
    document = {
        "schema_version": 1,
        "run_id": ctx.cfg.run_id,
        "call_id": record.call_id,
        "accepted_inputs_digest": descriptor["accepted_inputs_digest"],
        "profile_binding_sha256": descriptor["profile_binding_sha256"],
        "provider": "billing-export",
        "account_id": "operator-account",
        "request_id": "aggregate-call-request",
        "evidence_reference": "invoice-final-1",
        "observed_at": datetime.now(UTC).isoformat(),
        "status": "committed",
        "final": True,
        "cost_usd": 0.37,
    }
    evidence = tmp_path / "operator-observation.json"
    evidence.write_text(json.dumps(document) + "\n")
    return ctx, fleet, record, descriptor, document, evidence


def apply(paid, **changes):
    ctx, _, record, _, document, evidence = paid
    evidence.write_text(json.dumps({**document, **changes}) + "\n")
    return reconcile_call(
        ctx.run_dir.parent,
        ctx.cfg.run_id,
        record.call_id,
        observation=evidence,
        actor="operator:yossi",
        reason="verified final aggregate bill after teardown",
    )


def test_paid_usage_reconciles_and_a_retained_context_can_continue(paid):
    ctx, fleet, record, _, _, evidence = paid
    assert seed_budget(ctx, refresh=True) == 2
    report = list_calls(ctx.run_dir.parent, ctx.cfg.run_id)
    assert report["charged_usd"] == report["unreconciled_usd"] == 2
    assert report["calls"][0]["binding"]["profile_id"] == "qualified"
    result = apply(paid)
    assert result["settled_usd"] == 0.37 and result["unreconciled"] is False
    settled = CallLedger(ctx.state).record(record.call_id)
    imported = settled.observation["evidence"]
    assert imported["authority"] == "operator_attestation"
    assert imported["actor"] == "operator:yossi" and imported["observation"]["final"] is True
    assert imported["observation_sha256"] == hashlib.sha256(evidence.read_bytes()).hexdigest()
    assert settled.receipt is None  # candidate session metadata did not settle the charge
    _agent(ctx, "plan", 1, "next stage", None)
    assert len(fleet.commands) == 4
    assert CallLedger(ctx.state).charged_usd() == 2.37


def test_calls_inspection_never_mutates_state(paid):
    ctx, _, _, _, _, _ = paid
    before = {path: path.read_bytes() for path in ctx.run_dir.rglob("*") if path.is_file()}
    list_calls(ctx.run_dir.parent, ctx.cfg.run_id)
    assert before == {path: path.read_bytes() for path in ctx.run_dir.rglob("*") if path.is_file()}


def test_bill_above_the_grant_is_counted_and_blocks_more_work(paid):
    ctx, fleet, _, _, _, _ = paid
    apply(paid, cost_usd=4.5)
    assert CallLedger(ctx.state).charged_usd() == 4.5
    with pytest.raises(StageError, match="budget exhausted"):
        _agent(ctx, "plan", 1, "next stage", None)
    assert len(fleet.commands) == 2


def test_ambiguous_observation_retains_the_reservation(paid):
    ctx, _, record, _, _, _ = paid
    result = apply(paid, status="ambiguous", final=False, cost_usd=None)
    assert result["settled_usd"] is None and result["unreconciled"] is True
    assert CallLedger(ctx.state).charged_usd() == 2
    with pytest.raises(StageError, match="unresolved"):
        _agent(ctx, "spec", 1, "retry", None)
    assert CallLedger(ctx.state).record(record.call_id).observation["status"] == "ambiguous"


def test_definite_non_submission_settles_to_zero(paid):
    ctx, _, _, _, _, _ = paid
    assert apply(paid, status="definitely_absent", cost_usd=None)["settled_usd"] == 0
    assert CallLedger(ctx.state).charged_usd() == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": True},
        {"run_id": "another"},
        {"call_id": "foreign.1:abc"},
        {"accepted_inputs_digest": "inputs:" + "b" * 64},
        {"profile_binding_sha256": "b" * 64},
        {"cost_usd": True},
        {"cost_usd": -1},
        {"cost_usd": "0.1"},
        {"cost_usd": float("nan")},
        {"cost_usd": float("inf")},
        {"final": False},
        {"observed_at": "2026-10-11T00:00:00"},
        {"observed_at": "1970-01-01T00:00:00Z"},
        {"account_id": " "},
        {"request_id": ""},
        {"candidate": {"usage": 0}},
        {"status": "definitely_absent", "cost_usd": 0},
    ],
)
def test_invalid_or_mismatched_evidence_cannot_release_money(paid, changes):
    ctx, _, _, _, _, _ = paid
    with pytest.raises((ValueError, ReconciliationRefused)):
        apply(paid, **changes)
    assert CallLedger(ctx.state).charged_usd() == CallLedger(ctx.state).unreconciled_usd() == 2


@pytest.mark.parametrize("kind", ["duplicate", "oversized", "candidate", "artifact", "symlink", "hardlink"])
def test_candidate_files_or_invalid_json_cannot_be_billing_evidence(paid, kind):
    ctx, _, record, _, _, evidence = paid
    if kind == "duplicate":
        raw = evidence.read_text()
        evidence.write_text(raw.replace('"cost_usd": 0.37', '"cost_usd": 0.37, "cost_usd": 0'))
    elif kind == "oversized":
        evidence.write_text(" " * (64 * 1024 + 1))
    else:
        if kind == "candidate":
            destination = Path(ctx.sb.workdir) / "billing.json"
        elif kind == "artifact":
            destination = ctx.state.artifacts / "billing.json"
        else:
            destination = evidence.with_name("linked.json")
        destination.parent.mkdir(parents=True, exist_ok=True)
        if kind == "symlink":
            destination.symlink_to(evidence)
        elif kind == "hardlink":
            destination.hardlink_to(evidence)
        else:
            destination.write_bytes(evidence.read_bytes())
        evidence = destination
    with pytest.raises((ValueError, ReconciliationRefused)):
        reconcile_call(
            ctx.run_dir.parent, ctx.cfg.run_id, record.call_id, observation=evidence, actor="operator", reason="bill"
        )
    assert CallLedger(ctx.state).unreconciled_usd() == 2


def test_fifo_observation_is_refused_without_blocking_run_ownership(paid):
    ctx, _, record, _, _, evidence = paid
    pipe = evidence.with_name("operator-pipe")
    os.mkfifo(pipe)
    # A blocking open would hang indefinitely without a writer; bound the regression in a child.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "swfactory.cli",
            "state",
            "reconcile-call",
            ctx.cfg.run_id,
            record.call_id,
            "--root",
            str(ctx.run_dir.parent),
            "--observation",
            str(pipe),
            "--actor",
            "operator",
            "--reason",
            "bill",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    assert "regular file" in result.stderr
    assert CallLedger(ctx.state).unreconciled_usd() == 2
    with ctx.state.exclusive("still-available"):
        pass


def test_expanded_home_symlink_is_not_operator_evidence(paid, monkeypatch):
    ctx, _, record, _, _, evidence = paid
    link = evidence.with_name("linked.json")
    link.symlink_to(evidence)
    monkeypatch.setattr(Path, "expanduser", lambda path: link if str(path) == "~/linked.json" else path)
    with pytest.raises(ReconciliationRefused, match="operator files"):
        reconcile_call(
            ctx.run_dir.parent,
            ctx.cfg.run_id,
            record.call_id,
            observation=Path("~/linked.json"),
            actor="operator",
            reason="bill",
        )
    assert CallLedger(ctx.state).unreconciled_usd() == 2


@pytest.mark.parametrize("kind", ["unfinished", "foreign-label", "missing-cid", "substituted-cid", "legacy-intent"])
def test_a_free_host_lock_does_not_prove_remote_execution_has_ended(paid, kind):
    ctx, _, _, _, _, _ = paid
    journal = ctx.state._path("external-processes.jsonl")
    rows = ctx.state.read_jsonl("external-processes.jsonl")
    if kind == "unfinished":
        rows = rows[:-1]
    elif kind == "foreign-label":
        rows[-1]["labels"]["swfactory.epoch"] = "2"
    elif kind == "missing-cid":
        rows = [row for row in rows if row["event"] not in {"created", "start_intent"}]
    elif kind == "substituted-cid":
        next(row for row in rows if row["event"] == "start_intent")["container_id"] = "c" * 64
    else:
        rows[0].pop("container_id_required")
    journal.write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ReconciliationRefused):
        apply(paid)
    assert CallLedger(ctx.state).unreconciled_usd() == 2


def test_identical_replay_is_a_noop_but_different_settlement_is_refused(paid):
    ctx, _, _, _, _, _ = paid
    apply(paid)
    ledger_before = ctx.state.read_control("agent_calls.jsonl")
    assert apply(paid)["already_applied"] is True
    assert ctx.state.read_control("agent_calls.jsonl") == ledger_before
    with pytest.raises(ReconciliationRefused, match="immutable"):
        apply(paid, cost_usd=0.1)
    assert CallLedger(ctx.state).charged_usd() == 0.37


def test_archived_evidence_survives_a_crash_before_the_ledger_append(paid, monkeypatch):
    ctx, _, record, _, _, _ = paid
    original = CallLedger.reconcile

    def crash(*args, **kwargs):
        raise OSError("worker died before ledger append")

    monkeypatch.setattr(CallLedger, "reconcile", crash)
    with pytest.raises(OSError, match="worker died"):
        apply(paid)
    (archived,) = list(ctx.state._path("external-observations").rglob("*.json"))
    saved = archived.read_bytes()
    assert CallLedger(ctx.state).unreconciled_usd() == 2
    monkeypatch.setattr(CallLedger, "reconcile", original)
    apply(paid)
    assert archived.read_bytes() == saved
    assert CallLedger(ctx.state).record(record.call_id).settled_usd == 0.37


def test_observation_uses_historical_call_binding_after_reaccept(paid):
    ctx, _, _, _, _, _ = paid
    original = accepted_inputs.stored(ctx.state)
    accepted_inputs.reaccept(ctx.state, actor="operator", reason="changed issue")
    accepted_inputs.admit(ctx.state, original.model_copy(update={"issue_sha256": "b" * 64}))
    assert apply(paid)["settled_usd"] == 0.37


def test_concurrent_owner_blocks_import_before_money_or_evidence_changes(paid):
    ctx, _, _, _, _, _ = paid
    ledger = ctx.state.read_control("agent_calls.jsonl")
    with ctx.state.exclusive("active-stage"), pytest.raises(RunBusyError):
        apply(paid)
    assert ctx.state.read_control("agent_calls.jsonl") == ledger
    assert not ctx.state.has_control("external-observations")


def test_legacy_calls_without_host_descriptors_are_not_inferred_from_candidates(paid):
    ctx, _, record, _, _, _ = paid
    ctx.state.clear_control(f"external-calls/{hashlib.sha256(record.call_id.encode()).hexdigest()}.json")
    assert list_calls(ctx.run_dir.parent, ctx.cfg.run_id)["calls"][0]["binding"] is None
    with pytest.raises(ReconciliationRefused, match="pre-launch"):
        apply(paid)
    assert CallLedger(ctx.state).unreconciled_usd() == 2


def test_missing_run_inspection_does_not_create_state(tmp_path):
    with pytest.raises(FileNotFoundError):
        list_calls(tmp_path, "missing")
    assert list(tmp_path.iterdir()) == []


def test_malformed_money_never_becomes_zero_in_operator_reporting(tmp_path):
    state = RunState(tmp_path / "saved")
    state.write_control("agent_calls.jsonl", '{"call_id":"call","reserved_usd":NaN}\n')
    with pytest.raises(ValueError, match="reserved_usd"):
        list_calls(tmp_path, "saved")
