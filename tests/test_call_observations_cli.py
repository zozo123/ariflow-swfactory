"""Operator call recovery commands are thin host-evidence wiring, never worker launchers."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest
from test_call_observations import paid as paid
from typer.testing import CliRunner

from swfactory import agent, cli, sandbox
from swfactory.call_accounting import CallLedger
from swfactory.models import StageError
from swfactory.state import RunBusyError


@pytest.fixture
def observations(monkeypatch):
    module = ModuleType("swfactory.call_observations")

    class ReconciliationRefused(ValueError):
        pass

    module.ReconciliationRefused = ReconciliationRefused
    module.list_calls = lambda *args, **kwargs: {"calls": []}
    module.reconcile_call = lambda *args, **kwargs: {"status": "reconciled"}
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module


def _invoke(*arguments: str):
    return CliRunner().invoke(cli.app, ["state", *arguments])


def _reconcile_args(*extra: str) -> tuple[str, ...]:
    return (
        "reconcile-call",
        "run-1",
        "call-1",
        "--observation",
        "/operator/provider-observation.json",
        "--actor",
        "operator@example.com",
        "--reason",
        "matched provider invoice",
        *extra,
    )


def test_calls_emits_json_and_passes_saved_identity(observations):
    captured = []

    def list_calls(root, run_id):
        captured.append((root, run_id))
        return {"run_id": run_id, "calls": [{"call_id": "call-1", "usage": "unknown"}]}

    observations.list_calls = list_calls
    result = _invoke("calls", "run-1", "--root", "/operator/saved-runs")
    assert result.exit_code == 0, result.output
    assert captured == [(Path("/operator/saved-runs"), "run-1")]
    assert json.loads(result.output) == {"run_id": "run-1", "calls": [{"call_id": "call-1", "usage": "unknown"}]}


def test_calls_uses_factory_default_root(observations):
    captured = []
    observations.list_calls = lambda root, run_id: captured.append((root, run_id)) or {"calls": []}
    result = _invoke("calls", "run-1")
    assert result.exit_code == 0 and captured == [(Path(".factory"), "run-1")]


def test_reconcile_passes_attestation_and_prints_receipt(observations):
    captured = []

    def reconcile_call(root, run_id, call_id, **kwargs):
        captured.append((root, run_id, call_id, kwargs))
        return {"run_id": run_id, "call_id": call_id, "status": "reconciled"}

    observations.reconcile_call = reconcile_call
    result = _invoke(*_reconcile_args("--root", "/operator/saved-runs"))
    assert result.exit_code == 0, result.output
    assert captured == [
        (
            Path("/operator/saved-runs"),
            "run-1",
            "call-1",
            {
                "observation": Path("/operator/provider-observation.json"),
                "actor": "operator@example.com",
                "reason": "matched provider invoice",
            },
        )
    ]
    assert json.loads(result.output) == {"run_id": "run-1", "call_id": "call-1", "status": "reconciled"}


@pytest.mark.parametrize("command", ["calls", "reconcile-call"])
@pytest.mark.parametrize(
    "error,code",
    [
        (FileNotFoundError("saved run is missing"), 3),
        (PermissionError("saved file is unreadable"), 1),
        (ValueError("saved observation is malformed"), 1),
        (StageError("policy", "observation is not authoritative"), 1),
        (RunBusyError("run is busy"), 1),
    ],
)
def test_command_errors_have_actionable_exit_codes(observations, command, error, code):
    def failed(*args, **kwargs):
        raise error

    if command == "calls":
        observations.list_calls = failed
        arguments = ("calls", "run-1")
    else:
        observations.reconcile_call = failed
        arguments = _reconcile_args()
    result = _invoke(*arguments)
    assert result.exit_code == code, result.output
    assert str(error) in result.output


def test_reconciliation_refusal_is_reported(observations):
    def refused(*args, **kwargs):
        raise observations.ReconciliationRefused("provider binding differs")

    observations.reconcile_call = refused
    result = _invoke(*_reconcile_args())
    assert result.exit_code == 1 and "provider binding differs" in result.output


def test_missing_observation_is_not_a_usage_error(observations, tmp_path):
    missing = tmp_path / "missing.json"

    def reconcile_call(root, run_id, call_id, *, observation, actor, reason):
        return json.loads(observation.read_text())

    observations.reconcile_call = reconcile_call
    result = _invoke(*_reconcile_args("--observation", str(missing)))
    assert result.exit_code == 3 and "missing.json" in result.output


def test_malformed_observation_is_reported_as_failed_reconciliation(observations, tmp_path):
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{not-json}")

    def reconcile_call(root, run_id, call_id, *, observation, actor, reason):
        return json.loads(observation.read_text())

    observations.reconcile_call = reconcile_call
    result = _invoke(*_reconcile_args("--observation", str(malformed)))
    assert result.exit_code == 1 and "call reconciliation refused" in result.output


@pytest.mark.parametrize("option", ["--actor", "--reason"])
@pytest.mark.parametrize("blank", ["", " \t "])
def test_nonblank_attestation_required_before_module_call(observations, option, blank):
    def forbidden(*args, **kwargs):
        pytest.fail("blank operator attestation must not reach reconciliation")

    observations.reconcile_call = forbidden
    result = _invoke(*_reconcile_args(option, blank))
    assert result.exit_code == 1 and "actor and reason must be nonblank" in result.output


@pytest.mark.parametrize("missing", ["--observation", "--actor", "--reason"])
def test_attestation_and_observation_flags_are_required(observations, missing):
    arguments = list(_reconcile_args())
    index = arguments.index(missing)
    del arguments[index : index + 2]
    result = _invoke(*arguments)
    assert result.exit_code == 2 and "Missing option" in result.output


def test_recovery_commands_never_construct_runtime_or_provider(observations, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("saved call recovery must not construct a runtime or provider")

    for name in ("Config", "build_ctx", "ctx_for", "make_scm"):
        monkeypatch.setattr(cli, name, forbidden)
    monkeypatch.setattr(agent, "make_agent", forbidden)
    monkeypatch.setattr(sandbox, "make_sandbox", forbidden)
    assert _invoke("calls", "run-1").exit_code == 0
    assert _invoke(*_reconcile_args()).exit_code == 0


def test_state_help_describes_operator_reconciliation(observations):
    result = _invoke("--help")
    assert result.exit_code == 0
    assert "reconcile host run evidence" in result.output
    assert "reconcile-call" in result.output and "calls" in result.output


def test_cli_inspects_and_reconciles_real_paid_call_after_observed_teardown(paid, tmp_path):
    ctx, fleet, record, _, document, evidence = paid
    root = str(ctx.run_dir.parent)
    before = _invoke("calls", ctx.cfg.run_id, "--root", root)
    assert before.exit_code == 0, before.output
    report = json.loads(before.output)
    assert report["charged_usd"] == report["unreconciled_usd"] == 2
    assert report["calls"][0]["call_id"] == record.call_id
    assert report["calls"][0]["binding"]["profile_id"] == "qualified"

    def reconcile(path):
        return _invoke(
            "reconcile-call",
            ctx.cfg.run_id,
            record.call_id,
            "--root",
            root,
            "--observation",
            str(path),
            "--actor",
            "operator:yossi",
            "--reason",
            "verified final provider aggregate after teardown",
        )

    missing = reconcile(tmp_path / "missing-observation.json")
    assert missing.exit_code == 3, missing.output
    invalid = tmp_path / "foreign-observation.json"
    invalid.write_text(json.dumps({**document, "profile_binding_sha256": "b" * 64}) + "\n")
    refused = reconcile(invalid)
    assert refused.exit_code == 1 and "observation does not match" in refused.output
    assert CallLedger(ctx.state).charged_usd() == CallLedger(ctx.state).unreconciled_usd() == 2
    assert not ctx.state.has_control("external-observations")

    imported = reconcile(evidence)
    assert imported.exit_code == 0, imported.output
    result = json.loads(imported.output)
    assert result["settled_usd"] == 0.37 and result["unreconciled"] is False
    assert CallLedger(ctx.state).record(record.call_id).settled_usd == 0.37
    digest = hashlib.sha256(evidence.read_bytes()).hexdigest()
    key = hashlib.sha256(record.call_id.encode()).hexdigest()
    archived = json.loads(ctx.state.read_control(f"external-observations/{key}/{digest}.json"))
    assert archived["actor"] == "operator:yossi"
    assert archived["reason"] == "verified final provider aggregate after teardown"
    assert archived["authority"] == "operator_attestation" and archived["observation_sha256"] == digest
    assert archived["source"] == str(evidence.resolve()) and archived["observation"] == document
    assert CallLedger(ctx.state).record(record.call_id).observation["evidence"] == archived

    after = _invoke("calls", ctx.cfg.run_id, "--root", root)
    assert after.exit_code == 0, after.output
    report = json.loads(after.output)
    assert report["charged_usd"] == report["calls"][0]["settled_usd"] == 0.37
    assert report["unreconciled_usd"] == 0 and len(fleet.commands) == 2
