"""Opt-in real Docker recovery of a paid grant, using a provider-free stdlib wrapper."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest
from test_external_launch_recovery import context
from test_external_sandbox import live_image as live_image
from typer.testing import CliRunner

from swfactory import accepted_inputs
from swfactory.call_accounting import CallLedger
from swfactory.cli import app
from swfactory.external_agent import ExternalAgent
from swfactory.models import StageError
from swfactory.sandbox import DockerContainers, DockerSandbox
from swfactory.sandbox_governance import SandboxIdentity
from swfactory.stages import _agent


def test_real_docker_call_cli_billing_reconciliation_and_remaining_grant(tmp_path, live_image):
    ctx = context(tmp_path, max_budget_usd=2.0)
    profile_path = tmp_path / "profile.json"
    profile = json.loads(profile_path.read_text())
    # This wrapper buys nothing. It exercises the hard-cap accounting branch without
    # claiming to qualify a vendor runtime or contacting a model provider.
    wrapper = (
        "import json,pathlib,sys;"
        "request=json.loads(pathlib.Path(sys.argv[-1]).read_text());"
        "print(json.dumps({'schema_version':1,'call_id':request['call_id'],"
        "'profile_id':request['profile_id'],'status':'success',"
        "'text':json.dumps({'granted_usd':request['budget']['usd']}),'num_turns':1}))"
    )
    profile["argv"] = ["/usr/local/bin/python", "-c", wrapper]
    profile_path.write_text(json.dumps(profile) + "\n")
    ctx.cfg = ctx.cfg.model_copy(update={"run_id": "livecall01", "docker_image": live_image})
    ctx.run_dir = tmp_path / ".factory" / ctx.cfg.run_id
    ctx.agent = ExternalAgent(ctx.cfg)
    ctx.sb = DockerSandbox(
        ctx.sb.root,
        image=live_image,
        identity=SandboxIdentity("docker", "live-call-recovery", 1, ctx.cfg.run_id),
        state=ctx.state,
        network="none",
    )
    accepted_inputs.admit(ctx.state, accepted_inputs.snapshot(ctx.cfg, None, ctx.issue))
    runner = CliRunner()
    root = str(ctx.run_dir.parent)

    def calls():
        result = runner.invoke(app, ["state", "calls", ctx.cfg.run_id, "--root", root])
        assert result.exit_code == 0, result.output
        return json.loads(result.output)

    try:
        with ctx.state.exclusive("live-spec"):
            first = _agent(ctx, "spec", 1, "first fixture invocation", None)
        assert json.loads(first.text)["granted_usd"] == 2
        ledger = CallLedger(ctx.state)
        (record,) = ledger.records()
        before = calls()
        assert before["charged_usd"] == before["unreconciled_usd"] == 2
        assert before["calls"][0]["call_id"] == record.call_id and record.settled_usd is None
        assert DockerContainers().containers({**ctx.sb.identity.labels, "swfactory.external": "true"}) == []
        rows = ctx.state.read_jsonl("external-processes.jsonl")
        assert [row["event"] for row in rows] == ["intent", "created", "start_intent", "cleaned"]
        assert all(receipt["status"] == "converged" for receipt in ctx.sb.receipts)
        with ctx.state.exclusive("blocked-plan"), pytest.raises(StageError, match="budget exhausted"):
            _agent(ctx, "plan", 1, "cannot spend a held grant", None)
        assert len(ledger.records()) == 1

        key = hashlib.sha256(record.call_id.encode()).hexdigest()
        descriptor = json.loads(ctx.state.read_control(f"external-calls/{key}.json"))
        evidence = tmp_path / "operator-observation.json"
        evidence.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": ctx.cfg.run_id,
                    "call_id": record.call_id,
                    "accepted_inputs_digest": descriptor["accepted_inputs_digest"],
                    "profile_binding_sha256": descriptor["profile_binding_sha256"],
                    "provider": "synthetic-final-bill",
                    "account_id": "provider-free-fixture",
                    "request_id": "first-fixture-invocation",
                    "evidence_reference": "operator-fixture-final-1",
                    "observed_at": datetime.now(UTC).isoformat(),
                    "status": "committed",
                    "final": True,
                    "cost_usd": 0.37,
                }
            )
            + "\n"
        )
        reconciled = runner.invoke(
            app,
            [
                "state",
                "reconcile-call",
                ctx.cfg.run_id,
                record.call_id,
                "--root",
                root,
                "--observation",
                str(evidence),
                "--actor",
                "operator:live-fixture",
                "--reason",
                "verified synthetic final aggregate after real Docker teardown",
            ],
        )
        assert reconciled.exit_code == 0, reconciled.output
        assert json.loads(reconciled.output)["settled_usd"] == 0.37
        recovered = calls()
        assert recovered["charged_usd"] == 0.37 and recovered["unreconciled_usd"] == 0
        imported = ledger.record(record.call_id).observation["evidence"]
        assert imported["actor"] == "operator:live-fixture" and imported["authority"] == "operator_attestation"

        with ctx.state.exclusive("resumed-plan"):
            second = _agent(ctx, "plan", 1, "resume with the remaining grant", None)
        assert json.loads(second.text)["granted_usd"] == pytest.approx(1.63)
        assert ledger.records()[-1].reserved_usd == pytest.approx(1.63)
        assert ledger.charged_usd() == pytest.approx(2) and ledger.unreconciled_usd() == pytest.approx(1.63)
        assert DockerContainers().containers({**ctx.sb.identity.labels, "swfactory.external": "true"}) == []
        assert sum(row["event"] == "cleaned" for row in ctx.state.read_jsonl("external-processes.jsonl")) == 2
    finally:
        ctx.sb.close()
