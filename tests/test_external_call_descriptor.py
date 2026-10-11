"""A host call binding must survive the launcher before any candidate receipt exists."""

from __future__ import annotations

import hashlib
import json
import os
import signal
from datetime import datetime
from pathlib import Path

import pytest

from swfactory import accepted_inputs
from swfactory.call_accounting import CallLedger
from swfactory.config import Config, TargetContract
from swfactory.external_agent import ExternalAgent
from swfactory.models import AgentResult, Issue, StageError
from swfactory.sandbox import DockerSandbox, LocalSandbox
from swfactory.sandbox_governance import SandboxIdentity
from swfactory.stages import Ctx, _agent
from swfactory.state import RunState

EXECUTABLE = Path(__file__).parent / "fixtures" / "external_agent" / "fixture.py"


def descriptor_path(call_id: str) -> str:
    return "external-calls/" + hashlib.sha256(call_id.encode()).hexdigest() + ".json"


def context(tmp_path: Path, *, admit: bool = True, docker: bool = False) -> Ctx:
    checkout = tmp_path / "checkout"
    (checkout / "src").mkdir(parents=True, exist_ok=True)
    (checkout / "tests").mkdir(exist_ok=True)
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "id": "operator-installed",
                "version": "1",
                "argv": [str(EXECUTABLE.resolve())],
                "model": "fixture",
                "credential_env": [],
                "budget_mode": "hard_cap",
                "enforces_usd_limit": True,
            }
        )
    )
    cfg = Config(
        issue="x",
        run_id="callbinding1",
        agent="external",
        agent_profile=str(profile),
        sandbox="docker" if docker else "local",
        docker_image="example/agent@sha256:" + "a" * 64,
        allow_local_agent=True,
        workdir=str(checkout),
    )
    issue = Issue(id="X-1", title="test", body="test")
    ctx = Ctx(
        cfg=cfg,
        sb=LocalSandbox(checkout),
        agent=ExternalAgent(cfg),
        scm=None,  # type: ignore[arg-type]
        issue=issue,
        run_dir=tmp_path / "run",
        contract=TargetContract(test="true"),
    )
    if docker:
        ctx.sb = DockerSandbox(
            checkout,
            image=cfg.docker_image,
            identity=SandboxIdentity("docker", "cell", 1, "run"),
            state=ctx.state,
        )
    if admit:
        accepted_inputs.admit(ctx.state, accepted_inputs.snapshot(cfg, None, issue))
    return ctx


@pytest.mark.parametrize("admit", [False, True], ids=["unadmitted-unit-call", "admitted-epoch"])
def test_descriptor_is_persisted_before_the_launcher_and_is_not_candidate_writable(tmp_path, monkeypatch, admit):
    ctx = context(tmp_path, admit=admit)
    observed = {}

    def launch(self, sb, *, invocation, **kwargs):
        persisted = RunState(ctx.run_dir).read_control(descriptor_path(invocation.call_id))
        observed["content"] = persisted
        descriptor = json.loads(persisted)
        assert descriptor["call_id"] == invocation.call_id
        assert descriptor["accepted_inputs_digest"] == invocation.accepted_inputs_digest
        assert CallLedger(ctx.state).record(invocation.call_id).reserved_usd == descriptor["reserved_usd"]
        return AgentResult(
            agent="external",
            text="untrusted candidate text",
            cost_usd=None,
            raw_envelope={"status": "success", "data": {"authority": "candidate", "call_id": "forged"}},
        )

    monkeypatch.setattr(ExternalAgent, "run", launch)
    _agent(ctx, "spec", 1, "sensitive prompt that must not enter the descriptor", None)
    (call,) = CallLedger(ctx.state).records()
    raw = ctx.state.read_control(descriptor_path(call.call_id))
    assert raw == observed["content"], "the candidate result cannot overwrite its factory binding"
    descriptor = json.loads(raw)
    assert set(descriptor) == {
        "schema_version",
        "authority",
        "run_id",
        "call_id",
        "accepted_inputs_digest",
        "profile",
        "profile_binding_sha256",
        "stage",
        "iteration",
        "reserved_usd",
        "sandbox",
        "workdir",
        "sandbox_labels",
        "created_at",
    }
    assert descriptor["schema_version"] == 1 and descriptor["authority"] == "factory"
    assert descriptor["run_id"] == ctx.cfg.run_id
    assert (descriptor["stage"], descriptor["iteration"], descriptor["reserved_usd"]) == ("spec", 1, 2.0)
    assert descriptor["accepted_inputs_digest"] == accepted_inputs.digest_of(ctx.state)
    assert descriptor["profile"] == ctx.agent.binding
    canonical = json.dumps(
        ctx.agent.binding, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    assert descriptor["profile_binding_sha256"] == hashlib.sha256(canonical.encode()).hexdigest()
    assert descriptor["sandbox"] == ctx.sb.name and descriptor["workdir"] == str(Path(ctx.sb.workdir).resolve())
    assert descriptor["sandbox_labels"] == {}
    assert datetime.fromisoformat(descriptor["created_at"]).utcoffset().total_seconds() == 0
    assert "sensitive prompt" not in raw and "untrusted candidate text" not in raw and "forged" not in raw
    assert not (Path(ctx.sb.workdir) / descriptor_path(call.call_id)).exists()
    assert not (ctx.state.artifacts / descriptor_path(call.call_id)).exists()
    result_receipt = "external-agent/" + hashlib.sha256(call.call_id.encode()).hexdigest() + ".json"
    assert ctx.state.has_control(result_receipt), "the existing result receipt is still produced"


def test_descriptor_records_docker_ownership_before_a_throwing_launcher(tmp_path, monkeypatch):
    ctx = context(tmp_path, docker=True)
    monkeypatch.setattr(ctx.sb, "preflight_external", lambda *args, **kwargs: [])

    def launch(self, sb, *, invocation, **kwargs):
        descriptor = json.loads(ctx.state.read_control(descriptor_path(invocation.call_id)))
        assert descriptor["sandbox_labels"] == dict(sb.identity.labels)
        raise StageError("sandbox", "launcher interrupted")

    monkeypatch.setattr(ExternalAgent, "run", launch)
    with pytest.raises(StageError, match="launcher interrupted"):
        _agent(ctx, "spec", 1, "prompt", None)
    (call,) = CallLedger(ctx.state).records()
    assert call.unreconciled and call.charge == 2
    assert ctx.state.has_control(descriptor_path(call.call_id))
    assert not ctx.state.has_control("external-agent/" + hashlib.sha256(call.call_id.encode()).hexdigest() + ".json")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="the durability regression requires a POSIX worker")
def test_descriptor_survives_sigkill_inside_the_launcher(tmp_path, monkeypatch):
    ctx = context(tmp_path)

    def launch(self, sb, *, invocation, **kwargs):
        assert ctx.state.has_control(descriptor_path(invocation.call_id))
        os.kill(os.getpid(), signal.SIGKILL)
        raise AssertionError("unreachable")

    monkeypatch.setattr(ExternalAgent, "run", launch)
    child = os.fork()
    if child == 0:  # pragma: no cover - killed before returning to pytest
        _agent(ctx, "spec", 1, "prompt", None)
        os._exit(97)
    _, status = os.waitpid(child, 0)
    assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
    state = RunState(ctx.run_dir)
    (call,) = CallLedger(state).records()
    descriptor = json.loads(state.read_control(descriptor_path(call.call_id)))
    assert descriptor["call_id"] == call.call_id and descriptor["profile"] == ctx.agent.binding
    assert descriptor["accepted_inputs_digest"] == accepted_inputs.digest_of(state)
    assert call.unreconciled and call.charge == 2


def test_a_conflicting_descriptor_refuses_the_launcher_without_overwriting(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    reserve = CallLedger.reserve
    sentinel = '{"authority":"an existing descriptor"}\n'
    created = {}

    def reserve_with_conflict(self, **kwargs):
        attempt = reserve(self, **kwargs)
        path = descriptor_path(attempt.call_id)
        ctx.state.write_control(path, sentinel)
        created["path"] = path
        return attempt

    monkeypatch.setattr(CallLedger, "reserve", reserve_with_conflict)
    monkeypatch.setattr(ExternalAgent, "run", lambda *args, **kwargs: pytest.fail("conflicting call launched"))
    with pytest.raises(StageError, match="descriptor is immutable"):
        _agent(ctx, "spec", 1, "prompt", None)
    assert ctx.state.read_control(created["path"]) == sentinel
    assert CallLedger(ctx.state).unreconciled_usd() == 2


def test_native_calls_do_not_write_external_descriptors(tmp_path):
    ctx = context(tmp_path, admit=False)
    ctx.cfg = ctx.cfg.model_copy(update={"agent": "scripted", "agent_profile": None})

    class Native:
        kind = "scripted"

        def run(self, sb, **kwargs):
            return AgentResult(agent="scripted", text="native", cost_usd=0.25)

    ctx.agent = Native()  # type: ignore[assignment]
    assert _agent(ctx, "spec", 1, "prompt", None).text == "native"
    assert CallLedger(ctx.state).charged_usd() == 0.25
    assert not (ctx.state.root / "external-calls").exists()
