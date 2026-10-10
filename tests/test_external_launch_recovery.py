"""Trusted no-start observations free money; ambiguous launch attempts never do."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from swfactory import sandbox as sandbox_mod
from swfactory.call_accounting import CallLedger
from swfactory.config import Config, TargetContract
from swfactory.external_agent import ExternalAgent
from swfactory.models import Issue, RunResult, StageError
from swfactory.sandbox import DockerSandbox
from swfactory.sandbox_governance import SandboxIdentity
from swfactory.stages import Ctx, _agent, seed_budget

CID = "b" * 64
OTHER_CID = "c" * 64
DAEMON_REJECTION = RunResult(1, "", "Error response from daemon: invalid network configuration\n", 0.01)


def context(tmp_path: Path, *, max_budget_usd: float = 4.0) -> Ctx:
    checkout = tmp_path / "checkout"
    (checkout / "src").mkdir(parents=True, exist_ok=True)
    (checkout / "tests").mkdir(exist_ok=True)
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "id": "qualified",
                "version": "1",
                "argv": ["/opt/operator-wrapper"],
                "model": "fixture",
                "credential_env": [],
                "budget_mode": "hard_cap",
                "enforces_usd_limit": True,
            }
        )
    )
    cfg = Config(
        issue="x",
        agent="external",
        agent_profile=str(profile),
        sandbox="docker",
        workdir=str(checkout),
        docker_image="example/agent@sha256:" + "a" * 64,
        max_budget_usd=max_budget_usd,
        max_budget_usd_per_stage=2.0,
    )
    ctx = Ctx(
        cfg=cfg,
        sb=None,  # type: ignore[arg-type]
        agent=ExternalAgent(cfg),
        scm=None,  # type: ignore[arg-type]
        issue=Issue(id="X-1", title="test", body="test"),
        run_dir=tmp_path / "run",
        contract=TargetContract(test="true"),
    )
    ctx.sb = DockerSandbox(
        checkout,
        image=cfg.docker_image,
        identity=SandboxIdentity("docker", "cell", 1, "run"),
        state=ctx.state,
        network="none",
    )
    return ctx


class Fleet:
    """A daemon mock exposing foreign rows so the launcher must prove ownership."""

    def __init__(
        self,
        monkeypatch,
        *,
        create=None,
        start=None,
        actual_cid=CID,
        foreign=False,
        register_failure=False,
        running=False,
    ):
        self.create = create
        self.start = start
        self.actual_cid = actual_cid
        self.foreign = foreign
        self.register_failure = register_failure
        self.running = running
        self.rows = []
        self.removed = []
        self.commands = []
        self.request = None
        self.fail_remove = False
        monkeypatch.setattr(sandbox_mod, "DockerContainers", lambda: self)
        monkeypatch.setattr(sandbox_mod, "_run_external_subprocess", self.launch)

    def containers(self, labels=None):
        return list(self.rows)

    def remove(self, container_id):
        if self.fail_remove:
            raise RuntimeError("daemon removal did not complete")
        self.removed.append(container_id)
        self.rows = [row for row in self.rows if row["id"] != container_id]

    def launch(self, argv, **kwargs):
        command = list(argv)
        self.commands.append(command)
        if command[1] == "create":
            if isinstance(self.create, BaseException):
                raise self.create
            if self.create is not None and not self.register_failure:
                return self.create
            labels = dict(
                value.split("=", 1) for flag, value in zip(command, command[1:], strict=False) if flag == "--label"
            )
            if self.foreign:
                labels["swfactory.epoch"] = "2"
            self.rows.append(
                {
                    "id": self.actual_cid,
                    "name": command[command.index("--name") + 1],
                    "labels": labels,
                    "running": self.running,
                }
            )
            if self.create is not None:
                return self.create
            request_mount = next(
                value
                for flag, value in zip(command, command[1:], strict=False)
                if flag == "-v" and value.endswith(f":{sandbox_mod.EXTERNAL_SCRATCH}/request.json:ro")
            )
            self.request = json.loads(Path(request_mount.split(":", 1)[0]).read_text())
            return RunResult(0, CID + "\n", "", 0.01)
        assert command[1] == "start", "the external launcher must separate create and start"
        assert "--attach" in command and command[-1] == CID
        for row in self.rows:
            if row["id"] == CID:
                row["running"] = True
        if isinstance(self.start, BaseException):
            raise self.start
        if self.start is not None:
            return self.start
        assert self.request is not None
        candidate = {
            "schema_version": 1,
            "call_id": self.request["call_id"],
            "profile_id": self.request["profile_id"],
            "status": "success",
            "text": "candidate",
            "data": None,
            "num_turns": 1,
            "session_id": "session",
        }
        return RunResult(0, json.dumps(candidate) + "\n", "", 0.01)


def test_definite_create_refusal_releases_only_that_attempt_and_allows_retry(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch, create=DAEMON_REJECTION)
    with pytest.raises(StageError):
        _agent(ctx, "spec", 1, "prompt", None)
    ledger = CallLedger(ctx.state)
    (refused,) = ledger.records()
    assert refused.settled_usd == 0 and ledger.charged_usd() == 0
    assert refused.observation["status"] == "definitely_absent"
    assert ctx.sb.external_no_start_observation(refused.call_id).status == "definitely_absent"
    assert [command[1] for command in fleet.commands] == ["create"]
    assert "--pull=never" in fleet.commands[0]

    fleet.create = None
    assert _agent(ctx, "spec", 1, "retry", None).text == "candidate"
    retried = ledger.records()[-1]
    assert retried.call_id != refused.call_id and retried.settled_usd is None
    assert ledger.charged_usd() == ledger.unreconciled_usd() == 2
    assert [command[1] for command in fleet.commands] == ["create", "create", "start"]
    assert fleet.removed == [CID]


def test_durable_no_start_proof_recovers_before_an_exhausted_budget_guard(tmp_path, monkeypatch):
    ctx = context(tmp_path, max_budget_usd=2.0)
    fleet = Fleet(monkeypatch, create=DAEMON_REJECTION)
    reconcile = CallLedger.reconcile

    def interrupted_reconcile(self, call_id, outcome):
        raise SystemExit("worker died after the no-start proof was durable")

    monkeypatch.setattr(CallLedger, "reconcile", interrupted_reconcile)
    with pytest.raises(SystemExit):
        _agent(ctx, "spec", 1, "prompt", None)
    (prior,) = CallLedger(ctx.state).records()
    assert CallLedger(ctx.state).unreconciled_usd() == 2
    assert ctx.sb.external_no_start_observation(prior.call_id).status == "definitely_absent"

    monkeypatch.setattr(CallLedger, "reconcile", reconcile)
    restarted = context(tmp_path, max_budget_usd=2.0)
    fleet.create = None
    assert _agent(restarted, "spec", 1, "retry", None).text == "candidate"
    ledger = CallLedger(restarted.state)
    assert ledger.record(prior.call_id).settled_usd == 0
    assert ledger.charged_usd() == ledger.unreconciled_usd() == 2
    assert len(ledger.records()) == 2


def test_no_start_recovery_releases_a_reservation_cached_in_the_same_context(tmp_path, monkeypatch):
    ctx = context(tmp_path, max_budget_usd=2.0)
    fleet = Fleet(monkeypatch, create=DAEMON_REJECTION)
    reconcile = CallLedger.reconcile

    def interrupted_reconcile(self, call_id, outcome):
        raise SystemExit("reconciliation interrupted after the trusted proof was written")

    monkeypatch.setattr(CallLedger, "reconcile", interrupted_reconcile)
    with pytest.raises(SystemExit):
        _agent(ctx, "spec", 1, "prompt", None)
    assert seed_budget(ctx, refresh=True) == ctx.spent_usd == 2
    (prior,) = CallLedger(ctx.state).records()

    monkeypatch.setattr(CallLedger, "reconcile", reconcile)
    fleet.create = None
    assert _agent(ctx, "spec", 1, "retry", None).text == "candidate"
    ledger = CallLedger(ctx.state)
    assert ledger.record(prior.call_id).settled_usd == 0
    assert ledger.charged_usd() == ledger.unreconciled_usd() == 2
    assert len(ledger.records()) == 2
    assert ctx.spent_usd == 0, "the resolved reservation cannot remain cached as observed spend"


@pytest.mark.parametrize(
    "create",
    [
        RunResult(124, "", "Error response from daemon: timeout", 5, True),
        RunResult(-15, "", "Error response from daemon: interrupted", 0.01),
        RunResult(1, "", "Cannot connect to the Docker daemon", 0.01),
        RunResult(1, "unexpected output", "Error response from daemon: refused", 0.01),
        OSError("the create client disappeared"),
    ],
    ids=["timeout", "signal", "transport", "output", "exception"],
)
def test_ambiguous_create_keeps_budget_and_blocks_a_second_launch(tmp_path, monkeypatch, create):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch, create=create)
    with pytest.raises((StageError, OSError)):
        _agent(ctx, "spec", 1, "prompt", None)
    ledger = CallLedger(ctx.state)
    (attempt,) = ledger.records()
    assert ledger.charged_usd() == ledger.unreconciled_usd() == 2
    assert ctx.sb.external_no_start_observation(attempt.call_id) is None
    with pytest.raises(StageError):
        _agent(ctx, "spec", 1, "retry", None)
    assert len(fleet.commands) == 1 and not fleet.removed
    assert len(ledger.records()) == 1


@pytest.mark.parametrize(
    "start",
    [RunResult(1, "", "Error response from daemon: start refused", 0.01), OSError("start outcome unknown")],
    ids=["daemon-error", "interrupted"],
)
def test_attempted_start_never_supplies_a_no_start_billing_proof(tmp_path, monkeypatch, start):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch, start=start)
    with pytest.raises((StageError, OSError)):
        _agent(ctx, "spec", 1, "prompt", None)
    ledger = CallLedger(ctx.state)
    (attempt,) = ledger.records()
    assert ledger.charged_usd() == ledger.unreconciled_usd() == 2
    assert ctx.sb.external_no_start_observation(attempt.call_id) is None
    assert fleet.removed == [CID]
    with pytest.raises(StageError, match="observe and reconcile"):
        _agent(ctx, "spec", 1, "retry", None)
    assert len(fleet.commands) == 2


@pytest.mark.parametrize("foreign", [False, True], ids=["cid-mismatch", "foreign-epoch"])
def test_unverified_created_identity_is_never_started_or_released(tmp_path, monkeypatch, foreign):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch, actual_cid=CID if foreign else OTHER_CID, foreign=foreign)
    with pytest.raises(StageError):
        _agent(ctx, "spec", 1, "prompt", None)
    ledger = CallLedger(ctx.state)
    (attempt,) = ledger.records()
    assert ledger.unreconciled_usd() == 2
    assert ctx.sb.external_no_start_observation(attempt.call_id) is None
    assert [command[1] for command in fleet.commands] == ["create"]
    assert not fleet.removed, "a different resource must not be removed as the returned CID"


@pytest.mark.parametrize("phase", ["created", "start_intent"])
def test_recovery_cannot_remove_a_substituted_container_with_the_same_owned_name(tmp_path, monkeypatch, phase):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch)
    ledger = CallLedger(ctx.state)
    attempt = ledger.reserve(stage="spec", iteration=1, reserved_usd=2.0)
    ledger.mark_unknown(attempt, "worker died during external launch")
    labels = {
        **ctx.sb.identity.labels,
        "swfactory.external": "true",
        "swfactory.external-call": hashlib.sha256(attempt.call_id.encode()).hexdigest()[:16],
    }
    identity = {"call_id": attempt.call_id, "container": "interrupted-external", "labels": labels}
    ctx.state.append_json("external-processes.jsonl", {**identity, "event": "intent"})
    ctx.state.append_json("external-processes.jsonl", {**identity, "event": "created", "container_id": CID})
    if phase == "start_intent":
        ctx.state.append_json("external-processes.jsonl", {**identity, "event": phase, "container_id": CID})
    substituted = {"id": OTHER_CID, "name": identity["container"], "labels": labels, "running": True}
    fleet.rows.append(substituted)

    with pytest.raises(StageError):
        _agent(ctx, "spec", 1, "retry", None)
    assert not fleet.commands and not fleet.removed
    assert fleet.rows == [substituted]
    assert ledger.charged_usd() == ledger.unreconciled_usd() == 2
    assert len(ledger.records()) == 1
    assert ctx.sb.external_no_start_observation(attempt.call_id) is None


def test_cleanup_failure_cannot_write_no_start_proof(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch, create=DAEMON_REJECTION, register_failure=True)
    fleet.fail_remove = True
    with pytest.raises(StageError, match="cleanup|removal"):
        _agent(ctx, "spec", 1, "prompt", None)
    ledger = CallLedger(ctx.state)
    (attempt,) = ledger.records()
    assert ledger.unreconciled_usd() == 2
    assert ctx.sb.external_no_start_observation(attempt.call_id) is None
    assert not fleet.removed


def test_definite_refusal_removes_an_owned_stopped_container_before_releasing(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch, create=DAEMON_REJECTION, register_failure=True)
    with pytest.raises(StageError):
        _agent(ctx, "spec", 1, "prompt", None)
    ledger = CallLedger(ctx.state)
    (attempt,) = ledger.records()
    assert ledger.charged_usd() == 0 and attempt.settled_usd == 0
    assert fleet.removed == [CID] and not fleet.rows
    assert [command[1] for command in fleet.commands] == ["create"]
    assert ctx.sb.external_no_start_observation(attempt.call_id).status == "definitely_absent"


def test_running_container_contradicts_a_create_refusal_and_keeps_money_held(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    fleet = Fleet(monkeypatch, create=DAEMON_REJECTION, register_failure=True, running=True)
    with pytest.raises(StageError):
        _agent(ctx, "spec", 1, "prompt", None)
    ledger = CallLedger(ctx.state)
    (attempt,) = ledger.records()
    assert ledger.charged_usd() == ledger.unreconciled_usd() == 2
    assert ctx.sb.external_no_start_observation(attempt.call_id) is None
    assert [command[1] for command in fleet.commands] == ["create"]


@pytest.mark.parametrize(
    "corruption", ["call_id", "labels", "container", "phase", "exit_code", "missing_cleanup", "start_intent"]
)
def test_no_start_observation_rejects_mismatched_or_nonterminal_evidence(tmp_path, monkeypatch, corruption):
    ctx = context(tmp_path)
    Fleet(monkeypatch, create=DAEMON_REJECTION)
    with pytest.raises(StageError):
        _agent(ctx, "spec", 1, "prompt", None)
    (attempt,) = CallLedger(ctx.state).records()
    proof_path = "external-not-started/" + hashlib.sha256(attempt.call_id.encode()).hexdigest() + ".json"
    proof = json.loads(ctx.state.read_control(proof_path))
    if corruption == "call_id":
        proof["call_id"] = "a-different-call"
    elif corruption == "labels":
        proof["labels"]["swfactory.epoch"] = "2"
    elif corruption == "container":
        proof["container"] = "a-different-container"
    elif corruption == "phase":
        proof["phase"] = "start"
    elif corruption == "exit_code":
        proof["exit_code"] = True
    elif corruption == "missing_cleanup":
        records = ctx.state.read_jsonl("external-processes.jsonl")
        ctx.state.write_control(
            "external-processes.jsonl", "".join(json.dumps(row) + "\n" for row in records if row["event"] != "cleaned")
        )
    else:
        ctx.state.append_json("external-processes.jsonl", {**proof, "event": "start_intent"})
    ctx.state.write_control(proof_path, json.dumps(proof))
    with pytest.raises(StageError, match="observation is invalid"):
        ctx.sb.external_no_start_observation(attempt.call_id)
