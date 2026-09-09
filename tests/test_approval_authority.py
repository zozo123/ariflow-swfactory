"""Required human gates refuse automatic overrides and missing responses (issue #2066).

The hole this pins shut had three mouths, and a gate closed at only one of them is closed at none:

* ``dags/blueprints.py`` OR'd every blueprint gate flag with a global ``SWF_APPROVE=auto``, so a
  gate a blueprint declared ``mode = "human"`` was constructed with ``defaults=["Approve"]``;
* the same file read a missing or empty approval XCom as an approval by actor ``auto``, so marking
  the HITL task successful (what ``dag.test()`` does) authorized the rest of the line;
* nothing bound an answer to the Cell epoch or the artifact it was given for, so a recorded
  decision survived into a different epoch or over a rewritten artifact.

Everything here is hermetic: a LocalSandbox under ``tmp_path`` and, for the DAG-construction half,
a synthetic blueprint next to a copy of the generator.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from swfactory import accepted_inputs, stages
from swfactory.approval_policy import (
    approval_from_response,
    check_recorded,
    declared_mode,
    replay_approval,
)
from swfactory.models import Approval, StageError
from swfactory.stages import Gate

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "dags"
CELL_ID = "cell_" + "a" * 24

# One human gate and one deliberately automatic gate in the same file: the env-override test needs
# both, because "no gate defaults" would also pass if the generator had simply stopped defaulting.
MIXED_TOML = """
[blueprint]
name = "mixed"
[[targets]]
repo = "acme/app"
[stages]
order = ["intent", "plan", "build_and_test", "review", "deliver"]
[[gates]]
after = "intent"
artifact = "intent.md"
timeout_h = 2
mode = "human"
[[gates]]
after = "plan"
artifact = "plan.md"
timeout_h = 2
mode = "auto"
[sandbox]
ttl_s = 172800
"""


def _ctx(tmp_path: Path, *, gate_replay: str | None = None):
    """A real ``Ctx`` over a LocalSandbox so recording and validation touch real files."""
    from swfactory import accepted_inputs
    from swfactory.config import Config
    from swfactory.models import Issue
    from swfactory.sandbox import LocalSandbox

    cfg = Config(issue="demo/issue.md", run_id="abcd1234", gate_replay=gate_replay)
    issue = Issue(id="DEMO-1", title="t", body="")
    ctx = stages.Ctx(
        cfg=cfg,
        sb=LocalSandbox(tmp_path / "work"),
        agent=SimpleNamespace(kind="scripted"),
        scm=SimpleNamespace(kind="local"),
        issue=issue,
        run_dir=tmp_path / "run",
    )
    # Pin the run the way `_prepare_ctx` does on the first context. Recording and publication read
    # the accepted inputs fail-closed, so a Ctx built straight from the constructor -- which no
    # production path does -- would otherwise be testing an unpinned run that cannot exist.
    accepted_inputs.admit(ctx.state, accepted_inputs.snapshot(cfg, None, issue))
    return ctx


def _with_artifacts(ctx, text: str = "# intent\n"):
    ctx.write_artifact(f"{ctx.art}/intent.md", text)
    ctx.write_artifact(f"{ctx.art}/plan.md", text)
    return ctx


def _bind_cell(ctx, *, epoch: int = 3, managed: bool = True) -> None:
    ctx.state.write_control(
        "cell.json",
        json.dumps({"schema_version": 1, "cell_id": CELL_ID, "epoch": epoch, "managed": managed}),
    )


def _fixture(tmp_path: Path, actor: str = "smoke", decision: str = "approve") -> str:
    path = tmp_path / "gate-replay.json"
    path.write_text(
        json.dumps({g: {"decision": decision, "actor": actor} for g in ("intent", "plan")}),
        encoding="utf-8",
    )
    return str(path)


def _human(actor: str = "alice", responded_at: str = "2099-01-01T00:00:00Z") -> dict:
    """A HITL response shaped like the provider's own event, which carries `responded_at`.

    The timestamp is what tells a fresh answer from an old XCom read twice after a re-accept, so a
    fixture that omitted it would be testing a shape the operator never produces.
    """
    return {
        "chosen_options": ["Approve"],
        "params_input": {},
        "responded_by_user": {"id": "u1", "name": actor},
        "responded_at": responded_at,
    }


# ------------------------------------------------- SWF_APPROVE cannot override a declared gate


@pytest.fixture(scope="module")
def mixed_dag(tmp_path_factory: pytest.TempPathFactory):
    """The generator run with ``SWF_APPROVE=auto`` set, exactly as the issue's probe found it."""
    import os

    pytest.importorskip("airflow")
    from airflow.dag_processing.dagbag import DagBag

    root = tmp_path_factory.mktemp("mixed")
    (root / "dags").mkdir()
    (root / "blueprints").mkdir()
    shutil.copy(DAGS / "blueprints.py", root / "dags" / "blueprints.py")
    (root / "blueprints" / "mixed.toml").write_text(MIXED_TOML, encoding="utf-8")
    os.environ["AIRFLOW_HOME"] = str(root / "airflow_home")
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["SWF_APPROVE"] = "auto"  # the global knob the DAG used to OR into every gate
    try:
        bag = DagBag(dag_folder=str(root / "dags"))
        assert not bag.import_errors, bag.import_errors
        return bag.dags["mixed"]
    finally:
        os.environ.pop("SWF_APPROVE", None)


def test_env_approve_auto_cannot_default_a_declared_human_gate(mixed_dag) -> None:
    human = mixed_dag.get_task("job.approve_intent")
    auto = mixed_dag.get_task("job.approve_plan")
    # No defaults => the operator waits for a person and TIMES OUT (its declared terminal
    # behaviour) instead of answering itself; the auto gate keeps its declared default.
    assert human.defaults is None
    assert auto.defaults == ["Approve"]
    assert human.response_timeout == auto.response_timeout


def test_cli_approver_refuses_approve_auto_on_a_human_gate(tmp_path: Path) -> None:
    """Local replay holds the same line as the DAG: configuration is not an approver."""
    from swfactory.config import Config

    ctx = _with_artifacts(_ctx(tmp_path))
    ctx.cfg = Config(issue="x", approve="auto")
    with pytest.raises(StageError, match="declared human"):
        stages.cli_approver(Gate("intent", "intent.md"), ctx)
    # A gate that declares itself automatic still answers itself, in its declared mode.
    approval = stages.cli_approver(Gate("plan", "plan.md", "auto"), ctx)
    assert (approval.decision, approval.actor, approval.mode) == ("approve", "auto", "auto")


# ------------------------------------------------- missing / empty / malformed responses


@pytest.mark.parametrize("response", [None, {}, "", [], 0])
def test_a_gate_nobody_answered_is_not_approved(response) -> None:
    with pytest.raises(StageError, match="no recorded response"):
        approval_from_response(gate="intent", gate_mode="human", response=response)
    # ... and the same is true of a gate the blueprint declares automatic: the operator's default
    # is a real recorded answer, so its absence still means nobody answered.
    with pytest.raises(StageError, match="no recorded response"):
        approval_from_response(gate="intent", gate_mode="auto", response=response)


@pytest.mark.parametrize(
    "response",
    [
        "Approve",
        {"chosen_options": "Approve"},
        {"chosen_options": []},
        {"chosen_options": ["Approve", "Reject"]},
        {"chosen_options": ["Maybe"]},
        {"params_input": {}},
    ],
)
def test_malformed_responses_are_refused(response) -> None:
    with pytest.raises(StageError):
        approval_from_response(gate="intent", gate_mode="human", response=response)


def test_an_anonymous_answer_satisfies_only_a_declared_auto_gate() -> None:
    anonymous = {"chosen_options": ["Approve"], "responded_by_user": None}
    with pytest.raises(StageError, match="declared human"):
        approval_from_response(gate="intent", gate_mode="human", response=anonymous)
    approval = approval_from_response(gate="intent", gate_mode="auto", response=anonymous)
    assert (approval.actor, approval.mode) == ("auto", "auto")


@pytest.mark.parametrize("actor", ["auto", "replay:smoke"])
def test_a_response_cannot_claim_a_runtime_reserved_actor(actor: str) -> None:
    with pytest.raises(StageError, match="reserved actor"):
        approval_from_response(gate="intent", gate_mode="human", response=_human(actor))


# ------------------------------------------------- identity, epoch and artifact binding


def test_a_valid_human_answer_records_identity_and_evidence(tmp_path: Path) -> None:
    ctx = _with_artifacts(_ctx(tmp_path))
    _bind_cell(ctx, epoch=3)
    approval = approval_from_response(gate="intent", gate_mode="human", response=_human())
    stages.record_approval(ctx, approval)
    (recorded,) = json.loads(ctx.read_artifact(f"{ctx.art}/approvals.json"))
    assert recorded["actor"] == "alice" and recorded["mode"] == "human"
    assert recorded["cell_id"] == CELL_ID and recorded["cell_epoch"] == 3
    assert recorded["artifact_sha256"] == hashlib.sha256(b"# intent\n").hexdigest()


def test_recording_refuses_an_answer_bound_to_another_cell_epoch(tmp_path: Path) -> None:
    ctx = _with_artifacts(_ctx(tmp_path))
    _bind_cell(ctx, epoch=3)
    stale = Approval.model_validate(
        {**approval_from_response(gate="intent", gate_mode="human", response=_human()).model_dump(), "cell_epoch": 2}
    )
    with pytest.raises(StageError, match="bound to Cell"):
        stages.record_approval(ctx, stale)


def test_continuation_refuses_a_wrong_epoch_or_stale_artifact(tmp_path: Path) -> None:
    ctx = _with_artifacts(_ctx(tmp_path))
    _bind_cell(ctx, epoch=3)
    stages.record_approval(ctx, approval_from_response(gate="intent", gate_mode="human", response=_human()))
    gates = [Gate("intent", "intent.md")]
    approvals = stages._load_approvals(ctx)

    ctx.blueprint = None
    # The recorded chain is re-checked at continuation, not trusted because it exists on disk.
    _bind_cell(ctx, epoch=4)  # the Cell was fenced and re-issued under a new epoch
    with pytest.raises(StageError, match="bound to Cell"):
        for gate in gates:
            check_recorded(
                gate=gate.name,
                gate_mode=gate.mode,
                approval=approvals[0],
                managed=True,
                cell_id=CELL_ID,
                cell_epoch=4,
                # The run's real digest: this case is about the Cell binding, so the inputs check
                # must not be the thing that fires. Passing None would test the wrong refusal.
                inputs_digest=accepted_inputs.digest_of(ctx.state),
            )
    _bind_cell(ctx, epoch=3)
    ctx.write_artifact(f"{ctx.art}/intent.md", "# intent, rewritten after approval\n")
    with pytest.raises(StageError, match="does not match its artifact"):
        stages._validate_approvals(ctx, approvals)


def test_continuation_refuses_a_chain_with_no_decision_at_all(tmp_path: Path) -> None:
    """Marking ``record_<stage>`` successful too leaves an empty chain; publication still refuses,
    which is why the gate is enforced at continuation and not only where it is answered."""
    ctx = _with_artifacts(_ctx(tmp_path))
    with pytest.raises(StageError, match="missing decision for required gate"):
        stages._validate_approvals(ctx, [])


def test_continuation_refuses_an_automatic_decision_on_a_human_gate() -> None:
    automatic = approval_from_response(
        gate="intent", gate_mode="auto", response={"chosen_options": ["Approve"], "responded_by_user": None}
    )
    with pytest.raises(StageError, match="declared human"):
        check_recorded(
            gate="intent",
            gate_mode="human",
            approval=automatic,
            managed=False,
            cell_id=None,
            cell_epoch=None,
            inputs_digest=None,
        )


def test_rejection_keeps_its_terminal_behaviour(tmp_path: Path) -> None:
    """A refusal still records and still stops the line; only approval got stricter."""
    ctx = _with_artifacts(_ctx(tmp_path))
    rejection = approval_from_response(
        gate="intent",
        gate_mode="human",
        response={
            "chosen_options": ["Reject"],
            "responded_by_user": {"name": "alice"},
            "responded_at": "2099-01-01T00:00:00Z",
        },
    )
    stages.record_approval(ctx, rejection)
    rejected, gate = stages._validate_approvals(ctx, stages._load_approvals(ctx))
    assert (rejected, gate) == (True, "intent")


# ------------------------------------------------- the replay fixture, and its boundary


def test_replay_fixture_answers_an_unmanaged_gate_and_is_never_mistaken_for_a_person(tmp_path: Path) -> None:
    approval = approval_from_response(
        gate="intent", gate_mode="human", response=None, fixture_path=_fixture(tmp_path), managed=False
    )
    assert (approval.decision, approval.actor, approval.mode) == ("approve", "replay:smoke", "replay")


def test_replay_fixture_cannot_authorize_managed_work(tmp_path: Path) -> None:
    """A smoke test marks the gate successful; that must not authorize a backend-managed Cell."""
    with pytest.raises(StageError, match="backend-managed"):
        approval_from_response(
            gate="intent", gate_mode="human", response=None, fixture_path=_fixture(tmp_path), managed=True
        )
    # ... and continuation refuses the same decision even if it were recorded some other way.
    replayed = approval_from_response(
        gate="intent", gate_mode="human", response=None, fixture_path=_fixture(tmp_path), managed=False
    )
    with pytest.raises(StageError, match="replay-fixture"):
        check_recorded(
            gate="intent",
            gate_mode="human",
            approval=replayed,
            managed=True,
            cell_id=None,
            cell_epoch=None,
            inputs_digest=None,
        )


def test_a_malformed_replay_fixture_fails_closed(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text('{"intent": {"decision": "yes"}}', encoding="utf-8")
    with pytest.raises(StageError, match="decision and actor"):
        replay_approval("intent", fixture_path=str(bad), managed=False)
    with pytest.raises(StageError, match="unreadable"):
        replay_approval("intent", fixture_path=str(tmp_path / "absent.json"), managed=False)


# ------------------------------------------------- Airflow and local replay agree


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ({}, "human"),
        ({"auto": False}, "human"),
        ({"auto": True}, "auto"),
        ({"mode": "human"}, "human"),
        ({"mode": "auto"}, "auto"),
        ({"mode": "auto", "auto": True}, "auto"),
    ],
)
def test_dag_parse_time_and_runtime_resolve_the_same_gate_mode(spec: dict, expected: str) -> None:
    """``dags/blueprints.py`` cannot import swfactory at parse time, so it carries its own copy of
    this resolution; the two must never disagree about who owns a gate."""
    import importlib.util

    # The `test` job runs without Airflow on purpose -- that is `airflow-main`'s job -- and loading
    # the DAG module imports it. Skipping keeps this honest: the assertion below still runs in every
    # environment that can actually construct a DAG.
    pytest.importorskip("airflow")

    module_spec = importlib.util.spec_from_file_location("swf_dags_blueprints_mode", DAGS / "blueprints.py")
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    assert declared_mode(spec) == expected
    assert module.gate_mode(spec) == expected


def test_a_gate_that_contradicts_itself_is_rejected() -> None:
    with pytest.raises(ValueError, match="contradict"):
        declared_mode({"mode": "human", "auto": True})


def test_gatespec_folds_the_legacy_auto_flag_into_one_declaration() -> None:
    """Third-party blueprints still spelling ``auto`` keep working, with one stored meaning."""
    from pydantic import ValidationError

    from swfactory.blueprint import GateSpec

    base = {"after": "intent", "artifact": "intent.md"}
    assert GateSpec(**base).mode == "human"  # the safe default is the declared default
    assert GateSpec(**base, auto=True).mode == "auto"
    assert GateSpec(**base, auto=False).mode == "human"
    assert GateSpec(**base, auto=True).auto is True
    with pytest.raises(ValidationError, match="contradict"):
        GateSpec(**base, mode="human", auto=True)


def test_shipped_blueprints_declare_their_gate_authority_explicitly() -> None:
    """The lines the issue names must say ``mode = "human"`` in the file, not rely on a default."""
    import tomllib

    from swfactory.blueprint import blueprint_paths

    for path in blueprint_paths():
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        for gate in data.get("gates", []):
            assert "mode" in gate, f"{path.name}: gate after {gate['after']!r} declares no mode"
            assert "auto" not in gate, f"{path.name}: gate after {gate['after']!r} still uses the legacy flag"
    liquid = tomllib.loads((REPO / "blueprints" / "liquid.toml").read_text(encoding="utf-8"))
    assert [g["mode"] for g in liquid["gates"]] == ["human", "human"]


# ------------------------------------------------- the record task, end to end


@pytest.mark.parametrize("managed", [False, True])
def test_record_task_refuses_a_gate_that_was_only_marked_successful(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, managed: bool
) -> None:
    """``dag.test(mark_success_pattern=...)`` leaves no HITL response at all. That used to record
    actor "auto" and let the line continue."""
    import importlib.util

    pytest.importorskip("airflow")
    module_spec = importlib.util.spec_from_file_location("swf_dags_blueprints_record", DAGS / "blueprints.py")
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    ctx = _with_artifacts(_ctx(tmp_path))
    monkeypatch.setattr(module, "_ctx", lambda name, job, run_id: ctx)
    record = module._record_task("factory", "intent", "human").function
    ti = SimpleNamespace(map_index=0, xcom_pull=lambda **kw: None)
    dag_run = SimpleNamespace(run_id="manual__2026-09-02T03:00:00+00:00")
    job = {"job_idx": 0, "cell_managed": managed}
    with pytest.raises(StageError, match="no recorded response"):
        record(job, ti=ti, dag_run=dag_run)
    assert not ctx.state.has_artifact(f"{ctx.art}/approvals.json")


def test_record_task_uses_the_replay_fixture_only_for_unmanaged_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import importlib.util

    pytest.importorskip("airflow")
    module_spec = importlib.util.spec_from_file_location("swf_dags_blueprints_replay", DAGS / "blueprints.py")
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    ctx = _with_artifacts(_ctx(tmp_path, gate_replay=_fixture(tmp_path)))
    monkeypatch.setattr(module, "_ctx", lambda name, job, run_id: ctx)
    record = module._record_task("factory", "intent", "human").function
    ti = SimpleNamespace(map_index=0, xcom_pull=lambda **kw: None)
    dag_run = SimpleNamespace(run_id="manual__2026-09-02T03:00:00+00:00")

    out = record({"job_idx": 0, "cell_managed": False}, ti=ti, dag_run=dag_run)
    assert (out["actor"], out["mode"]) == ("replay:smoke", "replay")
    with pytest.raises(StageError, match="backend-managed"):
        record({"job_idx": 0, "cell_managed": True}, ti=ti, dag_run=dag_run)


# ------------------------------------------------- the third enforcement point must be wired


def _recorded(ctx, approvals: list[Approval]) -> list[Approval]:
    """Write approvals.json exactly as `record_approval` leaves it, bypassing its checks.

    Continuation has to stand on its own: a decision can reach `approvals.json` from an earlier
    epoch, an older release, or a hand-edited artifact directory. Building the file directly is how
    a test reaches `_validate_approvals` as a real chain rather than as a function call.
    """
    ctx.write_artifact(f"{ctx.art}/approvals.json", json.dumps([a.model_dump(mode="json") for a in approvals]))
    return approvals


def _stamped(ctx, gate: str, **overrides) -> Approval:
    digest = hashlib.sha256(ctx.read_artifact(f"{ctx.art}/{gate}.md").encode("utf-8")).hexdigest()
    base = {
        "gate": gate,
        "decision": "approve",
        "actor": "alice",
        "mode": "human",
        "at": "2026-09-09T00:00:00Z",
        "artifact_sha256": digest,
        "cell_id": CELL_ID,
        "cell_epoch": 3,
        "inputs_digest": accepted_inputs.digest_of(ctx.state),
        # After the current admission, so the staleness check reads it as a fresh answer. An answer
        # predating the pin is a separate test.
        "responded_at": "2099-01-01T00:00:00Z",
    }
    return Approval(**{**base, **overrides})


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"cell_epoch": 2}, "/2, not"),
        ({"cell_id": "cell_someone_else"}, "Cell"),
        ({"mode": "auto", "actor": "auto"}, "auto"),
        ({"mode": "replay", "actor": "replay:smoke"}, "replay"),
    ],
    ids=["foreign-epoch", "foreign-cell", "automatic-default", "replay-fixture"],
)
def test_continuation_refuses_a_recorded_chain_that_lacks_the_declared_authority(
    tmp_path: Path, overrides: dict, expected: str
) -> None:
    """Deleting the whole `check_recorded(...)` call from `_validate_approvals` used to leave the
    entire suite green: the function was tested directly, but nothing asserted that continuation
    ever calls it. An enforcement point defended only by the author remembering it is not defended.

    Each case here is a decision that recording would have refused, arriving at continuation
    anyway — which is the situation continuation exists for.
    """
    ctx = _with_artifacts(_ctx(tmp_path))
    _bind_cell(ctx, epoch=3, managed=True)
    gates = [g for g in stages._pipeline(ctx) if isinstance(g, Gate)]
    approvals = _recorded(ctx, [_stamped(ctx, g.name, **overrides) for g in gates])

    with pytest.raises(StageError) as caught:
        stages._validate_approvals(ctx, approvals)
    assert expected.casefold() in str(caught.value).casefold(), caught.value


def test_continuation_accepts_the_chain_recording_would_have_written(tmp_path: Path) -> None:
    """The mirror of the test above: without it, refusing everything would also pass."""
    ctx = _with_artifacts(_ctx(tmp_path))
    _bind_cell(ctx, epoch=3, managed=True)
    gates = [g for g in stages._pipeline(ctx) if isinstance(g, Gate)]
    approvals = _recorded(ctx, [_stamped(ctx, g.name) for g in gates])
    rejected, gate = stages._validate_approvals(ctx, approvals)
    assert rejected is False and gate is None


# ------------------------------------------------- the fixture must prove it is not production


@pytest.mark.parametrize(
    ("cfg", "expected"),
    [
        ({"scm": "github"}, "scm 'github'"),
        ({"managed": True}, "backend-managed"),
    ],
    ids=["publishes-to-github", "backend-managed-cell"],
)
def test_the_replay_fixture_cannot_answer_a_run_that_can_reach_the_outside(
    tmp_path: Path, cfg: dict, expected: str
) -> None:
    """Guarding the fixture on `managed` alone reopened the hole this module exists to close.

    `managed` is false for every direct CLI run, including `--scm github` — the command line the
    self-hosting guide documents. So the shipped fixture could approve a declared human gate and
    publish a real pull request. One environment variable had replaced another.

    The fixture now has to prove it is not production: each capability that lets a run reach the
    outside world disqualifies it on its own, rather than all of them being waived by one flag.
    """
    from swfactory.approval_policy import replay_approval

    with pytest.raises(StageError) as caught:
        replay_approval(
            "intent",
            fixture_path=_fixture(tmp_path),
            **{"managed": False, **cfg},
        )
    assert expected in str(caught.value)


def test_the_replay_fixture_still_answers_a_local_scripted_run(tmp_path: Path) -> None:
    """The mirror: the smoke path this fixture exists for must keep working, or the guard is just
    a way of deleting the feature."""
    from swfactory.approval_policy import replay_approval

    approval = replay_approval("intent", fixture_path=_fixture(tmp_path), managed=False)
    assert approval is not None
    assert approval.mode == "replay" and approval.actor == "replay:smoke"
    # The unattended evals run the real agent against a local remote. The agent decides what is
    # written, not where it goes, so it must not disqualify the fixture on its own.
    assert replay_approval("intent", fixture_path=_fixture(tmp_path), managed=False, agent="claude") is not None


def test_recording_checks_a_carried_digest_instead_of_restamping_it(tmp_path: Path) -> None:
    """Recording used to overwrite `artifact_sha256` with whatever the artifact digests to now.

    So a decision could be re-recorded over a changed artifact — an approver's yes to one plan
    silently becoming a yes to another — and the digest was defended only at continuation, one
    enforcement point where the design claims two.
    """
    ctx = _with_artifacts(_ctx(tmp_path))
    _bind_cell(ctx, epoch=3, managed=True)
    stale = _stamped(ctx, "intent")
    ctx.write_artifact(f"{ctx.art}/intent.md", "# intent, rewritten after the approver said yes\n")

    with pytest.raises(StageError) as caught:
        stages.record_approval(ctx, stale)
    assert "now digests to" in str(caught.value)


@pytest.mark.parametrize("actor", ["AUTO", "Auto", "REPLAY:smoke", "Replay:smoke"])
def test_a_reserved_actor_cannot_be_impersonated_by_changing_its_case(actor: str) -> None:
    """`approvals.json` is the record an auditor reads to see who approved. A responder named
    "AUTO" recorded actor='AUTO', mode='human' and impersonated the runtime's own actor name."""
    from swfactory.approval_policy import approval_from_response

    response = {"chosen_options": ["Approve"], "params_input": {}, "responded_by_user": {"id": "u1", "name": actor}}
    with pytest.raises(StageError) as caught:
        approval_from_response(gate="intent", gate_mode="human", response=response)
    assert "reserved actor" in str(caught.value)


def test_a_stale_answer_cannot_be_re_recorded_after_the_inputs_were_re_accepted(tmp_path: Path) -> None:
    """H1: the pin binds the approval RECORD, not the approval CHANNEL.

    `record_<stage>` builds its Approval from the raw operator response, so it arrives with no
    inputs digest and recording stamps whatever is pinned now. After a re-accept, an ordinary
    Airflow task clear or retry re-recorded alice's old answer against the NEW inputs and it
    validated cleanly — her yes to one plan silently becoming her yes to another. No attacker
    needed, and it falsified the documented promise that every gate must be answered again.

    The answer's own timestamp is what separates a person answering again from an old XCom being
    read twice.
    """
    ctx = _with_artifacts(_ctx(tmp_path))
    answered = "2026-05-01T00:00:00Z"
    ctx.state.write_control(accepted_inputs.ADMITTED_AT_FILE, "2026-04-01T00:00:00+00:00\n")
    stages.record_approval(
        ctx, approval_from_response(gate="intent", gate_mode="human", response=_human(responded_at=answered))
    )

    # The re-accept, and the admission that follows it, both happen after alice answered. The two
    # times are written explicitly rather than left to the wall clock, so the test states the
    # ordering it depends on instead of hoping for it.
    accepted_inputs.reaccept(ctx.state, actor="maintainer", reason="the blueprint limit changed")
    accepted_inputs.admit(ctx.state, accepted_inputs.snapshot(ctx.cfg, None, ctx.issue))
    ctx.state.write_control(accepted_inputs.ADMITTED_AT_FILE, "2026-06-01T00:00:00+00:00\n")

    with pytest.raises(StageError, match="was given before this run admitted"):
        stages.record_approval(
            ctx, approval_from_response(gate="intent", gate_mode="human", response=_human(responded_at=answered))
        )

    # A person answering again, after the re-accept, is recorded normally and against the new pin.
    stages.record_approval(ctx, approval_from_response(gate="intent", gate_mode="human", response=_human()))
    recorded = {a.gate: a for a in stages._load_approvals(ctx)}["intent"]
    assert recorded.actor == "alice"
    assert recorded.inputs_digest == accepted_inputs.digest_of(ctx.state)


def test_an_automatic_or_replay_decision_is_not_treated_as_stale(tmp_path: Path) -> None:
    """The mirror. Those decisions are produced by the runtime at record time rather than pulled
    from an operator's XCom, so a retry re-derives them under whatever is pinned now — there is no
    stale answer to launder, and refusing them would strand every automatic gate."""
    ctx = _with_artifacts(_ctx(tmp_path, gate_replay=_fixture(tmp_path)))
    accepted_inputs.reaccept(ctx.state, actor="maintainer", reason="policy changed")
    accepted_inputs.admit(ctx.state, accepted_inputs.snapshot(ctx.cfg, None, ctx.issue))

    replayed = approval_from_response(gate="intent", gate_mode="human", response=None, fixture_path=ctx.cfg.gate_replay)
    assert replayed.mode == "replay" and replayed.responded_at is None
    stages.record_approval(ctx, replayed)  # must not raise
