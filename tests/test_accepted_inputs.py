"""One Cell epoch executes the inputs it was admitted with, or refuses (issue #2065).

Nothing used to pin what an epoch accepted. ``dags/blueprints.py`` reloads the named blueprint for
every task context, ``runtime._prepare_ctx`` re-fetches the issue for every context, and
``stages.setup`` computed the policy hash only in setup with no later-stage identity check. So the
issue body, the blueprint on the worker's disk, or a policy-affecting ``SWF_*`` variable could
change between two tasks of one epoch and nothing noticed: a person approved one plan and the
factory built a different one.

Every test here drives ``runtime.build_ctx`` -- the one funnel the CLI and every Airflow task share
-- twice over the same run id, changing exactly one input in between. The second call must either
return the accepted snapshot or refuse, and it must refuse at context construction, before any
stage function, agent call or publication.

Hermetic: a LocalSandbox and a file issue under ``tmp_path``, no Airflow and no network.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from swfactory import accepted_inputs, stages
from swfactory import metrics as metrics_mod
from swfactory.blueprint import load
from swfactory.models import Approval, Issue, StageError
from swfactory.runtime import build_ctx

ROOT = Path(__file__).resolve().parents[1]
LOCAL = {"agent": "scripted", "sandbox": "local", "scm": "local", "approve": "auto"}
RUN_ID = "p1nn0001"
CELL_ID = "cell_" + "b" * 24

ISSUE_V1 = """---
id: DEMO-1
title: Add percent_change(old, new) to calc
labels: [factory]
---
Return the relative change from old to new as a fraction.
"""
ISSUE_V2 = ISSUE_V1.replace("as a fraction", "as a PERCENTAGE, and delete the old helper")


class _RefusingAgent:
    """Any use of the agent after a refusal is the bug: refusing after the model wrote code is not
    refusing. ``kind`` is a class attribute so building a Ctx stays legal; everything else blows."""

    kind = "scripted"

    def __getattr__(self, name: str) -> object:  # pragma: no cover - the assertion is the point
        raise AssertionError(f"agent I/O ({name!r}) reached before the accepted-inputs check")


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A worker cwd holding its own issue file; the blueprint target is seeded from the checkout."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "issue.md").write_text(ISSUE_V1, encoding="utf-8")
    for name in ("SWF_GATE_REPLAY", "SWF_MAX_TURNS", "SWF_RECORD_DIR", "SWF_SANDBOX_OWNER"):
        monkeypatch.delenv(name, raising=False)
    return tmp_path


def _job(**over: object) -> dict:
    job = {
        "issue": "issue.md",
        "repo": "zozo123/ariflow-swfactory",
        "dir": "demo/target",
        "base_branch": "main",
        "job_idx": 0,
    }
    job.update(over)
    return job


def _ctx(root: Path, *, blueprint=None, job=None, run_id: str = RUN_ID, **kw):
    return build_ctx(
        blueprint if blueprint is not None else load("factory"),
        job if job is not None else _job(),
        run_id=run_id,
        overrides=LOCAL,
        root=root,
        agent=_RefusingAgent(),  # type: ignore[arg-type]
        **kw,
    )


def _local_blueprint(root: Path, *, edit: tuple[str, str] | None = None):
    """A blueprint installed on *this worker*: ``blueprints/factory.toml`` next to the cwd."""
    directory = root / "blueprints"
    directory.mkdir(exist_ok=True)
    text = (ROOT / "blueprints" / "default.toml").read_text(encoding="utf-8")
    if edit is not None:
        old, new = edit
        assert old in text, old
        text = text.replace(old, new)
    (directory / "factory.toml").write_text(text, encoding="utf-8")
    return load("factory")


# ------------------------------------------------- box 1: changed inputs refuse the next task


def test_an_edited_issue_body_refuses_the_next_task_of_the_epoch(workspace: Path) -> None:
    first = _ctx(workspace)
    assert accepted_inputs.stored(first.state) is not None

    (workspace / "issue.md").write_text(ISSUE_V2, encoding="utf-8")
    with pytest.raises(StageError) as error:
        _ctx(workspace)
    assert "issue content" in str(error.value)
    # The refusal is the whole context construction, so no stage body, agent call or publish ran.
    assert first.state.read_jsonl(stages.RUN_STAGES_LOG) == []


def test_a_changed_blueprint_policy_refuses_the_next_task_of_the_epoch(workspace: Path) -> None:
    _ctx(workspace, blueprint=_local_blueprint(workspace))
    edited = _local_blueprint(workspace, edit=("max_turns = 40", "max_turns = 400"))
    with pytest.raises(StageError) as error:
        _ctx(workspace, blueprint=edited)
    assert "blueprint" in str(error.value)


def test_a_policy_affecting_environment_change_refuses_the_next_task(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ctx(workspace)
    monkeypatch.setenv("SWF_MAX_TURNS", "400")  # an ambient worker knob, not the accepted policy
    with pytest.raises(StageError) as error:
        _ctx(workspace)
    assert "policy" in str(error.value)


def test_an_operational_credential_setting_may_differ_between_workers(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Credentials and per-worker paths are not the accepted policy; fencing them would only make
    a second worker impossible to use, without protecting anything a person approved."""
    accepted = _ctx(workspace)
    monkeypatch.setenv("SWF_RECORD_DIR", str(workspace / "recordings"))
    monkeypatch.setenv("SWF_SANDBOX_OWNER", "other-worker@example.com")
    again = _ctx(workspace)
    assert accepted_inputs.stored(again.state) == accepted_inputs.stored(accepted.state)


def test_the_same_issue_reached_by_another_path_is_the_same_accepted_inputs(workspace: Path) -> None:
    """A file issue's URL is ``file://<this worker's absolute path>``. Pinning it would fence two
    workers apart over where their checkout lives, which is not something anyone approved."""
    accepted = _ctx(workspace)
    elsewhere = workspace / "mirror"
    elsewhere.mkdir()
    (elsewhere / "issue.md").write_text(ISSUE_V1, encoding="utf-8")
    again = _ctx(workspace, job=_job(issue="mirror/issue.md"))
    assert accepted_inputs.stored(again.state) == accepted_inputs.stored(accepted.state)


def test_every_config_setting_is_classified_as_policy_identity_or_operational() -> None:
    """A new Config field must be deliberately placed. Without this, adding a policy knob silently
    lands outside the accepted digest and becomes the next thing a worker can change mid-epoch."""
    from swfactory.config import IDENTITY_SETTINGS, Config

    known = set(accepted_inputs.POLICY_SETTINGS) | set(accepted_inputs.OPERATIONAL_SETTINGS) | set(IDENTITY_SETTINGS)
    assert set(Config.model_fields) == known


# ------------------------------------------------- box 2: restarts and other workers


def test_a_restart_with_identical_inputs_reuses_the_accepted_snapshot(workspace: Path) -> None:
    first = _ctx(workspace)
    pinned = accepted_inputs.stored(first.state)
    second = _ctx(workspace)  # a retry/resume rebuilds the whole Ctx from scratch
    assert accepted_inputs.stored(second.state) == pinned
    assert pinned is not None and pinned.digest.startswith("inputs:")


def test_a_worker_with_a_different_installed_blueprint_version_cannot_reinterpret_the_epoch(
    workspace: Path,
) -> None:
    _ctx(workspace, blueprint=_local_blueprint(workspace))
    # Same blueprint NAME, different content: exactly the drift a second worker's checkout causes.
    other = _local_blueprint(workspace, edit=('description = "Default SDLC line', 'description = "Rewritten line'))
    with pytest.raises(StageError) as error:
        _ctx(workspace, blueprint=other)
    assert "new epoch" in str(error.value)


def test_a_different_cell_epoch_over_the_same_run_directory_refuses(workspace: Path) -> None:
    managed = {"cell_id": CELL_ID, "cell_epoch": 2, "cell_managed": True, "cell_policy_digest": "policy:x"}
    _ctx(workspace, job=_job(**managed))
    with pytest.raises(StageError) as error:
        _ctx(workspace, job=_job(**{**managed, "cell_epoch": 3}))
    assert "cell epoch" in str(error.value)


# ------------------------------------------------- box 3: one digest across approval and receipt


def _approve(ctx, gate: str = "intent") -> Approval:
    """Answer one gate and return the decision AS RECORDED (host-stamped digests and all)."""
    ctx.write_artifact(f"{ctx.art}/{'intent.md' if gate == 'intent' else 'plan.md'}", "# artifact\n")
    stages.record_approval(
        ctx,
        Approval(
            gate=gate,
            decision="approve",
            actor="alice",
            mode="human",
            at=datetime.now(UTC),
            responded_at=datetime.now(UTC),
        ),
    )
    recorded = json.loads(ctx.read_artifact(f"{ctx.art}/approvals.json"))
    return Approval.model_validate(next(row for row in recorded if row["gate"] == gate))


def test_a_recorded_approval_carries_the_accepted_inputs_digest(workspace: Path) -> None:
    ctx = _ctx(workspace)
    _approve(ctx)
    recorded = json.loads(ctx.read_artifact(f"{ctx.art}/approvals.json"))
    pinned = accepted_inputs.stored(ctx.state)
    assert pinned is not None
    assert [row["inputs_digest"] for row in recorded] == [pinned.digest]


def test_an_answer_given_for_other_inputs_is_refused_rather_than_restamped(workspace: Path) -> None:
    ctx = _ctx(workspace)
    ctx.write_artifact(f"{ctx.art}/intent.md", "# artifact\n")
    stale = Approval(
        gate="intent",
        decision="approve",
        actor="alice",
        mode="human",
        at=datetime.now(UTC),
        responded_at=datetime.now(UTC),
        inputs_digest="inputs:" + "0" * 64,
    )
    with pytest.raises(StageError) as error:
        stages.record_approval(ctx, stale)
    assert "inputs" in str(error.value)


def test_publication_refuses_an_approval_bound_to_other_inputs_before_publishing(workspace: Path) -> None:
    ctx = _ctx(workspace)
    approval = _approve(ctx)
    drifted = approval.model_copy(update={"inputs_digest": "inputs:" + "1" * 64})
    with pytest.raises(StageError) as error:
        stages._validate_approvals(ctx, [drifted])
    assert "inputs" in str(error.value)


def test_report_and_publication_receipt_name_the_same_accepted_digests(workspace: Path) -> None:
    ctx = _ctx(workspace)
    approval = _approve(ctx)
    pinned = accepted_inputs.stored(ctx.state)
    assert pinned is not None
    report = stages.build_report(ctx, [approval])
    assert report.inputs_digest == pinned.digest
    written = metrics_mod.write_run_metrics(ctx, [], [approval])
    assert written["inputs_digest"] == pinned.digest
    assert written["policy_sha256"] == pinned.policy_sha256
    body = stages.pr_body(ctx, findings=[], dropped_nits=0, approvals=[approval], stages=[])
    assert pinned.digest in body


# ------------------------------------------------- box 4: setup identity, replay, new epoch


def test_setup_still_refuses_a_run_id_bound_to_another_job_identity(workspace: Path) -> None:
    ctx = _ctx(workspace)
    ctx.state.write_control("identity.json", json.dumps({"schema": 3, "run_id": ctx.cfg.run_id}) + "\n")
    with pytest.raises(StageError) as error:
        stages.setup(ctx)
    assert "already bound to a different job identity" in str(error.value)


def _replay_fixture(path: Path, actor: str = "smoke", decision: str = "approve") -> str:
    path.write_text(
        json.dumps({gate: {"decision": decision, "actor": actor} for gate in ("intent", "plan")}),
        encoding="utf-8",
    )
    return str(path)


def test_changing_the_replay_fixture_answers_mid_epoch_refuses(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``SWF_GATE_REPLAY`` decides gate answers, so its CONTENT is accepted policy."""
    fixture = workspace / "gate-replay.json"
    monkeypatch.setenv("SWF_GATE_REPLAY", _replay_fixture(fixture))
    _ctx(workspace)
    _replay_fixture(fixture, actor="someone-else", decision="reject")
    with pytest.raises(StageError) as error:
        _ctx(workspace)
    assert "policy" in str(error.value)


def test_introducing_a_replay_fixture_mid_epoch_refuses(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _ctx(workspace)
    monkeypatch.setenv("SWF_GATE_REPLAY", _replay_fixture(workspace / "gate-replay.json"))
    with pytest.raises(StageError) as error:
        _ctx(workspace)
    assert "policy" in str(error.value)


def test_the_same_replay_answers_at_another_path_are_the_same_policy(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fixture PATH is operational (it differs per worker checkout); its answers are not."""
    monkeypatch.setenv("SWF_GATE_REPLAY", _replay_fixture(workspace / "gate-replay.json"))
    first = _ctx(workspace)
    elsewhere = workspace / "other"
    elsewhere.mkdir()
    monkeypatch.setenv("SWF_GATE_REPLAY", _replay_fixture(elsewhere / "gate-replay.json"))
    again = _ctx(workspace)
    assert accepted_inputs.stored(again.state) == accepted_inputs.stored(first.state)


def test_a_new_epoch_is_the_documented_route_for_genuinely_changed_inputs(workspace: Path) -> None:
    ctx = _ctx(workspace)
    approval = _approve(ctx)
    before = accepted_inputs.stored(ctx.state)
    assert before is not None

    (workspace / "issue.md").write_text(ISSUE_V2, encoding="utf-8")
    with pytest.raises(StageError):
        _ctx(workspace)

    accepted_inputs.reaccept(ctx.state, actor="alice", reason="issue #7 edited")
    # Retiring the pin does not itself accept anything: the NEXT task admits what is now there.
    assert accepted_inputs.stored(ctx.state) is None
    after = accepted_inputs.stored(_ctx(workspace).state)
    assert after is not None and after.digest != before.digest

    # The old answer approved the old inputs, so it no longer publishes: it must be re-answered.
    with pytest.raises(StageError) as error:
        stages._validate_approvals(ctx, [approval])
    assert "accepted inputs" in str(error.value)
    history = ctx.state.read_jsonl(accepted_inputs.HISTORY_LOG)
    assert [row["event"] for row in history] == ["accepted", "superseded", "accepted"]
    assert history[1] == {
        "event": "superseded",
        "actor": "alice",
        "reason": "issue #7 edited",
        "superseded_digest": before.digest,
    }
    assert history[2]["digest"] == after.digest


def test_a_backend_managed_cell_re_accepts_by_advancing_its_epoch_not_locally(workspace: Path) -> None:
    managed = {"cell_id": CELL_ID, "cell_epoch": 2, "cell_managed": True, "cell_policy_digest": "policy:x"}
    ctx = _ctx(workspace, job=_job(**managed))
    with pytest.raises(StageError) as error:
        accepted_inputs.reaccept(ctx.state, actor="alice", reason="edited")
    assert "advancing the Cell epoch" in str(error.value)
    assert accepted_inputs.stored(ctx.state) is not None  # the pin survives a refused re-accept


def test_who_owns_the_epoch_is_part_of_the_snapshot(tmp_path: Path) -> None:
    """H2: `managed` decides who owns the Cell epoch, so leaving it out pinned the wrong half.

    A re-run of the same Airflow run that dropped `factory_cell_bindings` produced the same
    cell_id and epoch with `cell_managed=False`, and was admitted with no refusal at all.
    `cell_evidence` then reported unmanaged, which lifts both the refusal that stops a worker
    re-pinning a backend-managed Cell locally and the ban on a replay fixture authorizing managed
    work. The fence advertised that it pinned the epoch while the flag deciding who owns that
    epoch stayed loose.
    """
    from swfactory.config import Config

    cfg = Config(issue="demo/issue.md", run_id=RUN_ID)
    issue = Issue(id="DEMO-1", title="t", body="")
    kw = {"cell_id": "cell_" + "a" * 24, "cell_epoch": 3}
    managed = accepted_inputs.snapshot(cfg, None, issue, managed=True, **kw)
    unmanaged = accepted_inputs.snapshot(cfg, None, issue, managed=False, **kw)

    assert managed.digest != unmanaged.digest, "the ownership flag does not reach the digest"
    assert "who owns this Cell epoch" in ", ".join(accepted_inputs.describe_mismatch(managed, unmanaged))


def test_flipping_the_ownership_flag_off_is_refused_by_the_pin(tmp_path: Path) -> None:
    """The same defect at the fence rather than in the model."""
    from swfactory.config import Config
    from swfactory.state import RunState

    cfg = Config(issue="demo/issue.md", run_id=RUN_ID)
    issue = Issue(id="DEMO-1", title="t", body="")
    state = RunState(tmp_path / "run")
    kw = {"cell_id": "cell_" + "a" * 24, "cell_epoch": 3}

    accepted_inputs.admit(state, accepted_inputs.snapshot(cfg, None, issue, managed=True, **kw))
    with pytest.raises(StageError, match="who owns this Cell epoch"):
        accepted_inputs.admit(state, accepted_inputs.snapshot(cfg, None, issue, managed=False, **kw))
