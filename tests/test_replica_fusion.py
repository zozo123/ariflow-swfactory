"""Real Git/pytest acceptance of bounded fusion at the Airflow build entrypoint."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from support import git, make_repo

from swfactory import stages, work_stage
from swfactory.blueprint import Blueprint, FusionSpec, Target
from swfactory.models import AgentResult, Issue, Plan, StageError
from swfactory.replica_fusion import CONTROL, REPORT
from swfactory.sandbox import LocalSandbox


class ExperimentalAgent:
    kind = "scripted"

    def __init__(self, *, overlap=False, second_round=False, broken_synthesis=False):
        self.overlap = overlap
        self.second_round = second_round
        self.broken_synthesis = broken_synthesis
        self.calls = []

    def run(self, sb, *, stage, iteration, prompt, schema, **kwargs):
        role = prompt.splitlines()[0].rsplit(": ", 1)[1]
        self.calls.append((role, iteration, sb.workdir, prompt))
        if role in {"diagnostic", "challenger"}:
            return AgentResult(agent="scripted", data={"observations": ["Both independent obligations must hold."]})
        if self.second_round and iteration < 20:
            sb.write("left.py", "correct = False\n")
        elif role == "repair-a":
            sb.write("left.py", "correct = True\n")
        elif role == "repair-b":
            sb.write("left.py" if self.overlap else "right.py", "correct = True\n")
        elif not self.broken_synthesis:
            sb.write("left.py", "correct = True\n")
            sb.write("right.py", "correct = True\n")
        return AgentResult(agent="scripted", data={"summary": role})


def context(tmp_path: Path, agent: ExperimentalAgent, *, rounds=2):
    repo, _ = make_repo(
        tmp_path,
        {
            "factory.toml": '[commands]\ntest = "python -m pytest --junitxml=.factory/junit.xml"\n'
            '[paths]\nsource = "src"\ntests = "tests"\nprotected = ["factory.toml", "tests"]\n',
            "left.py": "correct = False\n",
            "right.py": "correct = False\n",
            "tests/test_obligations.py": "import left, right\n\ndef test_left():\n    assert left.correct\n\n"
            "def test_right():\n    assert right.correct\n",
        },
    )
    # The target verifier uses the same independently maintained test bytes for all candidates.
    cfg = stages.Config(
        issue="FUSION-1",
        repo="example/product",
        target_dir="",
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(repo),
        run_id="fusion-test",
        max_budget_usd=8,
        max_budget_usd_per_stage=2,
    )
    bp = Blueprint(
        name="fusion-test",
        targets=[Target(repo="example/product")],
        order=["intent", "plan", "build_and_test", "review", "deliver"],
        fusion=FusionSpec(max_rounds=rounds),
    )
    ctx = stages.Ctx(
        cfg,
        LocalSandbox(repo),
        agent,
        None,
        Issue(id="FUSION-1", title="Fix both obligations", body=""),
        tmp_path / "run",
        blueprint=bp,
    )
    stages._seed_exclude(ctx)
    stages._record_workspace_head(ctx, git(repo, "rev-parse", "HEAD"))
    ctx.write_artifact(
        f"{ctx.art}/plan.json",
        Plan(files=["left.py", "right.py"], steps=["Fix both"], tests=["Both must pass"]).model_dump_json(),
    )
    ctx.write_artifact(f"{ctx.art}/plan.md", "Fix left and right; retain the independent tests.")
    ctx.write_artifact(f"{ctx.art}/spec.md", "Both flags must be correct.")
    return ctx


def report(ctx):
    return json.loads(ctx.read_artifact(f"{ctx.art}/{REPORT}"))


def test_four_separate_checkpoints_create_a_fusion_only_win_and_replay_without_model_calls(tmp_path, monkeypatch):
    agent = ExperimentalAgent()
    ctx = context(tmp_path, agent)
    monkeypatch.setenv("PYTHONPATH", str(Path(stages.__file__).resolve().parents[2]))
    result = work_stage.build_and_test(ctx)
    evidence = report(ctx)

    assert result.numbers["fusion_only"] == 1
    assert {role for role, *_ in agent.calls} == {"repair-a", "repair-b", "diagnostic", "challenger"}
    assert len({path for _, _, path, _ in agent.calls}) == 4
    attempts = evidence["attempts"]
    assert not attempts["round-1-repair-a"]["verification"]["passed"]
    assert not attempts["round-1-repair-b"]["verification"]["passed"]
    assert attempts["round-1-composition"]["verification"]["passed"]
    assert evidence["final_verification"]["passed"]
    assert evidence["verifications"] == 4
    assert evidence["comparison"]["single_candidate"]["passed"] is False
    assert evidence["comparison"]["best_of_candidates"]["passed"] is False
    assert evidence["comparison"]["selected_candidate"]["passed"] is True
    assert all(not Path(path).exists() for _, _, path, _ in agent.calls)
    before = list(agent.calls)
    work_stage.build_and_test(ctx)
    assert agent.calls == before


def test_overlapping_repairs_trigger_semantic_synthesis_using_retained_observations(tmp_path):
    agent = ExperimentalAgent(overlap=True)
    ctx = context(tmp_path, agent)
    work_stage.build_and_test(ctx)
    evidence = report(ctx)
    assert evidence["winner"]["role"] == "synthesis"
    assert "overlapping" in evidence["rounds"][0]["composition"]
    prompt = next(prompt for role, _, _, prompt in agent.calls if role == "synthesis")
    assert "Both independent obligations" in prompt
    assert "diff --git" in prompt


def test_second_round_is_guided_by_counterexamples_and_is_bounded(tmp_path):
    agent = ExperimentalAgent(second_round=True)
    ctx = context(tmp_path, agent)
    work_stage.build_and_test(ctx)
    evidence = report(ctx)
    assert len(evidence["rounds"]) == 2
    assert any(
        "Controller-observed failures" in prompt and '"passed": false' in prompt
        for role, iteration, _, prompt in agent.calls
        if iteration >= 20
    )
    assert len(agent.calls) <= 10
    assert evidence["verifications"] <= 9


def test_passing_individual_is_retained_when_synthesis_fails(tmp_path):
    agent = ExperimentalAgent(broken_synthesis=True)
    original = agent.run

    def run(sb, **kwargs):
        result = original(sb, **kwargs)
        if kwargs["prompt"].splitlines()[0].endswith("repair-a"):
            sb.write("right.py", "correct = True\n")
        return result

    agent.run = run
    ctx = context(tmp_path, agent)
    work_stage.build_and_test(ctx)
    assert report(ctx)["winner"]["role"] == "repair-a"


def test_cancellation_in_doubt_attempts_and_changed_inputs_refuse_adoption(tmp_path):
    ctx = context(tmp_path, ExperimentalAgent())
    ctx.state.write_control("fusion-cancelled", "true")
    with pytest.raises(StageError, match="cancelled"):
        work_stage.build_and_test(ctx)
    assert "correct = False" in (Path(ctx.sb.workdir) / "left.py").read_text()
    assert not ctx.agent.calls


def test_interrupted_paid_attempt_is_not_replayed(tmp_path):
    ctx = context(tmp_path, ExperimentalAgent())
    from swfactory.replica_fusion import FusionExperiment

    experiment = FusionExperiment(ctx)
    experiment.progress["attempts"]["lost"] = {"state": "in-flight"}
    experiment.save()
    with pytest.raises(StageError, match="in doubt"):
        work_stage.build_and_test(ctx)
    assert not ctx.agent.calls


def test_pinned_spec_cannot_change_on_retry(tmp_path):
    ctx = context(tmp_path, ExperimentalAgent())
    from swfactory.replica_fusion import FusionExperiment

    FusionExperiment(ctx)
    assert ctx.state.has_control(CONTROL)
    ctx.blueprint.fusion.max_rounds = 1
    with pytest.raises(StageError, match="inputs changed"):
        work_stage.build_and_test(ctx)


def test_missing_individual_verification_is_not_a_fusion_only_win(tmp_path):
    agent = ExperimentalAgent(overlap=True)
    original = agent.run

    def run(sb, **kwargs):
        if kwargs["prompt"].splitlines()[0].endswith("repair-b"):
            return AgentResult(agent="scripted", is_error=True, subtype="refused")
        return original(sb, **kwargs)

    agent.run = run
    ctx = context(tmp_path, agent)
    work_stage.build_and_test(ctx)
    assert report(ctx)["winner"]["role"] == "synthesis"
    assert report(ctx)["winner"]["fusion_only"] is False
    assert report(ctx)["comparison"]["best_of_candidates"]["passed"] is None


def test_stale_backend_cell_epoch_refuses_before_spending_or_launch(tmp_path, monkeypatch):
    ctx = context(tmp_path, ExperimentalAgent())
    ctx.state.write_control(
        "cell.json", json.dumps({"managed": True, "cell_id": "cell", "epoch": 7, "policy_digest": "policy:bound"})
    )
    monkeypatch.setattr(
        "swfactory.cell_callback.post",
        lambda *args, **kwargs: {"epoch": 8, "policy_digest": "policy:bound", "state": "running"},
    )
    with pytest.raises(StageError, match="active Cell epoch"):
        work_stage.build_and_test(ctx)
    assert not ctx.agent.calls
    assert not ctx.state.has_control("replica-fusion-budget.json")
