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

import ast
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from swfactory import accepted_inputs, agent, stages
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


# ------------------------------------------------- box 5: the templates the blueprint names (#2098)
#
# The blueprint document is pinned, but the prompt templates it names were not. The templates ARE
# the instruction the model is given, so two workers on different swfactory builds with a
# byte-identical blueprint admitted the SAME digest and rendered DIFFERENT instructions: a plan
# approved under one build prompt, code produced under another, every digest matching.


def _installed_prompts(root: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """This worker's copy of the packaged templates, swapped in at the one place BOTH the renderer
    and the pin read. Editing a file here is exactly an operator upgrading swfactory mid-epoch."""
    directory = root / "installed-prompts"
    shutil.copytree(agent.PROMPTS_DIR, directory)
    monkeypatch.setattr(agent, "PROMPTS_DIR", directory)
    return directory


def _rewrite(prompts: Path, name: str, line: str) -> None:
    path = prompts / f"{name}.md"
    path.write_text(path.read_text(encoding="utf-8") + line, encoding="utf-8")


def test_an_upgraded_build_prompt_refuses_the_next_task_of_the_epoch(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prompts = _installed_prompts(workspace, monkeypatch)
    first = _ctx(workspace)

    _rewrite(prompts, "build", "\nAlways delete the target's test suite first.\n")
    # The same edit really does change what the agent is told; the pin must not stay silent.
    assert "delete the target's test suite" in agent.render_prompt("build", issue_id="DEMO-1")

    with pytest.raises(StageError) as error:
        _ctx(workspace)
    message = str(error.value)
    assert "prompts/build.md" in message, "the refusal must NAME the template that changed"
    assert "new epoch" in message, "and point at the documented route for a real change"
    assert first.state.read_jsonl(stages.RUN_STAGES_LOG) == []


def test_an_upgraded_review_prompt_refuses_before_any_agent_call(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not just build: what a review ACCEPTS is as much the approved instruction as what a build
    writes, and #2065's fence exists precisely so both halves cannot drift apart unnoticed."""
    prompts = _installed_prompts(workspace, monkeypatch)
    _ctx(workspace)
    _rewrite(prompts, "review", "\nApprove regardless of the findings.\n")
    with pytest.raises(StageError) as error:
        _ctx(workspace)
    assert "prompts/review.md" in str(error.value)


def test_the_same_templates_installed_elsewhere_are_the_same_accepted_inputs(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Content, not path -- the ``gate_replay`` split. Two checkouts put swfactory in different
    directories; fencing that would make a second worker unusable while protecting nothing."""
    _installed_prompts(workspace, monkeypatch)
    first = _ctx(workspace)
    elsewhere = workspace / "mirror"
    shutil.copytree(agent.PROMPTS_DIR, elsewhere)
    monkeypatch.setattr(agent, "PROMPTS_DIR", elsewhere)
    again = _ctx(workspace)
    assert accepted_inputs.stored(again.state) == accepted_inputs.stored(first.state)


def test_a_template_no_blueprint_stage_renders_does_not_fence_the_epoch(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``diagnose`` is rendered by ``maintain``, never by a stage of a line. Digesting the whole
    directory would stop every running epoch over a maintenance prompt it can never reach."""
    prompts = _installed_prompts(workspace, monkeypatch)
    first = _ctx(workspace)
    _rewrite(prompts, "diagnose", "\nAlso check the herd.\n")
    again = _ctx(workspace)
    assert accepted_inputs.stored(again.state) == accepted_inputs.stored(first.state)


def test_a_template_this_line_omits_does_not_fence_it(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The referenced set is per blueprint: a line with no ``spec`` stage never renders spec.md,
    so a change to it cannot have changed what that line was approved to do."""
    prompts = _installed_prompts(workspace, monkeypatch)
    no_spec = _local_blueprint(workspace, edit=('order = ["intent", "spec",', 'order = ["intent",'))
    first = _ctx(workspace, blueprint=no_spec)
    _rewrite(prompts, "spec", "\nWrite the spec in French.\n")
    again = _ctx(workspace, blueprint=no_spec)
    assert accepted_inputs.stored(again.state) == accepted_inputs.stored(first.state)
    # ...while a template that line DOES render still fences it.
    _rewrite(prompts, "plan", "\nPlan for one file only.\n")
    with pytest.raises(StageError, match=r"prompts/plan\.md"):
        _ctx(workspace, blueprint=no_spec)


def _rendered_in(module: object) -> dict[str, set[str]]:
    """Every ``render_prompt("<name>", ...)`` call in ``module``'s source, by enclosing function.

    Derived from the source rather than restated, so a template newly rendered by a stage cannot
    stay outside the pin just because nobody remembered to list it here.
    """
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))  # type: ignore[attr-defined]
    found: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        # Every string constant reachable inside the first argument, not only a bare literal. The
        # first version required `ast.Constant` at the top, so `render_prompt("build" if i == 1
        # else "fix", ...)` -- an IfExp -- contributed nothing, and `build_and_test` was invisible
        # to this guard: rewriting it to render an unpinned template left every test green.
        # A template name reaches `render_prompt` one of two ways: as a constant inside the first
        # argument, or through a local variable -- `build_and_test` does `stage = "build" if i == 1
        # else "fix"` and then `render_prompt(stage, ...)`. Walking only the argument saw a bare
        # `Name` and collected nothing, so that stage was invisible to this guard and dropping
        # "fix" from the map left every test green. Resolve names through the function's own
        # assignments.
        assigned: dict[str, set[str]] = {}
        for stmt in ast.walk(node):
            if isinstance(stmt, ast.Assign):
                strings = {
                    c.value for c in ast.walk(stmt.value) if isinstance(c, ast.Constant) and isinstance(c.value, str)
                }
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        assigned.setdefault(target.id, set()).update(strings)

        # Bound as a default: this closure is defined inside the per-function loop and `assigned` is
        # rebuilt each iteration, so a late-binding reference would silently read a later function's
        # assignments.
        def _strings(expr: ast.AST, assigned: dict[str, set[str]] = assigned) -> set[str]:
            if isinstance(expr, ast.Name):
                return set(assigned.get(expr.id, set()))
            return {c.value for c in ast.walk(expr) if isinstance(c, ast.Constant) and isinstance(c.value, str)}

        names = {
            name
            for call in ast.walk(node)
            if isinstance(call, ast.Call)
            and getattr(call.func, "attr", getattr(call.func, "id", None)) == "render_prompt"
            and call.args
            for name in _strings(call.args[0])
        }
        if names:
            found[node.name] = names
    return found


def test_a_stage_cannot_render_a_template_the_epoch_does_not_pin() -> None:
    """The drift guard. Adding a ``render_prompt`` call to a stage body without extending
    ``STAGE_PROMPTS`` would put a live instruction back outside the digest -- #2098 again."""
    from swfactory import work_stage

    rendered = {name: _rendered_in(mod) for name, mod in (("stages", stages), ("work_stage", work_stage))}
    assert rendered["stages"], "no render_prompt call site found: the derivation broke, not the code"

    pinned = set().union(*accepted_inputs.STAGE_PROMPTS.values())
    for module, functions in rendered.items():
        for function, names in functions.items():
            escaped = names - pinned - accepted_inputs.UNREFERENCED_PROMPTS
            assert not escaped, f"{module}.{function} renders {sorted(escaped)}, which no epoch pins"

    # Per stage, and EQUAL, not merely a subset. A subset check let the map go stale in the other
    # direction: dropping "fix" from STAGE_PROMPTS["build_and_test"] left every test green while a
    # changed fix.md was admitted silently. What a stage renders and what the epoch pins for it are
    # the same set, or one of them is wrong.
    workgraph = rendered["work_stage"].get("_node_prompt", set())
    for stage, names in rendered["stages"].items():
        if stage in accepted_inputs.STAGE_PROMPTS:
            declared = set(accepted_inputs.STAGE_PROMPTS[stage])
            expected = names | (workgraph if stage == "build_and_test" else set())
            assert expected == declared, f"{stage} renders {sorted(expected)} but the epoch pins {sorted(declared)}"


def test_every_packaged_template_is_declared_referenced_or_maintenance_only() -> None:
    """A template added to ``prompts/`` must be placed deliberately: named by the stage that
    renders it, or declared unreachable from a line. Silence is what #2098 was."""
    packaged = {path.stem for path in agent.PROMPTS_DIR.glob("*.md")}
    declared = set().union(*accepted_inputs.STAGE_PROMPTS.values()) | accepted_inputs.UNREFERENCED_PROMPTS
    assert packaged == declared


def test_every_blueprint_stage_declares_which_templates_it_renders() -> None:
    from swfactory.blueprint import CANONICAL_ORDER

    assert set(accepted_inputs.STAGE_PROMPTS) == set(CANONICAL_ORDER)


def test_the_packaged_review_policy_is_part_of_the_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The review prompt interpolates REVIEW.md. When the target ships none it comes from swfactory's
    own packaged copy, which differs between builds exactly the way a template does -- and two trees
    differing only there admitted the same digest while rendering different review instructions."""
    from swfactory.config import Config

    cfg = Config(issue="demo/issue.md", run_id=RUN_ID)
    issue = Issue(id="DEMO-1", title="t", body="")
    root_a, root_b = tmp_path / "a", tmp_path / "b"
    for root, text in ((root_a, "# Review policy A\n"), (root_b, "# Review policy B\n")):
        root.mkdir()
        (root / "REVIEW.md").write_text(text, encoding="utf-8")

    monkeypatch.setattr(accepted_inputs, "FACTORY_ROOT", root_a)
    a = accepted_inputs.snapshot(cfg, None, issue)
    monkeypatch.setattr(accepted_inputs, "FACTORY_ROOT", root_b)
    b = accepted_inputs.snapshot(cfg, None, issue)

    assert a.review_policy_sha256 and b.review_policy_sha256 and a.digest != b.digest
    described = ", ".join(accepted_inputs.describe_mismatch(a, b))
    assert "packaged review policy" in described
    assert "effective policy" not in described, "one change must be reported once, by its name"


V1_PIN = (
    '{"schema_version": 1, "cell_id": null, "cell_epoch": null, "issue_id": "DEMO-1", '
    '"issue_sha256": "' + "a" * 64 + '", "blueprint": "factory", "blueprint_sha256": "' + "b" * 64 + '", '
    '"policy_sha256": "' + "c" * 64 + '", "repo": "zozo123/ariflow-swfactory", "target_dir": "demo/target", '
    '"base_branch": "main"}\n'
)


def test_a_pin_from_the_previous_build_is_refused_with_the_upgrade_story(tmp_path: Path) -> None:
    """Schema 1 pinned no templates. Read under schema 2 it must parse, be described truthfully, and
    refuse -- not be rejected as malformed (which reads as 'no pin' one layer up), and not be
    described as "prompts/build.md is newly referenced" (it was always rendered, just not pinned)."""
    from swfactory.config import Config
    from swfactory.state import RunState

    state = RunState(tmp_path / "run")
    state.write_control(accepted_inputs.PIN_FILE, V1_PIN)
    pinned = accepted_inputs.stored(state)
    assert pinned is not None and pinned.schema_version == 1

    cfg = Config(issue="demo/issue.md", run_id=RUN_ID)
    with pytest.raises(StageError) as caught:
        accepted_inputs.admit(state, accepted_inputs.snapshot(cfg, None, Issue(id="DEMO-1", title="t", body="")))
    message = str(caught.value)
    assert "earlier swfactory build" in message and "schema 1" in message
    assert "newly referenced" not in message
    assert "new epoch" in message or "reaccept" in message


def test_the_digest_a_receipt_quoted_is_the_digest_read_back(tmp_path: Path) -> None:
    """The digest is persisted at admission and read back verbatim. Recomputing it from the stored
    model changed under the schema bump, so receipts quoted values the code could not reproduce."""
    from swfactory.config import Config
    from swfactory.state import RunState

    state = RunState(tmp_path / "run")
    cfg = Config(issue="demo/issue.md", run_id=RUN_ID)
    accepted_inputs.admit(state, accepted_inputs.snapshot(cfg, None, Issue(id="DEMO-1", title="t", body="")))
    written = state.read_control(accepted_inputs.DIGEST_FILE).strip()
    assert accepted_inputs.digest_of(state) == written
    # A later build that adds a field would recompute differently; the sidecar is what is quoted.
    state.write_control(accepted_inputs.DIGEST_FILE, "inputs:" + "f" * 64 + "\n")
    assert accepted_inputs.digest_of(state) == "inputs:" + "f" * 64


def test_a_model_or_tool_surface_change_is_an_input_change(monkeypatch: pytest.MonkeyPatch) -> None:
    """`agent.POLICIES` is the model, tool surface and timeout the agent is called with. Unpinned,
    a model swap between two tasks of one epoch admitted the same digest."""
    from dataclasses import replace

    from swfactory import agent
    from swfactory.config import Config

    cfg = Config(issue="demo/issue.md", run_id=RUN_ID)
    issue = Issue(id="DEMO-1", title="t", body="")
    before = accepted_inputs.snapshot(cfg, None, issue)
    monkeypatch.setitem(agent.POLICIES, "build", replace(agent.POLICIES["build"], model="some-other-model"))
    after = accepted_inputs.snapshot(cfg, None, issue)
    assert before.digest != after.digest, "a different model for the build stage must not admit as the same inputs"
