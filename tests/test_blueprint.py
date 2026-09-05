"""Blueprint schema, loader, fan-out and Config mapping. Hermetic: files under tmp_path only."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from swfactory import blueprint as bp_mod
from swfactory import stages
from swfactory.agent import POLICIES
from swfactory.blueprint import CANONICAL_ORDER, Blueprint, load, loads
from swfactory.stages import Ctx, Gate

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOML = (ROOT / "blueprints" / "default.toml").read_text(encoding="utf-8")


def _data(**changes: Any) -> dict[str, Any]:
    """A valid blueprint as the flattened dict ``Blueprint`` validates; ``changes`` override."""
    base: dict[str, Any] = {
        "name": "line",
        "targets": [{"repo": "o/a", "dir": "demo/target"}, {"repo": "o/b"}],
        "order": list(CANONICAL_ORDER),
        "gates": [
            {"after": "intent", "artifact": "intent.md", "timeout_h": 2},
            {"after": "plan", "artifact": "plan.md", "timeout_h": 6},
        ],
        "sandbox": {"kind": "local", "ttl_s": 7 * 3600},
    }
    base.update(changes)
    return base


def _names(items: tuple) -> list[object]:
    return [i if isinstance(i, Gate) else i.__name__ for i in items]


# ---------------------------------------------------------------- the shipped files


@pytest.mark.parametrize("path", bp_mod.blueprint_paths(), ids=lambda p: p.stem)
def test_every_shipped_blueprint_loads(path: Path) -> None:
    bp = load(str(path))
    assert bp.name == ("factory" if path.stem == "default" else path.stem)
    assert bp.order[0] == "intent" and bp.order[-1] == "deliver"
    assert bp.targets and bp.sandbox.ttl_s > bp.gate_timeout_h * 3600


def test_default_blueprint_is_v1_pipeline() -> None:
    bp = load("factory")
    assert bp.name == "factory" and bp.sandbox.kind == "islo"
    assert _names(bp.pipeline()) == _names(stages.PIPELINE)
    assert bp.labels == ["factory", "agent-authored"] and bp.review.nit_cap == 3
    assert bp.gate_timeout_h == 24 and bp.limits.budget_usd == 8.0


def test_hotfix_pipeline_has_no_spec_and_gates_after_intent_and_plan() -> None:
    bp = load("hotfix")
    items = _names(bp.pipeline())
    assert "spec" not in items
    assert items == [
        "intent",
        Gate("intent", "intent.md", auto=True),  # gates[].auto reaches the CLI walk too
        "plan",
        Gate("plan", "plan.md", auto=False),
        "build_and_test",
        "review",
        "deliver",
    ]
    assert bp.gate_after("intent").auto is True and bp.gate_after("plan").timeout_h == 4
    assert bp.gate_after("build_and_test") is None
    assert bp.gate_timeout_h == 4


def test_toolset_line_runs_the_default_order_on_airflows_own_sandbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`blueprints/toolset.toml` must reach a real ``ToolsetSandbox``, not just the docs.

    The backend name is a runtime knob (``Config.toolset_backend``/``SWF_TOOLSET_BACKEND``), not a
    blueprint key, so the line is runnable against whichever ``SandboxBackend`` is installed; the
    import itself is stubbed here because the released provider is not a test dependency.
    """
    from swfactory import sandbox as sandbox_mod

    bp = load("toolset")
    assert bp.name != bp_mod.DEFAULT_BLUEPRINT  # experimental: never the default line
    assert [i for i in _names(bp.pipeline()) if not isinstance(i, Gate)] == list(CANONICAL_ORDER)
    assert bp.gate_after("intent").auto is True  # unattended runs finish on their own
    assert bp.gate_after("plan").auto is False and bp.gate_after("plan").timeout_h == 2
    assert bp.sandbox.kind == "toolset" and "toolset" in bp.labels
    assert len(bp.targets) == 1 and bp.targets[0].dir == "demo/target"

    job = bp.jobs({"issues": ["demo/issue.md"]})[0]
    cfg = bp.config(job, run_id="r1234567")
    assert cfg.sandbox == "toolset" and cfg.toolset_backend == "sbx"
    assert cfg.sandbox_ttl_s == 10_800 > cfg.gate_timeout_h * 3600
    assert bp.config(job, run_id="r1", agent="claude").sandbox == "toolset"  # a real boundary

    seen: list[str] = []
    monkeypatch.setattr(
        sandbox_mod, "load_toolset_backend", lambda name, **kw: seen.append(name) or object()
    )
    sb = sandbox_mod.make_sandbox(cfg, "DEMO-1")
    assert isinstance(sb, sandbox_mod.ToolsetSandbox)
    assert seen == ["sbx"]
    assert sb.repo_root == cfg.toolset_workdir
    assert sb.workdir == f"{cfg.toolset_workdir}/demo/target"


def test_cli_approver_honours_gate_auto_without_prompting(monkeypatch: pytest.MonkeyPatch) -> None:
    """`auto = true` on a gate approves under `--approve prompt` (actor "auto"), as the DAG does."""
    from swfactory.config import Config

    monkeypatch.setattr(stages.typer, "confirm", lambda *a, **k: pytest.fail("prompted"))
    ctx = _ctx(None)
    ctx.cfg = Config(issue="x", approve="prompt")
    approval = stages.cli_approver(Gate("intent", "intent.md", auto=True), ctx)
    assert (approval.gate, approval.decision, approval.actor) == ("intent", "approve", "auto")
    ctx.cfg = Config(issue="x", approve="auto")
    assert stages.cli_approver(Gate("plan", "plan.md"), ctx).actor == "auto"


def test_stages_registry_matches_canonical_order() -> None:
    assert tuple(stages.STAGES) == CANONICAL_ORDER == stages.CANONICAL_ORDER
    assert all(stages.STAGES[n].__name__ == n for n in CANONICAL_ORDER)


# ---------------------------------------------------------------- loading


def test_load_resolves_cwd_then_factory_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = tmp_path / "blueprints"
    local.mkdir()
    (local / "mine.toml").write_text(DEFAULT_TOML.replace('name = "factory"', 'name = "mine"'))
    monkeypatch.chdir(tmp_path)
    assert load("mine").name == "mine"
    assert load("hotfix").name == "hotfix"  # falls back to the factory checkout
    assert load(str(local / "mine.toml")).name == "mine"
    with pytest.raises(FileNotFoundError):
        load("nope")
    with pytest.raises(FileNotFoundError):
        load(str(tmp_path / "missing.toml"))


def test_load_rejects_name_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    local = tmp_path / "blueprints"
    local.mkdir()
    (local / "other.toml").write_text(DEFAULT_TOML)  # name = "factory"
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="expected 'other'"):
        load("other")


def test_loads_rejects_unknown_section_and_bad_toml() -> None:
    with pytest.raises(ValueError, match="unknown blueprint sections \\['limit'\\]"):
        loads(DEFAULT_TOML.replace("[limits]", "[limit]"))
    with pytest.raises(ValueError):
        loads("[blueprint\nname = 'x'")


# ---------------------------------------------------------------- validators


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"gates": [{"after": "verify", "artifact": "x.md"}]}, "not in stages.order"),
        ({"order": ["intent", "spec", "review"], "gates": []}, "end with 'deliver'"),
        ({"order": ["spec", "plan", "deliver"], "gates": []}, "start with 'intent'"),
        ({"order": ["intent", "plan", "spec", "deliver"]}, "subsequence"),
        ({"order": ["intent", "intent", "deliver"]}, "subsequence"),
        ({"order": ["intent", "ship"]}, "unknown stages"),
        ({"order": []}, "must not be empty"),
        ({"sandbox": {"kind": "local", "ttl_s": 6 * 3600}}, "ttl_s"),
        ({"gates": [{"after": "spec", "artifact": "spec.md"}]}, "gates may only follow"),
        (
            {
                "gates": [
                    {"after": "intent", "artifact": "a"},
                    {"after": "intent", "artifact": "b"},
                ]
            },
            "more than one gate",
        ),
        ({"limits": {"budget_usd_per_stage": 9.0, "budget_usd": 8.0}}, "budget_usd_per_stage"),
        ({"policy": {"deploy": {}}}, "unknown stages"),
        ({"policy": {"build": {"disallowed_tools": []}}}, "disallowed_tools"),
        ({"policy": {"build": {"writes": False}}}, "writes"),
        ({"policy": {"build": {"extra_allowed_tools": ["Bash(npm test*)"]}}}, "shell"),
        ({"policy": {"plan": {"extra_allowed_tools": ["Write"]}}}, "read-only"),
        ({"name": "-bad"}, "pattern"),
        ({"name": "x" * 64}, "pattern"),
        ({"targets": []}, "at least 1"),
        ({"trigger": {"kind": "cron"}}, "requires trigger.cron"),
        ({"bogus": 1}, "bogus"),
    ],
)
def test_validation_errors(changes: dict[str, Any], match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        Blueprint.model_validate(_data(**changes))


def test_policy_override_via_toml_is_additive_only() -> None:
    text = DEFAULT_TOML.replace(
        "extra_allowed_tools = []", 'extra_allowed_tools = ["NotebookEdit"]'
    )
    bp = loads(text)
    assert bp.policy["build"].extra_allowed_tools == ["NotebookEdit"]
    with pytest.raises(ValueError, match="disallowed_tools"):
        loads(text + "\n[policy.fix]\ndisallowed_tools = []\n")


def test_valid_variants() -> None:
    assert Blueprint.model_validate(_data(gates=[])).gate_timeout_h == 0
    bp = Blueprint.model_validate(_data(trigger={"kind": "cron", "cron": "0 6 * * 1"}))
    assert bp.trigger.cron == "0 6 * * 1"
    assert Blueprint.model_validate(_data(order=["intent", "deliver"], gates=[])).order == [
        "intent",
        "deliver",
    ]


# ---------------------------------------------------------------- jobs


def test_jobs_is_issues_times_targets() -> None:
    bp = Blueprint.model_validate(_data())
    jobs = bp.jobs({"issues": ["42", 43]})
    assert [(j["issue"], j["repo"], j["dir"], j["base_branch"], j["job_idx"]) for j in jobs] == [
        ("42", "o/a", "demo/target", "main", 0),
        ("42", "o/b", "", "main", 1),
        ("43", "o/a", "demo/target", "main", 2),
        ("43", "o/b", "", "main", 3),
    ]
    assert set(jobs[0]) == {"issue", "repo", "dir", "base_branch", "job_idx"}


def test_jobs_compat_and_filter() -> None:
    bp = Blueprint.model_validate(_data())
    assert [j["issue"] for j in bp.jobs({"issue": 7})] == ["7", "7"]
    # The UI params form sends `issues` as its default [] next to a filled `issue`.
    assert [j["issue"] for j in bp.jobs({"issues": [], "issue": "42"})] == ["42", "42"]
    only_b = bp.jobs({"issues": ["1"], "targets": ["o/b"]})
    assert [(j["repo"], j["job_idx"]) for j in only_b] == [("o/b", 0)]
    with pytest.raises(ValueError, match="not in blueprint"):
        bp.jobs({"issues": ["1"], "targets": ["o/zzz"]})
    for conf in ({}, None, {"issues": []}, {"issue": ""}):
        with pytest.raises(ValueError, match="issues"):
            bp.jobs(conf)


# ---------------------------------------------------------------- config


def test_config_maps_limits_sandbox_and_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWF_MAX_TURNS", raising=False)
    bp = loads(
        DEFAULT_TOML.replace("max_turns = 40", "max_turns = 55")
        .replace("# snapshot = ", "snapshot = ")
        .replace('kind = "islo"', 'kind = "srt"')
    )
    job = bp.jobs({"issues": ["demo/issue.md"]})[0]
    cfg = bp.config(job, run_id="r1234567")
    assert cfg.issue == "demo/issue.md" and cfg.run_id == "r1234567"
    assert (cfg.repo, cfg.target_dir, cfg.base_branch) == (
        "zozo123/ariflow-swfactory",
        "demo/target",
        "main",
    )
    assert cfg.blueprint == "factory" and cfg.sandbox == "srt"
    assert cfg.gateway_profile == "swfactory" and cfg.islo_environment == "swfactory"
    assert cfg.sandbox_ttl_s == 172_800 and cfg.sandbox_idle_s == 900
    assert cfg.islo_snapshot == "swf-golden-20260902"
    assert cfg.max_build_iterations == 3 and cfg.max_review_fixes == 1 and cfg.max_turns == 55
    assert cfg.max_budget_usd_per_stage == 2.0 and cfg.max_budget_usd == 8.0
    assert cfg.stage_timeout_h == 3 and cfg.max_parallel_jobs == 4 and cfg.gate_timeout_h == 24


def test_config_overrides_then_env_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    bp = load("factory")
    job = bp.jobs({"issues": ["1"]})[0]
    cfg = bp.config(job, run_id="r1", sandbox="local", max_turns=12, agent=None)
    assert cfg.sandbox == "local" and cfg.max_turns == 12 and cfg.agent == "scripted"
    monkeypatch.setenv("SWF_MAX_TURNS", "99")
    monkeypatch.setenv("SWF_SANDBOX", "local")
    cfg = bp.config(job, run_id="r1", max_turns=12)
    assert cfg.max_turns == 99 and cfg.sandbox == "local"


def test_config_keeps_trust_boundary() -> None:
    bp = load("factory")
    job = bp.jobs({"issues": ["1"]})[0]
    with pytest.raises(ValueError, match="agent=claude requires"):
        bp.config(job, run_id="r1", agent="claude", sandbox="local")
    assert bp.config(job, run_id="r1", agent="claude", sandbox="srt").sandbox == "srt"


# ---------------------------------------------------------------- stage plumbing


def _ctx(bp: Blueprint | None) -> Ctx:
    return Ctx(cfg=None, sb=None, agent=None, scm=None, issue=None, run_dir=Path(), blueprint=bp)  # type: ignore[arg-type]


def test_policy_override_is_applied_additively() -> None:
    bp = Blueprint.model_validate(
        _data(policy={"build": {"extra_allowed_tools": ["NotebookEdit", "Read"], "model": "m1"}})
    )
    pol = stages._policy(_ctx(bp), "build")
    base = POLICIES["build"]
    assert pol.allowed_tools == base.allowed_tools + ("NotebookEdit",)
    assert pol.model == "m1" and pol.disallowed_tools == base.disallowed_tools and pol.writes
    assert stages._policy(_ctx(bp), "fix") is POLICIES["fix"]
    assert stages._policy(_ctx(None), "build") is base


def test_blueprint_knobs_default_to_v1_without_blueprint() -> None:
    assert stages._nit_cap(_ctx(None)) == stages.NIT_CAP == 3
    assert stages._nit_cap(_ctx(Blueprint.model_validate(_data(review={"nit_cap": 1})))) == 1
    assert stages._pipeline(_ctx(None)) is stages.PIPELINE
    assert _names(stages._pipeline(_ctx(load("hotfix")))) == _names(load("hotfix").pipeline())


def test_crabbox_command_downloads_unless_in_place() -> None:
    cmd = stages.crabbox_command("local-container", ".factory/junit.xml", "uv run pytest")
    assert "-artifact-glob" not in cmd
    assert "-download .factory/junit.xml=.factory/junit.xml" in cmd
    assert cmd.endswith("-- uv run pytest") and "-provider local-container" in cmd
    for provider in stages.IN_PLACE_PROVIDERS:
        cmd = stages.crabbox_command(provider, ".factory/junit.xml", "uv run pytest")
        assert "-download" not in cmd and "-artifact-glob" not in cmd
        assert f"-provider {provider} -junit .factory/junit.xml -ttl 45m" in cmd


def test_protected_for_frees_tests_dir_outside_fix():
    from swfactory.config import TargetContract, protected_for

    c = TargetContract(test="uv run pytest", protected=["factory.toml", "tests/"])
    assert protected_for(c, "build") == ["factory.toml"]
    assert protected_for(c, "plan") == ["factory.toml"]
    assert protected_for(c, "fix") == ["factory.toml", "tests"]
