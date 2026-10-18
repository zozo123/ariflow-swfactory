"""Per-stage ``max_turns``: a blueprint's ``[policy.<stage>] max_turns`` reaches one call, and only
that call, through ``stages._policy`` and ``stages._agent``.

The run-wide ``limits.max_turns`` used to be the only turn knob, so a draft-style build budget
(VOLO's one-shot draft: build with a handful of turns while review keeps its 40) could not be
expressed. The override is tighten-only: the blueprint refuses a value above the limit, and the
call takes the minimum with ``Config.max_turns`` even when the run's own limit was lowered after
the blueprint loaded (an ``SWF_MAX_TURNS`` or CLI override).

Hermetic: an in-memory sandbox and a recording agent; no provider.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from swfactory.agent import ClaudeAgent
from swfactory.blueprint import CANONICAL_ORDER, Blueprint
from swfactory.config import Config
from swfactory.models import AgentResult, BuildSummary, Issue, Review, RunResult
from swfactory.stages import Ctx, _agent

FACTORY_TOML = '[commands]\ntest = "pytest"\n[paths]\nsource = "src"\ntests = "tests"\n'


class MemSandbox:
    name = "mem"
    workdir = "/work"

    def __init__(self) -> None:
        self.files = {"factory.toml": FACTORY_TOML}

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        del cmd, cwd, timeout_s
        return RunResult(0, "", "", 0.0)

    def read(self, path: str) -> str:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def write(self, path: str, content: str) -> None:
        self.files[path] = content

    def exists(self, path: str) -> bool:
        return path in self.files


class RecordingAgent:
    """Records the cfg and policy each call received, and the ``claude -p`` line they would build."""

    kind = "scripted"

    def __init__(self) -> None:
        self.calls: dict[str, dict[str, Any]] = {}

    def run(self, sb: Any, *, stage: str, cfg: Config, policy: Any, schema: Any, **kw: Any) -> AgentResult:
        del sb, kw
        argv = ClaudeAgent().argv(prompt_path="p.md", out_path="o.json", policy=policy, schema=schema, cfg=cfg)
        self.calls[stage] = {"cfg_max_turns": cfg.max_turns, "argv": argv}
        data = {"summary": "s", "files_changed": []} if schema is BuildSummary else {"verdict": "approve"}
        return AgentResult(agent="scripted", data=data, cost_usd=0.0, num_turns=2)


def _blueprint(**policy: Any) -> Blueprint:
    return Blueprint.model_validate(
        {
            "name": "line",
            "targets": [{"repo": "o/a", "dir": "demo/target"}],
            "order": list(CANONICAL_ORDER),
            "limits": {"max_turns": 40},
            "policy": policy,
            "sandbox": {"kind": "local"},
        }
    )


def _ctx(tmp_path: Path, blueprint: Blueprint, *, max_turns: int = 40) -> tuple[Ctx, RecordingAgent]:
    agent = RecordingAgent()
    ctx = Ctx(
        cfg=Config(issue="x", run_id="turn0001", max_turns=max_turns),
        sb=MemSandbox(),  # type: ignore[arg-type]
        agent=agent,  # type: ignore[arg-type]
        scm=None,  # type: ignore[arg-type]
        issue=Issue(id="T-1", title="t", body="b"),
        run_dir=tmp_path / "run",
        blueprint=blueprint,
    )
    return ctx, agent


def test_the_build_call_runs_under_its_stage_cap_while_review_keeps_the_run_limit(tmp_path: Path) -> None:
    ctx, agent = _ctx(tmp_path, _blueprint(build={"max_turns": 3}))
    _agent(ctx, "build", 1, "prompt", BuildSummary)
    _agent(ctx, "review", 1, "prompt", Review)

    assert agent.calls["build"]["cfg_max_turns"] == 3
    assert "--max-turns 3 " in agent.calls["build"]["argv"]
    assert agent.calls["review"]["cfg_max_turns"] == 40
    assert "--max-turns 40 " in agent.calls["review"]["argv"]
    assert ctx.cfg.max_turns == 40, "the run's own Config is never mutated by one call's cap"


def test_a_run_limit_lowered_after_load_still_wins_over_the_stage_cap(tmp_path: Path) -> None:
    """The blueprint validated 30 <= 40, but the worker runs with max_turns=10: the call gets 10."""
    ctx, agent = _ctx(tmp_path, _blueprint(build={"max_turns": 30}), max_turns=10)
    _agent(ctx, "build", 1, "prompt", BuildSummary)
    assert agent.calls["build"]["cfg_max_turns"] == 10
    assert "--max-turns 10 " in agent.calls["build"]["argv"]
