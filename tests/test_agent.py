"""Agent seam: policies, claude argv/envelope parsing, scripted fixtures, and the guard hook."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from swfactory import agent as agent_mod
from swfactory.agent import (
    GUARD_DENY,
    GUARD_DST,
    GUARD_MATCHER,
    GUARD_PATH_DENY,
    POLICIES,
    SETTINGS_DST,
    ClaudeAgent,
    FixtureMissing,
    Policy,
    ScriptedAgent,
    guard_deny_rules,
    install_guard,
    make_agent,
    render_prompt,
)
from swfactory.config import Config
from swfactory.models import BuildSummary, Plan, Review, RunResult, StageError

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD = REPO_ROOT / ".claude" / "hooks" / "swf_guard.py"

# ------------------------------------------------ fakes (sandbox.py is deliberately not imported)


class FakeSandbox:
    """In-memory Sandbox: files in a dict, every run() recorded and answered by a table."""

    name = "fake"
    workdir = "/work"

    def __init__(self, files: dict[str, str] | None = None) -> None:
        self.files = dict(files or {})
        self.runs: list[str] = []
        self.answers: dict[str, RunResult] = {}

    def ensure(self) -> None: ...

    def close(self) -> None: ...

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        self.runs.append(cmd)
        for needle, res in self.answers.items():
            if needle in cmd:
                return res
        return RunResult(0, "", "", 0.0)

    def read(self, path: str) -> str:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def write(self, path: str, content: str) -> None:
        self.files[path] = content

    def exists(self, path: str) -> bool:
        return path in self.files


class GitSandbox:
    """Minimal real-filesystem sandbox over a tmp git repo (bash -lc semantics)."""

    name = "git"

    def __init__(self, workdir: Path) -> None:
        self.workdir = str(workdir)

    def ensure(self) -> None: ...

    def close(self) -> None: ...

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        p = subprocess.run(
            ["bash", "-lc", cmd], cwd=cwd or self.workdir, capture_output=True, text=True
        )
        return RunResult(p.returncode, p.stdout, p.stderr, 0.0)

    def read(self, path: str) -> str:
        return (Path(self.workdir) / path).read_text()

    def write(self, path: str, content: str) -> None:
        target = Path(self.workdir) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def exists(self, path: str) -> bool:
        return (Path(self.workdir) / path).exists()


def _git(repo: Path, *args: str) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True, env=env
    ).stdout


def _git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / "hello.txt").write_text("hello\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")
    return repo


def _claude_cfg(**kw) -> Config:
    return Config(issue="DEMO-1", agent="claude", sandbox="islo", **kw)


# ---------------------------------------------------------------- policies


def test_read_only_stages_have_no_write_or_shell_tools() -> None:
    for stage in ("spec", "plan"):
        tools = POLICIES[stage].allowed_tools
        assert not POLICIES[stage].writes
        assert not any(t.startswith(("Edit", "Write", "MultiEdit", "Bash")) for t in tools), stage


def test_non_write_policies_never_allow_edit_or_write() -> None:
    for stage, pol in POLICIES.items():
        if not pol.writes:
            assert not any(
                t.startswith(("Edit", "Write", "MultiEdit")) for t in pol.allowed_tools
            ), stage
        assert "WebFetch" in pol.disallowed_tools and "WebSearch" in pol.disallowed_tools


def test_write_stages_are_build_and_fix() -> None:
    assert {s for s, p in POLICIES.items() if p.writes} == {"build", "fix"}


# ---------------------------------------------------------------- claude argv


def test_claude_argv_has_required_flags_and_schema() -> None:
    cfg = _claude_cfg(max_turns=7, max_budget_usd_per_stage=1.5)
    cmd = ClaudeAgent().argv(
        prompt_path=".factory/prompt.plan.1.md",
        out_path=".factory/agent.plan.1.json",
        policy=POLICIES["plan"],
        schema=Plan,
        cfg=cfg,
    )
    assert cmd.startswith('claude -p "$(cat .factory/prompt.plan.1.md)"')
    for flag in (
        "--output-format json",
        "--permission-mode acceptEdits",
        "--allowedTools Read Grep Glob",
        "--disallowedTools WebFetch WebSearch",
        "--max-turns 7",
        "--max-budget-usd 1.5",
        "--no-session-persistence",
        "--json-schema",
        "> .factory/agent.plan.1.json",
    ):
        assert flag in cmd, flag
    assert '"files"' in cmd  # the Plan JSON schema is inlined
    assert "--dangerously-skip-permissions" not in cmd
    assert "--bare" not in cmd
    assert "--model" not in cmd


def test_claude_argv_without_schema_and_with_model() -> None:
    pol = Policy(("Read",), model="claude-sonnet-4-5")
    cmd = ClaudeAgent().argv(
        prompt_path="p.md", out_path="o.json", policy=pol, schema=None, cfg=_claude_cfg()
    )
    assert "--json-schema" not in cmd
    assert "--model claude-sonnet-4-5" in cmd
    assert "'Bash(uv run *)'" in ClaudeAgent().argv(
        prompt_path="p", out_path="o", policy=POLICIES["build"], schema=None, cfg=_claude_cfg()
    )


# ---------------------------------------------------------------- claude run


def test_claude_run_parses_envelope_and_writes_artifact() -> None:
    envelope = {
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "result": "long prose that must not be committed",
        "structured_output": {"verdict": "approve", "findings": []},
        "total_cost_usd": 0.42,
        "num_turns": 5,
        "duration_ms": 1234,
        "session_id": "sess-1",
    }
    sb = FakeSandbox({".factory/agent.review.1.json": json.dumps(envelope)})
    res = ClaudeAgent().run(
        sb,
        stage="review",
        iteration=1,
        prompt="review this",
        policy=POLICIES["review"],
        schema=Review,
        cfg=_claude_cfg(),
        issue_id="DEMO-1",
    )
    assert res.agent == "claude" and not res.is_error and res.subtype == "success"
    assert res.data == {"verdict": "approve", "findings": []}
    assert (res.cost_usd, res.num_turns, res.duration_ms, res.session_id) == (
        0.42,
        5,
        1234,
        "sess-1",
    )
    assert res.text.startswith("long prose")
    assert sb.files[".factory/prompt.review.1.md"] == "review this"
    stored = json.loads(sb.files["docs/factory/DEMO-1/agent/review.1.json"])
    assert "result" not in stored and stored["total_cost_usd"] == 0.42
    assert any(c.startswith("claude -p") for c in sb.runs)
    assert SETTINGS_DST not in sb.files  # read-only stage: no guard installed


def test_claude_run_write_stage_installs_guard_from_factory_toml() -> None:
    envelope = {
        "is_error": False,
        "subtype": "success",
        "result": '{"summary": "ok", "files_changed": []}',
    }
    sb = FakeSandbox(
        {
            ".factory/agent.build.1.json": json.dumps(envelope),
            "factory.toml": (
                '[commands]\ntest = "uv run pytest"\n[paths]\n'
                'protected = ["factory.toml", "tests/"]\n'
            ),
            ".factory/agent.fix.2.json": json.dumps(envelope),
        }
    )
    res = ClaudeAgent().run(
        sb,
        stage="build",
        iteration=1,
        prompt="build",
        policy=POLICIES["build"],
        schema=BuildSummary,
        cfg=_claude_cfg(),
        issue_id="DEMO-1",
    )
    assert res.data == {"summary": "ok", "files_changed": []}  # fallback: result text is JSON
    settings = json.loads(sb.files[SETTINGS_DST])
    # build may write tests (playbook: tests are protected during FIX tasks only)
    assert settings["env"]["SWF_PROTECTED"] == "factory.toml"
    assert sb.files[GUARD_DST] == GUARD.read_text()
    ClaudeAgent().run(
        sb,
        stage="fix",
        iteration=2,
        prompt="fix",
        policy=POLICIES["fix"],
        schema=BuildSummary,
        cfg=_claude_cfg(),
        issue_id="DEMO-1",
    )
    settings = json.loads(sb.files[SETTINGS_DST])
    assert settings["env"]["SWF_PROTECTED"] == "factory.toml:tests/"


def test_claude_run_extracts_fenced_json_when_structured_output_missing() -> None:
    plan = '{"files": ["a.py"], "steps": ["s"], "tests": ["t"], "risks": []}'
    text = f"Here is the plan:\n```json\n{plan}\n```\n"
    envelope = {"is_error": False, "subtype": "success", "result": text}
    sb = FakeSandbox({".factory/agent.plan.1.json": json.dumps(envelope)})
    res = ClaudeAgent().run(
        sb,
        stage="plan",
        iteration=1,
        prompt="p",
        policy=POLICIES["plan"],
        schema=Plan,
        cfg=_claude_cfg(),
        issue_id="DEMO-1",
    )
    assert not res.is_error and res.data["files"] == ["a.py"]


def test_claude_run_model_error_does_not_raise() -> None:
    envelope = {"is_error": True, "subtype": "error_max_turns", "result": "", "num_turns": 40}
    sb = FakeSandbox({".factory/agent.spec.1.json": json.dumps(envelope)})
    res = ClaudeAgent().run(
        sb,
        stage="spec",
        iteration=1,
        prompt="p",
        policy=POLICIES["spec"],
        schema=None,
        cfg=_claude_cfg(),
        issue_id="DEMO-1",
    )
    assert res.is_error and res.subtype == "error_max_turns" and res.num_turns == 40


def test_claude_run_schema_mismatch_is_error_not_exception() -> None:
    envelope = {
        "is_error": False,
        "subtype": "success",
        "result": "x",
        "structured_output": {"nope": 1},
    }
    sb = FakeSandbox({".factory/agent.plan.1.json": json.dumps(envelope)})
    res = ClaudeAgent().run(
        sb,
        stage="plan",
        iteration=1,
        prompt="p",
        policy=POLICIES["plan"],
        schema=Plan,
        cfg=_claude_cfg(),
        issue_id="DEMO-1",
    )
    assert res.is_error and res.subtype == "error_schema_validation"


def test_claude_run_unparseable_output_is_error() -> None:
    sb = FakeSandbox({".factory/agent.spec.1.json": "not json at all"})
    res = ClaudeAgent().run(
        sb,
        stage="spec",
        iteration=1,
        prompt="p",
        policy=POLICIES["spec"],
        schema=None,
        cfg=_claude_cfg(),
        issue_id="DEMO-1",
    )
    assert res.is_error and res.subtype == "error_unparseable_output"


def test_claude_run_missing_output_raises_sandbox_error() -> None:
    sb = FakeSandbox()
    with pytest.raises(StageError) as ei:
        ClaudeAgent().run(
            sb,
            stage="spec",
            iteration=1,
            prompt="p",
            policy=POLICIES["spec"],
            schema=None,
            cfg=_claude_cfg(),
            issue_id="DEMO-1",
        )
    assert ei.value.kind == "sandbox" and ei.value.retryable


def test_claude_run_records_fixtures(tmp_path: Path) -> None:
    envelope = {"is_error": False, "subtype": "success", "result": "# spec\n"}
    sb = FakeSandbox({".factory/agent.spec.1.json": json.dumps(envelope)})
    cfg = _claude_cfg(record_dir=str(tmp_path / "rec"))
    ClaudeAgent().run(
        sb,
        stage="spec",
        iteration=1,
        prompt="p",
        policy=POLICIES["spec"],
        schema=None,
        cfg=cfg,
        issue_id="DEMO-1",
    )
    assert (tmp_path / "rec" / "spec.1.md").read_text() == "# spec\n"
    assert f"run_id={cfg.run_id}" in (tmp_path / "rec" / "RECORDING").read_text()


# ---------------------------------------------------------------- install_guard


def test_install_guard_is_idempotent_and_excludes_files() -> None:
    sb = FakeSandbox({".git/info/exclude": "# git ls-files --others --exclude-from=...\n"})
    sb.answers["rev-parse --show-cdup"] = RunResult(0, "\n\n", "", 0.0)
    install_guard(sb, ["tests/", "factory.toml"])
    install_guard(sb, ["tests/", "factory.toml"])
    settings = json.loads(sb.files[SETTINGS_DST])
    assert settings["env"]["SWF_PROTECTED"] == "tests/:factory.toml"
    assert settings["hooks"]["PreToolUse"][0]["matcher"] == GUARD_MATCHER
    assert "Bash(git push*)" in settings["permissions"]["deny"]
    exclude = sb.files[".git/info/exclude"].splitlines()
    assert exclude.count(SETTINGS_DST) == 1 and exclude.count(GUARD_DST) == 1
    assert exclude[0].startswith("# git ls-files")


def test_install_guard_native_deny_rules_and_matcher() -> None:
    sb = FakeSandbox()
    install_guard(sb, ["tests/", "src/calc/", "bands.yaml"])
    settings = json.loads(sb.files[SETTINGS_DST])
    matcher = settings["hooks"]["PreToolUse"][0]["matcher"]
    assert set(matcher.split("|")) == {"Edit", "Write", "MultiEdit", "NotebookEdit", "Bash"}
    hook = settings["hooks"]["PreToolUse"][0]["hooks"][0]
    assert hook == {"type": "command", "command": f"python3 {GUARD_DST}"}
    deny = settings["permissions"]["deny"]
    for rule in GUARD_DENY + GUARD_PATH_DENY:
        assert rule in deny, rule
    for rule in (
        "Edit(REVIEW.md)",
        "Edit(bands.yaml)",
        "Edit(factory.toml)",
        "Edit(.claude/**)",
        "Edit(.github/**)",
        "Read(.claude/hooks/**)",
        "Write(REVIEW.md)",
        "Write(bands.yaml)",
        "Write(factory.toml)",
        "Edit(tests)",
        "Edit(tests/**)",
        "Write(tests)",
        "Write(tests/**)",
        "Edit(src/calc)",
        "Edit(src/calc/**)",
    ):
        assert rule in deny, rule
    assert "Edit(tests/)" not in deny  # trailing slash normalised
    assert len(deny) == len(set(deny))  # bands.yaml protected twice -> one rule
    assert deny[: len(GUARD_DENY)] == list(GUARD_DENY)  # Bash rules keep their place
    assert settings["env"]["SWF_PROTECTED"] == "tests/:src/calc/:bands.yaml"


def test_guard_deny_rules_skip_empty_entries() -> None:
    rules = guard_deny_rules(["", "/", " docs/ "])
    assert rules[len(GUARD_DENY) + len(GUARD_PATH_DENY) :] == [
        "Edit(docs)",
        "Edit(docs/**)",
        "Write(docs)",
        "Write(docs/**)",
    ]


def test_install_guard_writes_files_before_probing_sandbox() -> None:
    """Under srt `.claude/` is read-only for the sandboxed shell: files land first, via write()."""
    sb = FakeSandbox()
    install_guard(sb, [])
    assert sb.runs and "mkdir -p .claude/hooks" in sb.runs[0]
    assert SETTINGS_DST in sb.files and GUARD_DST in sb.files
    assert sb.files[GUARD_DST] == GUARD.read_text()


def test_install_guard_in_subdir_uses_repo_root_exclude() -> None:
    sb = FakeSandbox()
    sb.answers["rev-parse --show-cdup"] = RunResult(0, "../../\ndemo/target/\n", "", 0.0)
    install_guard(sb, [])
    exclude = sb.files["../../.git/info/exclude"].splitlines()
    assert exclude == [f"demo/target/{SETTINGS_DST}", f"demo/target/{GUARD_DST}"]


# ---------------------------------------------------------------- scripted


def test_scripted_fixture_lookup_order(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    (b / "build.1.patch").write_text("")
    (a / "build.md").write_text("")
    ag = ScriptedAgent([a, b])
    assert ag.fixture("build", 1) == a / "build.md"  # first dir wins even over a specific name
    (a / "build.1.json").write_text("{}")
    assert ag.fixture("build", 1) == a / "build.1.json"  # iteration-specific beats generic
    (a / "build.1.patch").write_text("")
    assert ag.fixture("build", 1) == a / "build.1.patch"  # patch beats json
    assert ag.fixture("build", 2) == a / "build.md"
    with pytest.raises(FixtureMissing) as ei:
        ag.fixture("review", 1)
    assert isinstance(ei.value, StageError) and ei.value.kind == "agent"


def test_scripted_md_and_json_fixtures(tmp_path: Path) -> None:
    (tmp_path / "spec.md").write_text("# spec\n")
    (tmp_path / "plan.json").write_text(
        json.dumps({"files": ["src/x.py"], "steps": ["a"], "tests": ["t"], "risks": []})
    )
    (tmp_path / "review.1.json").write_text(json.dumps({"verdict": "maybe"}))
    ag = ScriptedAgent([tmp_path])
    sb = FakeSandbox()
    cfg = Config(issue="DEMO-1")
    common = {"sb": sb, "prompt": "p", "cfg": cfg, "issue_id": "DEMO-1"}

    spec = ag.run(stage="spec", iteration=1, policy=POLICIES["spec"], schema=None, **common)
    assert spec.agent == "scripted" and spec.text == "# spec\n" and spec.cost_usd == 0

    plan = ag.run(stage="plan", iteration=1, policy=POLICIES["plan"], schema=Plan, **common)
    assert plan.data["files"] == ["src/x.py"] and not plan.is_error

    review = ag.run(stage="review", iteration=1, policy=POLICIES["review"], schema=Review, **common)
    assert review.is_error and review.subtype == "error_schema_validation"

    stored = json.loads(sb.files["docs/factory/DEMO-1/agent/plan.1.json"])
    assert stored["agent"] == "scripted" and stored["fixture"].endswith("plan.json")
    assert sb.files[".factory/prompt.spec.1.md"] == "p"


def test_scripted_patch_applies_to_git_repo(tmp_path: Path) -> None:
    repo = _git_repo(tmp_path)
    (repo / "hello.txt").write_text("hello\nworld\n")
    (repo / "new.py").write_text("x = 1\n")
    _git(repo, "add", "-N", "new.py")
    diff = _git(repo, "diff", "HEAD")
    _git(repo, "reset", "-q", "--hard", "HEAD")
    _git(repo, "clean", "-fdq")
    assert not (repo / "new.py").exists()

    fx = tmp_path / "fx"
    fx.mkdir()
    (fx / "build.1.patch").write_text("# recorded by swfactory run abc\n" + diff)

    res = ScriptedAgent([fx]).run(
        GitSandbox(repo),
        stage="build",
        iteration=1,
        prompt="p",
        policy=POLICIES["build"],
        schema=BuildSummary,
        cfg=Config(issue="DEMO-1"),
        issue_id="DEMO-1",
    )
    assert not res.is_error
    assert (repo / "hello.txt").read_text() == "hello\nworld\n"
    assert (repo / "new.py").read_text() == "x = 1\n"
    assert res.data == {
        "summary": "applied scripted fixture build.1.patch",
        "files_changed": ["hello.txt", "new.py"],
    }
    assert (repo / "docs/factory/DEMO-1/agent/build.1.json").exists()


def test_scripted_patch_failure_is_error_and_read_only_stage_is_policy_error(
    tmp_path: Path,
) -> None:
    repo = _git_repo(tmp_path)
    fx = tmp_path / "fx"
    fx.mkdir()
    (fx / "fix.1.patch").write_text("this is not a patch\n")
    args = {"prompt": "p", "cfg": Config(issue="DEMO-1"), "issue_id": "DEMO-1"}
    res = ScriptedAgent([fx]).run(
        GitSandbox(repo), stage="fix", iteration=1, policy=POLICIES["fix"], schema=None, **args
    )
    assert res.is_error and res.subtype == "error_patch_apply"
    with pytest.raises(StageError) as ei:
        ScriptedAgent([fx]).run(
            FakeSandbox(), stage="fix", iteration=1, policy=POLICIES["review"], schema=None, **args
        )
    assert ei.value.kind == "policy"


def test_make_agent() -> None:
    assert isinstance(make_agent(Config(issue="x")), ScriptedAgent)
    assert make_agent(Config(issue="x", fixtures_dir="d")).fixtures_dirs == [Path("d")]
    assert isinstance(make_agent(_claude_cfg()), ClaudeAgent)
    assert isinstance(make_agent(Config(issue="x")), agent_mod.Agent)


# ---------------------------------------------------------------- prompts


def test_render_prompt_all_stages_and_missing_vars() -> None:
    for stage in ("spec", "plan", "build", "fix", "review", "diagnose"):
        out = render_prompt(stage, issue_id="DEMO-1")
        assert "{" not in out.replace("{}", ""), stage  # every placeholder resolved
    out = render_prompt("fix", issue_id="DEMO-1", plan="P", failures=None)
    assert "DEMO-1" in out and "P" in out and "(none)" in out
    assert "REVIEW POLICY TEXT" in render_prompt("review", review_policy="REVIEW POLICY TEXT")


# ---------------------------------------------------------------- swf_guard hook


def _hook(tmp_path: Path, payload: dict, **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(GUARD)],
        input=json.dumps(payload),
        cwd=tmp_path,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
    )


def test_guard_denies_protected_paths_and_allows_source(tmp_path: Path) -> None:
    env = {"SWF_PROTECTED": "tests/"}
    denied = _hook(
        tmp_path,
        {"tool_name": "Edit", "tool_input": {"file_path": str(tmp_path / "tests/test_a.py")}},
        **env,
    )
    assert denied.returncode == 2 and "protected" in denied.stderr
    rel = _hook(
        tmp_path, {"tool_name": "Write", "tool_input": {"file_path": "tests/test_b.py"}}, **env
    )
    assert rel.returncode == 2
    allowed = _hook(
        tmp_path,
        {"tool_name": "Edit", "tool_input": {"file_path": str(tmp_path / "src/calc/x.py")}},
        **env,
    )
    assert allowed.returncode == 0 and allowed.stderr == ""
    fixed = _hook(
        tmp_path,
        {"tool_name": "MultiEdit", "tool_input": {"file_path": "REVIEW.md"}},
        SWF_PROTECTED="",
    )
    assert fixed.returncode == 2
    dotclaude = _hook(
        tmp_path,
        {"tool_name": "Write", "tool_input": {"file_path": ".claude/settings.local.json"}},
        SWF_PROTECTED="",
    )
    assert dotclaude.returncode == 2
    lines = (tmp_path / ".factory/hooks.jsonl").read_text().splitlines()
    log = [json.loads(line) for line in lines]
    assert [e["decision"] for e in log] == ["deny", "deny", "allow", "deny", "deny"]
    assert set(log[0]) == {"ts", "tool", "path_or_cmd", "decision"}


def test_guard_bash_denylist(tmp_path: Path) -> None:
    for cmd in (
        "git push origin HEAD",
        "gh pr create",
        "git commit -am x",
        "curl https://x",
        "wget https://x",
    ):
        r = _hook(tmp_path, {"tool_name": "Bash", "tool_input": {"command": cmd}})
        assert r.returncode == 2 and "denylist" in r.stderr, cmd
    for cmd in ("uv run pytest", "git diff", "git status"):
        assert (
            _hook(tmp_path, {"tool_name": "Bash", "tool_input": {"command": cmd}}).returncode == 0
        ), cmd
    assert (
        _hook(tmp_path, {"tool_name": "Read", "tool_input": {"file_path": "tests/x"}}).returncode
        == 0
    )


def test_guard_notebook_edit_uses_notebook_path(tmp_path: Path) -> None:
    env = {"SWF_PROTECTED": "tests/"}
    denied = _hook(
        tmp_path,
        {"tool_name": "NotebookEdit", "tool_input": {"notebook_path": "tests/nb.ipynb"}},
        **env,
    )
    assert denied.returncode == 2 and "protected" in denied.stderr
    allowed = _hook(
        tmp_path,
        {"tool_name": "NotebookEdit", "tool_input": {"notebook_path": "notebooks/nb.ipynb"}},
        **env,
    )
    assert allowed.returncode == 0
