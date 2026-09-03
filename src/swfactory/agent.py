"""Agent seam: who does a stage's work inside the sandbox.

Two implementations share one contract (``Agent``): ``ClaudeAgent`` runs ``claude -p`` in the
sandbox and reads its JSON envelope back; ``ScriptedAgent`` replays recorded fixtures so the
whole pipeline is testable without a model. Per-stage tool ``Policy`` and ``install_guard`` make
the write stages deterministic-safe: the hook, not the prompt, is the gate.
"""

from __future__ import annotations

import json
import re
import shlex
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydantic import BaseModel, ValidationError

from swfactory.config import FACTORY_ROOT, Config, load_target_contract, protected_for
from swfactory.models import AgentKind, AgentResult, StageError

if TYPE_CHECKING:
    from swfactory.sandbox import Sandbox

PROMPTS_DIR = Path(__file__).parent / "prompts"
GUARD_SRC = FACTORY_ROOT / ".claude" / "hooks" / "swf_guard.py"
GUARD_DST = ".claude/hooks/swf_guard.py"
SETTINGS_DST = ".claude/settings.local.json"
# The factory's stage skill (spec/plan shape, review contract): copied into every target so the
# CLAUDE.md -> skills -> hooks layering holds when the agent's cwd is not this repo.
SKILL_SRC = FACTORY_ROOT / ".claude" / "skills" / "swfactory"
SKILL_DST = ".claude/skills/swfactory"
GUARD_DENY = (
    "Bash(git push*)",
    "Bash(gh pr *)",
    "Bash(git commit*)",
    "Bash(curl *)",
    "Bash(wget *)",
    "Read(.env*)",
)
# Native Claude Code path rules: the PRIMARY gate (checked before hooks, not bypassable by hook
# output). The python hook below is defense-in-depth plus the audit log. Edit(...) rules cover
# Edit/Write/MultiEdit/NotebookEdit; the Write(...) twins are belt-and-braces. ``docs/factory/**``
# (the artifact chain) and ``.factory/**`` (stage scratch, hook log) are the orchestrator's:
# the agent must not be able to forge review.json / approvals.json / plan.json or its own audit.
GUARD_PATH_DENY = (
    "Edit(REVIEW.md)",
    "Edit(bands.yaml)",
    "Edit(factory.toml)",
    "Edit(.claude/**)",
    "Edit(.github/**)",
    "Edit(docs/factory/**)",
    "Edit(.factory/**)",
    "Read(.claude/hooks/**)",
    "Write(REVIEW.md)",
    "Write(bands.yaml)",
    "Write(factory.toml)",
    "Write(.claude/**)",
    "Write(.github/**)",
    "Write(docs/factory/**)",
    "Write(.factory/**)",
)
GUARD_MATCHER = "Edit|Write|MultiEdit|NotebookEdit|Bash"
_FIXTURE_EXTS = ("patch", "json", "md")
_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
_DIFF_FILE_RE = re.compile(r"^diff --git a/\S+ b/(\S+)$", re.MULTILINE)


# ---------------------------------------------------------------- policy


@dataclass(frozen=True)
class Policy:
    """Tool surface and limits for one stage's agent call."""

    allowed_tools: tuple[str, ...]
    disallowed_tools: tuple[str, ...] = ("WebFetch", "WebSearch")
    writes: bool = False  # True => install_guard() before the call
    timeout_s: int = 1800
    model: str | None = None


READ_ONLY: tuple[str, ...] = ("Read", "Grep", "Glob")
_WRITE_TOOLS: tuple[str, ...] = ("Edit", "Write")

POLICIES: dict[str, Policy] = {
    "spec": Policy(READ_ONLY),
    "plan": Policy(READ_ONLY),
    "build": Policy(
        READ_ONLY
        + _WRITE_TOOLS
        + ("Bash(uv run *)", "Bash(uv sync*)", "Bash(git diff*)", "Bash(git status*)"),
        writes=True,
    ),
    "fix": Policy(READ_ONLY + _WRITE_TOOLS + ("Bash(uv run *)", "Bash(git diff*)"), writes=True),
    "review": Policy(READ_ONLY + ("Bash(git diff*)", "Bash(uv run pytest*)")),
    "diagnose": Policy(READ_ONLY + ("Bash(gh run view *)",)),
}


# ---------------------------------------------------------------- protocol


@runtime_checkable
class Agent(Protocol):
    """Does one stage's work in ``sb`` and returns a typed result.

    Contract: (1) never raises on model/policy errors — they surface as ``is_error``/``subtype``;
    ``StageError(kind="sandbox")`` only when the output cannot be read back. (2) Writes the raw
    result envelope (minus prose) to ``<artifacts_dir>/agent/<stage>.<iteration>.json``.
    (3) When ``policy.writes``, edits are left uncommitted in ``sb.workdir``; the stage commits.
    """

    kind: AgentKind

    def run(
        self,
        sb: Sandbox,
        *,
        stage: str,
        iteration: int,
        prompt: str,
        policy: Policy,
        schema: type[BaseModel] | None,
        cfg: Config,
        issue_id: str,
    ) -> AgentResult: ...


# ---------------------------------------------------------------- prompts


class _Vars(dict):
    def __missing__(self, key: str) -> str:
        return "(none)"


def render_prompt(stage: str, **vars: Any) -> str:
    """Render ``prompts/<stage>.md``; missing or empty variables render as ``(none)``."""
    template = (PROMPTS_DIR / f"{stage}.md").read_text(encoding="utf-8")
    filled = _Vars({k: "(none)" if v is None or v == "" else v for k, v in vars.items()})
    return template.format_map(filled)


# ---------------------------------------------------------------- guard


def guard_deny_rules(protected: Sequence[str]) -> list[str]:
    """``permissions.deny`` for a write stage: Bash/Read rules, fixed path rules, and per
    protected glob ``g`` (trailing slash dropped) ``Edit(g)``, ``Edit(g/**)`` + Write twins."""
    rules = [*GUARD_DENY, *GUARD_PATH_DENY]
    for entry in protected:
        g = entry.strip().rstrip("/")
        if not g:
            continue
        rules += [f"Edit({g})", f"Edit({g}/**)", f"Write({g})", f"Write({g}/**)"]
    return list(dict.fromkeys(rules))


def _target_protected(sb: Sandbox, stage: str) -> Sequence[str]:
    """The target's ``protected`` globs, narrowed to ``stage``; ``()`` without a factory.toml —
    the guard still installs its fixed rules, and ``stages.setup`` has already refused such a
    target (it loads the same contract and raises), so no pipeline failure is being swallowed."""
    try:
        return protected_for(load_target_contract(sb), stage)
    except ValueError:
        return ()


def install_guard(sb: Sandbox, protected: Sequence[str]) -> None:
    """Install the PreToolUse guard into the target checkout (idempotent).

    Writes ``.claude/settings.local.json`` (hook wiring, native ``permissions.deny`` path and
    Bash rules, and ``SWF_PROTECTED`` in ``env``), copies ``swf_guard.py`` next to it and the
    factory's ``swfactory`` skill under ``.claude/skills/``, and lists all of them in
    ``.git/info/exclude`` so they never appear in the delivered patch. The files are written by
    the orchestrator BEFORE the in-sandbox probe: under srt ``.claude/`` is kernel-read-only for
    the sandboxed shell, so the directory must already exist.
    """
    settings = {
        "hooks": {
            "PreToolUse": [
                {
                    "matcher": GUARD_MATCHER,
                    "hooks": [{"type": "command", "command": f"python3 {GUARD_DST}"}],
                }
            ]
        },
        "permissions": {"deny": guard_deny_rules(protected)},
        "env": {"SWF_PROTECTED": ":".join(protected)},
    }
    sb.write(SETTINGS_DST, json.dumps(settings, indent=2) + "\n")
    sb.write(GUARD_DST, GUARD_SRC.read_text(encoding="utf-8"))
    for src in sorted(p for p in SKILL_SRC.rglob("*") if p.is_file()):
        sb.write(f"{SKILL_DST}/{src.relative_to(SKILL_SRC).as_posix()}", src.read_text("utf-8"))
    probe = sb.run(
        'mkdir -p .claude/hooks && mkdir -p "$(git rev-parse --git-dir)/info" '
        "&& git rev-parse --show-cdup --show-prefix"
    )
    if not probe.ok:
        return  # not a git checkout: nothing to exclude
    cdup, prefix = (probe.stdout.splitlines() + ["", ""])[:2]
    exclude_path = f"{cdup}.git/info/exclude"
    try:
        existing = sb.read(exclude_path)
    except FileNotFoundError:
        existing = ""
    present = set(existing.splitlines())
    wanted = (f"{prefix}{SETTINGS_DST}", f"{prefix}{GUARD_DST}", f"{prefix}{SKILL_DST}/")
    missing = [p for p in wanted if p not in present]
    if missing:
        head = existing.rstrip("\n") + "\n" if existing.strip() else ""
        sb.write(exclude_path, head + "\n".join(missing) + "\n")


# ---------------------------------------------------------------- claude


class ClaudeAgent:
    """Runs Claude Code non-interactively inside the sandbox and parses its JSON envelope."""

    kind: AgentKind = "claude"

    def argv(
        self,
        *,
        prompt_path: str,
        out_path: str,
        policy: Policy,
        schema: type[BaseModel] | None,
        cfg: Config,
    ) -> str:
        """Build the ``claude -p`` command line; stdout goes to ``out_path`` (never parsed live)."""
        q = shlex.quote
        parts = [
            f'claude -p "$(cat {q(prompt_path)})"',
            "--output-format json",
            "--permission-mode acceptEdits",
        ]
        if policy.allowed_tools:
            parts.append("--allowedTools " + " ".join(q(t) for t in policy.allowed_tools))
        if policy.disallowed_tools:
            parts.append("--disallowedTools " + " ".join(q(t) for t in policy.disallowed_tools))
        parts += [
            f"--max-turns {cfg.max_turns}",
            f"--max-budget-usd {cfg.max_budget_usd_per_stage}",
            "--no-session-persistence",
        ]
        if schema is not None:
            parts.append(f"--json-schema {q(json.dumps(schema.model_json_schema()))}")
        if policy.model:
            parts.append(f"--model {q(policy.model)}")
        parts.append(f"> {q(out_path)}")
        return " ".join(parts)

    def run(
        self,
        sb: Sandbox,
        *,
        stage: str,
        iteration: int,
        prompt: str,
        policy: Policy,
        schema: type[BaseModel] | None,
        cfg: Config,
        issue_id: str,
    ) -> AgentResult:
        """Write the prompt, guard the checkout if needed, run claude, parse the envelope."""
        art = Config.artifacts_dir(issue_id)
        prompt_path = f".factory/prompt.{stage}.{iteration}.md"
        out_path = f".factory/agent.{stage}.{iteration}.json"
        sb.run(f"mkdir -p .factory {shlex.quote(art + '/agent')}")
        sb.write(prompt_path, prompt)
        if policy.writes:
            install_guard(sb, _target_protected(sb, stage))
        cmd = self.argv(
            prompt_path=prompt_path, out_path=out_path, policy=policy, schema=schema, cfg=cfg
        )
        res = sb.run(cmd, timeout_s=policy.timeout_s)
        try:
            raw = sb.read(out_path)
        except FileNotFoundError as e:
            raise StageError(
                "sandbox",
                f"claude wrote no output at {out_path} (rc={res.exit_code}, "
                f"timed_out={res.timed_out}): {res.stderr[-500:]}",
                retryable=True,
            ) from e
        result, envelope = _parse_envelope(raw, schema, stderr=res.stderr)
        envelope["stage"], envelope["iteration"] = stage, iteration
        sb.write(f"{art}/agent/{stage}.{iteration}.json", _dumps(envelope))
        if cfg.record_dir:
            _record(sb, Path(cfg.record_dir), stage, iteration, result, policy, cfg.run_id)
        return result


def _parse_envelope(
    raw: str, schema: type[BaseModel] | None, *, stderr: str
) -> tuple[AgentResult, dict]:
    """Turn a ``claude --output-format json`` envelope into (AgentResult, envelope-minus-prose)."""
    try:
        env = json.loads(raw)
        if not isinstance(env, dict):
            raise ValueError("envelope is not a JSON object")
    except ValueError as e:
        env = {
            "is_error": True,
            "subtype": "error_unparseable_output",
            "error": str(e),
            "raw_head": raw[:2000],
            "stderr": stderr[-2000:],
        }
        return AgentResult(agent="claude", text=raw, is_error=True, subtype=env["subtype"]), env

    text = env.get("result") or ""
    if not isinstance(text, str):
        text = json.dumps(text)
    is_error = bool(env.get("is_error", False))
    subtype = str(env.get("subtype") or ("error" if is_error else "success"))
    data = env.get("structured_output")
    if schema is not None and not isinstance(data, dict):
        data = _extract_json(text)
    if not isinstance(data, dict):
        data = None
    if schema is not None and not is_error:
        if data is None:
            is_error, subtype = True, "error_no_structured_output"
        else:
            try:
                schema.model_validate(data)
            except ValidationError as e:
                is_error, subtype = True, "error_schema_validation"
                env["schema_error"] = str(e)
    result = AgentResult(
        agent="claude",
        text=text,
        data=data,
        cost_usd=float(env.get("total_cost_usd") or 0.0),
        num_turns=int(env.get("num_turns") or 0),
        duration_ms=int(env.get("duration_ms") or 0),
        session_id=env.get("session_id"),
        is_error=is_error,
        subtype=subtype,
    )
    stored = {k: v for k, v in env.items() if k != "result"}
    stored["is_error"], stored["subtype"] = is_error, subtype
    return result, stored


def _extract_json(text: str) -> dict | None:
    """Best-effort JSON object from result text: whole text, fenced block, or outermost braces."""
    candidates = [text.strip()]
    candidates += _FENCE_RE.findall(text)
    start, end = text.find("{"), text.rfind("}")
    if 0 <= start < end:
        candidates.append(text[start : end + 1])
    for c in candidates:
        try:
            obj = json.loads(c)
        except ValueError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


def _record(
    sb: Sandbox,
    record_dir: Path,
    stage: str,
    iteration: int,
    result: AgentResult,
    policy: Policy,
    run_id: str,
) -> None:
    """Dump a real agent output as ScriptedAgent fixtures (``--record``)."""
    record_dir.mkdir(parents=True, exist_ok=True)
    (record_dir / "RECORDING").write_text(f"run_id={run_id}\n", encoding="utf-8")
    if result.data is not None:
        (record_dir / f"{stage}.{iteration}.json").write_text(_dumps(result.data), encoding="utf-8")
    elif result.text:
        (record_dir / f"{stage}.{iteration}.md").write_text(result.text, encoding="utf-8")
    if policy.writes:
        diff = sb.run("git add -N . && git diff HEAD")
        if diff.ok and diff.stdout.strip():
            header = f"# recorded by swfactory run {run_id}\n"
            (record_dir / f"{stage}.{iteration}.patch").write_text(
                header + diff.stdout, encoding="utf-8"
            )


def _dumps(obj: Any) -> str:
    return json.dumps(obj, indent=2, sort_keys=True) + "\n"


# ---------------------------------------------------------------- scripted


class FixtureMissing(StageError):
    """A stage has no fixture: the scripted run refuses to skip a link in the chain."""

    def __init__(self, stage: str, iteration: int, dirs: Sequence[Path]) -> None:
        where = ", ".join(str(d) for d in dirs)
        super().__init__(
            "agent", f"no fixture for stage={stage} iteration={iteration} in [{where}]"
        )


class ScriptedAgent:
    """Replays recorded fixtures: ``.patch`` applies a diff, ``.json`` is data, ``.md`` is text."""

    kind: AgentKind = "scripted"

    def __init__(self, fixtures_dirs: Sequence[Path]) -> None:
        self.fixtures_dirs = [Path(d) for d in fixtures_dirs]

    def fixture(self, stage: str, iteration: int) -> Path:
        """First existing fixture, searching dirs in order; per dir ``{stage}.{iteration}.*``
        then ``{stage}.*`` with extensions patch, json, md."""
        names = [f"{stage}.{iteration}.{e}" for e in _FIXTURE_EXTS]
        names += [f"{stage}.{e}" for e in _FIXTURE_EXTS]
        for d in self.fixtures_dirs:
            for name in names:
                if (d / name).is_file():
                    return d / name
        raise FixtureMissing(stage, iteration, self.fixtures_dirs)

    def run(
        self,
        sb: Sandbox,
        *,
        stage: str,
        iteration: int,
        prompt: str,
        policy: Policy,
        schema: type[BaseModel] | None,
        cfg: Config,
        issue_id: str,
    ) -> AgentResult:
        """Replay the fixture for (stage, iteration); cost and turns are always zero."""
        path = self.fixture(stage, iteration)
        text = path.read_text(encoding="utf-8")
        art = Config.artifacts_dir(issue_id)
        sb.run(f"mkdir -p .factory {shlex.quote(art + '/agent')}")
        sb.write(f".factory/prompt.{stage}.{iteration}.md", prompt)
        if path.suffix == ".patch":
            result = self._apply_patch(sb, text, name=path.name, policy=policy, schema=schema)
        elif path.suffix == ".json":
            result = _validate_json(text, schema)
        else:
            result = AgentResult(agent="scripted", text=text)
        envelope = {
            "agent": "scripted",
            "fixture": str(path),
            "stage": stage,
            "iteration": iteration,
            "is_error": result.is_error,
            "subtype": result.subtype,
            "structured_output": result.data,
        }
        sb.write(f"{art}/agent/{stage}.{iteration}.json", _dumps(envelope))
        return result

    @staticmethod
    def _apply_patch(
        sb: Sandbox,
        text: str,
        *,
        name: str,
        policy: Policy,
        schema: type[BaseModel] | None,
    ) -> AgentResult:
        if not policy.writes:
            raise StageError("policy", f"patch fixture {name} used by a read-only stage")
        sb.write(".factory/scripted.patch", text)
        res = sb.run("git apply --3way .factory/scripted.patch")
        if not res.ok:
            return AgentResult(
                agent="scripted",
                text=(res.stderr or res.stdout)[-2000:],
                is_error=True,
                subtype="error_patch_apply",
            )
        files = sorted(set(_DIFF_FILE_RE.findall(text)))
        data = None
        if schema is not None:
            summary = {"summary": f"applied scripted fixture {name}", "files_changed": files}
            try:
                data = schema.model_validate(summary).model_dump()
            except ValidationError:
                data = None
        return AgentResult(agent="scripted", text=f"applied {name}", data=data)


def _validate_json(text: str, schema: type[BaseModel] | None) -> AgentResult:
    try:
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("fixture is not a JSON object")
        if schema is not None:
            schema.model_validate(data)
    except (ValueError, ValidationError) as e:
        return AgentResult(
            agent="scripted", text=str(e), is_error=True, subtype="error_schema_validation"
        )
    return AgentResult(agent="scripted", text=text, data=data)


# ---------------------------------------------------------------- factory


def make_agent(cfg: Config) -> Agent:
    """Pick the agent for ``cfg.agent``."""
    if cfg.agent == "claude":
        return ClaudeAgent()
    return ScriptedAgent([Path(cfg.fixtures_dir)])
