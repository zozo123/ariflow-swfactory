"""``swfactory doctor``: pre-flight checks for the real (islo + GitHub + Claude) path.

Every check is a pure function of an injected *runner* (``argv -> stdout``, raising on a non-zero
exit or a missing binary), a ``which`` lookup and a filesystem root, so the whole module is
unit-testable without islo, gh or a network. Nothing here mutates anything: a failing check
carries the exact ``fix`` command the operator should run (``deploy/islo/bootstrap.sh`` runs the
same commands idempotently).

Argv shapes (verified against ``islo 0.48.1`` and ``gh 2.83``)::

    islo --version
    islo status --output json          # {"auth": {"authenticated": bool}, "integrations": [..]}
    islo status                        # text fallback: "Connected Integrations" section
    islo gateway ls --output json      # [{"name", "default_action", "internet_enabled", ...}]
    islo environment list --output json
    islo snapshot ls --output json     # prints NOTHING (not "[]") when there are no snapshots
    gh auth status
    gh repo view <owner/name> --json name
    claude --version
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from swfactory import blueprint as blueprint_mod
from swfactory.config import Config, TargetContract

Runner = Callable[[Sequence[str]], str]
Which = Callable[[str], str | None]

FACTORY_ROOT = Path(__file__).resolve().parents[2]
SRT_NPM_PACKAGE = "@anthropic-ai/sandbox-runtime"
# Hosts the deny-by-default gateway profile must allow for a factory run to work.
GATEWAY_ALLOW_HOSTS = (
    "api.anthropic.com",
    "github.com",
    "api.github.com",
    "pypi.org",
    "files.pythonhosted.org",
    "astral.sh",
)
# ``islo login --tool claude`` and ``--tool anthropic`` both exist; either satisfies the check.
_CLAUDE_INTEGRATIONS = frozenset({"claude", "anthropic"})
_INTEGRATION_KEYS = ("tool", "name", "provider", "type", "slug", "id")
_DISCONNECTED = frozenset({"disconnected", "expired", "error", "revoked", "pending"})
_COMMAND_TIMEOUT_S = 120


class DoctorCommandError(RuntimeError):
    """A doctor command exited non-zero (message carries the exit code and stderr head)."""


@dataclass(frozen=True)
class Check:
    """One pre-flight result. ``required=False`` marks an informational check (never fails
    the exit code)."""

    name: str
    ok: bool
    detail: str = ""
    fix: str = ""
    required: bool = True

    @property
    def status(self) -> str:
        """``ok`` / ``FAIL`` (required and failing) / ``warn`` (informational and failing)."""
        if self.ok:
            return "ok"
        return "FAIL" if self.required else "warn"


# ---------------------------------------------------------------- runner


def subprocess_runner(argv: Sequence[str]) -> str:
    """Default runner: ``argv`` to completion, stdout back; non-zero -> ``DoctorCommandError``.

    A missing binary raises ``FileNotFoundError`` (from ``subprocess``), which the checks report
    as "not found on PATH".
    """
    proc = subprocess.run(
        list(argv), capture_output=True, text=True, check=False, timeout=_COMMAND_TIMEOUT_S
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout).strip().splitlines()
        raise DoctorCommandError(
            f"{' '.join(argv)} exited {proc.returncode}: {err[0][:200] if err else ''}"
        )
    return proc.stdout


def _try(runner: Runner, argv: Sequence[str]) -> tuple[str | None, str]:
    """``(stdout, "")`` on success, ``(None, reason)`` on any failure (never raises)."""
    try:
        return runner(argv), ""
    except FileNotFoundError:
        return None, f"{argv[0]} not found on PATH"
    except subprocess.TimeoutExpired:
        return None, f"{' '.join(argv)} timed out"
    except Exception as e:  # noqa: BLE001 - a doctor reports, it never crashes
        return None, str(e) or e.__class__.__name__


def _json(text: str) -> object | None:
    """Parse JSON leniently: empty output (``islo snapshot ls`` with no snapshots) is ``None``."""
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def _items(data: object) -> list[dict]:
    """The list of records in a listing: a bare list, or the first list value of an object."""
    if isinstance(data, dict):
        data = next((v for v in data.values() if isinstance(v, list)), [])
    return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []


def _names(items: list[dict]) -> list[str]:
    return [str(x["name"]) for x in items if isinstance(x.get("name"), str)]


# ---------------------------------------------------------------- integrations parsing (pure)


def integration_names(status: object) -> set[str] | None:
    """Lower-cased connected integration names from ``islo status --output json``.

    ``None`` when the document has no ``integrations`` key (caller falls back to text). Items
    may be plain strings or objects; an object whose ``status``/``state`` says disconnected, or
    whose ``connected`` is false, is skipped.
    """
    if not isinstance(status, dict) or "integrations" not in status:
        return None
    raw = status.get("integrations")
    if isinstance(raw, dict):  # {"github": {...}, "claude": {...}} or {"github": true}
        raw = [
            {"name": k, **(v if isinstance(v, dict) else {"connected": bool(v)})}
            for k, v in raw.items()
        ]
    names: set[str] = set()
    for item in raw if isinstance(raw, list) else []:
        if isinstance(item, str):
            names.add(item.strip().lower())
            continue
        if not isinstance(item, dict):
            continue
        state = str(item.get("status") or item.get("state") or "").strip().lower()
        if state in _DISCONNECTED or item.get("connected") is False:
            continue
        names.update(
            str(item[k]).strip().lower() for k in _INTEGRATION_KEYS if isinstance(item.get(k), str)
        )
    return names


def integration_names_from_text(text: str) -> set[str]:
    """Connected integration names from the ``Connected Integrations`` section of ``islo status``.

    The section runs until the next blank line; ``No integrations connected`` yields an empty
    set; otherwise the first word of every entry line is a name (``github: connected as ...``).
    """
    lines = text.splitlines()
    try:
        start = next(i for i, ln in enumerate(lines) if "connected integrations" in ln.lower())
    except StopIteration:
        return set()
    names: set[str] = set()
    for ln in lines[start + 1 :]:
        if not ln.strip():
            break
        low = ln.strip().lower()
        if low.startswith("no integrations") or low.startswith("integrations power"):
            continue
        if m := re.match(r"[-*\s]*([a-z][a-z0-9_-]*)", low):
            names.add(m.group(1))
    return names


# ---------------------------------------------------------------- checks


def _check_islo_cli(runner: Runner) -> Check:
    out, err = _try(runner, ["islo", "--version"])
    if out is None:
        return Check(
            "islo cli", False, err, "install islo >= 0.48 (https://islo.dev) and put it on PATH"
        )
    return Check("islo cli", True, out.strip() or "present")


def _check_islo_auth(runner: Runner) -> tuple[Check, dict | None]:
    out, err = _try(runner, ["islo", "status", "--output", "json"])
    data = _json(out) if out is not None else None
    if not isinstance(data, dict):
        return Check("islo auth", False, err or "islo status returned no JSON", "islo login"), None
    auth = data.get("auth") if isinstance(data.get("auth"), dict) else {}
    if not auth.get("authenticated"):
        return Check("islo auth", False, "not authenticated", "islo login"), data
    who = ", ".join(f"{k}={auth[k]}" for k in ("tenant_name", "method", "tier") if auth.get(k))
    return Check("islo auth", True, who or "authenticated"), data


def _check_integrations(runner: Runner, status: dict | None) -> list[Check]:
    names = integration_names(status)
    source = "json"
    if names is None:
        text, err = _try(runner, ["islo", "status"])
        if text is None:
            missing = f"cannot read integrations: {err}"
            return [
                Check("integration github", False, missing, "islo login --tool github"),
                Check("integration claude", False, missing, "islo login --tool claude"),
            ]
        names, source = integration_names_from_text(text), "text"
    have = ", ".join(sorted(names)) or "none"
    gh_ok = "github" in names
    claude_ok = bool(names & _CLAUDE_INTEGRATIONS)
    return [
        Check(
            "integration github",
            gh_ok,
            f"connected ({source})" if gh_ok else f"not connected; have: {have}",
            "" if gh_ok else "islo login --tool github",
        ),
        Check(
            "integration claude",
            claude_ok,
            f"connected ({source})" if claude_ok else f"not connected; have: {have}",
            "" if claude_ok else "islo login --tool claude",
        ),
    ]


def gateway_fix(profile: str) -> str:
    """The create + allow-rule commands for a deny-by-default profile (also in bootstrap.sh)."""
    return (
        f"islo gateway create --name {profile} --default-action deny --internet-access true;"
        f" then for each of {' '.join(GATEWAY_ALLOW_HOSTS)}:"
        f" islo gateway {profile} add-rule --host <host> --action allow"
    )


def _check_gateway(runner: Runner, profile: str) -> Check:
    out, err = _try(runner, ["islo", "gateway", "ls", "--output", "json"])
    if out is None:
        return Check("gateway profile", False, err, gateway_fix(profile))
    items = _items(_json(out))
    match = next((x for x in items if x.get("name") == profile), None)
    if match is None:
        have = ", ".join(_names(items)) or "none"
        return Check(
            "gateway profile", False, f"{profile!r} not found; have: {have}", gateway_fix(profile)
        )
    action = str(match.get("default_action") or "").lower()
    internet = match.get("internet_enabled")
    detail = (
        f"{profile!r} default_action={action or '?'} internet_enabled={internet} "
        f"rules={match.get('rule_count', '?')}"
    )
    problems = []
    if action != "deny":
        problems.append("default_action must be deny")
    if internet is not True:
        problems.append("internet access must be enabled for the explicit allow rules")
    if problems:
        return Check(
            "gateway profile",
            False,
            f"{detail} ({'; '.join(problems)})",
            gateway_fix(profile),
        )
    return Check("gateway profile", True, detail)


def environment_fix(env: str) -> str:
    """The create command for the environment carrying the phantom Anthropic key."""
    return (
        f"islo environment create --name {env} --gateway-secret "
        f"'ANTHROPIC_API_KEY=<real key>;host=api.anthropic.com;auth=bearer'"
    )


def _check_environment(runner: Runner, env: str) -> Check:
    out, err = _try(runner, ["islo", "environment", "list", "--output", "json"])
    if out is None:
        return Check("islo environment", False, err, environment_fix(env))
    names = _names(_items(_json(out)))
    if env not in names:
        have = ", ".join(names) or "none"
        return Check(
            "islo environment", False, f"{env!r} not found; have: {have}", environment_fix(env)
        )
    return Check("islo environment", True, f"{env!r} present")


def _check_snapshot(runner: Runner, snapshot: str) -> Check:
    fix = (
        "bake it: SNAPSHOT=1 deploy/islo/bootstrap.sh (README 'Warm start'), "
        "or unset [sandbox] snapshot"
    )
    out, err = _try(runner, ["islo", "snapshot", "ls", "--output", "json"])
    if out is None:
        return Check("islo snapshot", False, err, fix)
    names = _names(_items(_json(out)))  # empty stdout == no snapshots
    if snapshot not in names:
        have = ", ".join(names) or "none"
        return Check("islo snapshot", False, f"{snapshot!r} not found; have: {have}", fix)
    return Check("islo snapshot", True, f"{snapshot!r} present")


def _check_gh_auth(runner: Runner) -> Check:
    out, err = _try(runner, ["gh", "auth", "status"])
    if out is None:
        return Check(
            "gh auth", False, err, "gh auth login  (or export GH_TOKEN=<swfactory-bot PAT>)"
        )
    account = re.search(r"Logged in to (\S+) account (\S+)", out)
    return Check("gh auth", True, f"{account.group(2)}@{account.group(1)}" if account else "ok")


def _check_gh_repo(runner: Runner, repo: str) -> Check:
    out, err = _try(runner, ["gh", "repo", "view", repo, "--json", "name"])
    data = _json(out) if out is not None else None
    if not isinstance(data, dict) or not data.get("name"):
        return Check(
            "gh repo",
            False,
            err or f"gh repo view {repo} returned no name",
            f"check the repo name {repo!r} in the blueprint and that GH_TOKEN can read it",
        )
    return Check("gh repo", True, f"{repo} reachable")


def _check_claude(runner: Runner, *, required: bool) -> Check:
    out, err = _try(runner, ["claude", "--version"])
    if out is None:
        return Check(
            "claude cli",
            False,
            err + ("" if required else " (islo image ships claude; host copy optional)"),
            "npm i -g @anthropic-ai/claude-code (or: curl -fsSL https://claude.ai/install.sh | sh)",
            required=required,
        )
    return Check("claude cli", True, out.strip(), required=required)


def _check_srt(which: Which) -> Check:
    if which("srt"):
        return Check("srt", True, "srt on PATH", required=False)
    if which("npx"):
        return Check("srt", True, f"npx present (npx -y {SRT_NPM_PACKAGE})", required=False)
    return Check(
        "srt",
        False,
        "neither srt nor npx on PATH (only --sandbox srt needs them)",
        f"npm i -g {SRT_NPM_PACKAGE}",
        required=False,
    )


def _check_blueprint(name: str) -> Check:
    try:
        bp = blueprint_mod.load(name)
    except (OSError, ValueError) as e:
        return Check(
            "blueprint", False, str(e), f"fix blueprints/{name}.toml (see README 'Blueprints')"
        )
    return Check(
        "blueprint",
        True,
        f"{bp.name!r}: {' > '.join(bp.order)}; sandbox={bp.sandbox.kind}; "
        f"targets={len(bp.targets)}",
    )


def _check_factory_toml(target_dir: str, root: Path) -> Check:
    rel = Path(target_dir) / "factory.toml"
    path = rel if rel.is_absolute() else root / rel
    if not path.is_file() and not rel.is_absolute() and (FACTORY_ROOT / rel).is_file():
        path = FACTORY_ROOT / rel
    fix = f'add {rel} with [commands] test = "..." (the factory never guesses commands)'
    if not path.is_file():
        return Check("factory.toml", False, f"{path} missing", fix)
    try:
        contract = TargetContract.parse(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return Check("factory.toml", False, f"{path}: {e}", fix)
    return Check("factory.toml", True, f"{path} test={contract.test!r}")


# ---------------------------------------------------------------- driver


def run_doctor(
    cfg: Config,
    runner: Runner | None = None,
    *,
    which: Which = shutil.which,
    root: Path | None = None,
) -> list[Check]:
    """Run every check for ``cfg`` and return them in display order. Never raises.

    ``runner`` defaults to :func:`subprocess_runner` (resolved at call time so tests can patch
    the module attribute); ``which``/``root`` are the other injection points. When the islo CLI
    is missing, the islo-dependent checks are reported as failed-skipped instead of each
    repeating the error.
    """
    runner = runner if runner is not None else subprocess_runner
    root = Path(root) if root is not None else Path.cwd()
    checks = [_check_islo_cli(runner)]
    if checks[0].ok:
        auth, status = _check_islo_auth(runner)
        checks.append(auth)
        checks += _check_integrations(runner, status)
        checks.append(_check_gateway(runner, cfg.gateway_profile))
        checks.append(_check_environment(runner, cfg.islo_environment))
        if cfg.islo_snapshot:
            checks.append(_check_snapshot(runner, cfg.islo_snapshot))
    else:
        skipped = "skipped: islo CLI unavailable"
        checks += [
            Check("islo auth", False, skipped, "islo login"),
            Check("integration github", False, skipped, "islo login --tool github"),
            Check("integration claude", False, skipped, "islo login --tool claude"),
            Check("gateway profile", False, skipped, gateway_fix(cfg.gateway_profile)),
            Check("islo environment", False, skipped, environment_fix(cfg.islo_environment)),
        ]
    checks.append(_check_gh_auth(runner))
    checks.append(_check_gh_repo(runner, cfg.repo))
    checks.append(_check_claude(runner, required=cfg.sandbox != "islo"))
    checks.append(_check_srt(which))
    checks.append(_check_blueprint(cfg.blueprint))
    checks.append(_check_factory_toml(cfg.target_dir, root))
    return checks


def failed(checks: Sequence[Check]) -> list[Check]:
    """The required checks that did not pass."""
    return [c for c in checks if c.required and not c.ok]


def exit_code(checks: Sequence[Check]) -> int:
    """1 if any required check fails, else 0 (informational checks never fail the run)."""
    return 1 if failed(checks) else 0


def table(checks: Sequence[Check]) -> str:
    """Human-readable report: one line per check plus a ``fix:`` line for every failure."""
    width = max((len(c.name) for c in checks), default=4)
    lines = []
    for c in checks:
        lines.append(f"{c.status:<5}{c.name:<{width}}  {c.detail}".rstrip())
        if not c.ok and c.fix:
            lines.append(f"{'':<5}{'':<{width}}  fix: {c.fix}")
    bad = failed(checks)
    warn = sum(1 for c in checks if not c.ok and not c.required)
    summary = f"{len(checks)} checks, {len(bad)} failed" + (f", {warn} warnings" if warn else "")
    return "\n".join([*lines, summary])


def to_json(checks: Sequence[Check]) -> str:
    """Machine-readable report (``--json``): a list of check objects plus ``status``."""
    return json.dumps([{**asdict(c), "status": c.status} for c in checks], indent=2)
