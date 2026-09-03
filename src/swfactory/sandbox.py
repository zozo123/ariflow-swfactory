"""Sandboxes: where the target repo is checked out and where commands run.

Three implementations share one protocol:

* ``LocalSandbox`` — a directory on the orchestrator host, used by the scripted demo and tests.
  The child environment is scrubbed of every credential prefix in ``SCRUB_PREFIXES``.
* ``SrtSandbox`` — the same directory, but every ``run()`` goes through the Anthropic Sandbox
  Runtime (``srt``): OS-level write confinement (Seatbelt / bubblewrap) to the workdir plus an
  egress domain allowlist. The cloudless path for a real agent on a keyed dev box; the only
  credential it forwards is an explicit ``pass_env`` allowlist (``ANTHROPIC_API_KEY`` when the
  caller asks for it), never the whole host environment.
* ``IsloSandbox`` — an islo MicroVM. It receives a read-only clone via ``--source`` and the
  gateway's phantom ``ANTHROPIC_API_KEY``; it never receives a GitHub write token. The argv it
  builds never carries ``--env`` / ``--env-file`` (unit-tested invariant).

No environment passthrough exists on this protocol, by design.
"""

from __future__ import annotations

import json
import os
import posixpath
import shlex
import shutil
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol, runtime_checkable

from swfactory.config import Config
from swfactory.models import RunResult, StageError

# Exit code reported when a command is killed by the timeout (mirrors coreutils `timeout`).
TIMEOUT_EXIT_CODE = 124
# Bound for the short control-plane calls (`islo cp`, `islo rm`, `islo resume`, `test -e`).
_CONTROL_TIMEOUT_S = 300

# srt: npm package used when no `srt` binary is on PATH, and the settings file location.
SRT_NPM_PACKAGE = "@anthropic-ai/sandbox-runtime"
SRT_SETTINGS_PATH = ".factory/srt-settings.json"
# Domains Claude Code itself needs (auth, telemetry, crash reporting); always allowed for srt.
SRT_CLAUDE_DOMAINS = (
    "api.anthropic.com",
    "platform.claude.com",
    "console.anthropic.com",
    "statsig.anthropic.com",
    "sentry.io",
)
# Credential stores the sandboxed process must never read (relative to $HOME).
SRT_DENY_READ = (".ssh", ".aws", ".config/gh")
# Host paths Claude Code / uv / npm write to (relative to $HOME unless absolute).
SRT_ALLOW_WRITE_HOME = (".claude", ".claude.json", ".cache")
SRT_ALLOW_WRITE_ABS = ("/tmp", "/private/tmp")
SRT_ALLOW_WRITE_OPTIONAL = ("Library/Caches",)  # macOS only: added when the dir exists
# Directories under the workdir the agent must never write, whatever factory.toml says.
SRT_FIXED_DENY_WRITE = (".claude", ".github")
_GLOB_CHARS = frozenset("*?[")
# Sandboxes whose workdir is a directory on the orchestrator host (seeded from the target dir,
# used as LocalGitScm's base repo); islo clones the target itself.
HOST_SANDBOXES = frozenset({"local", "srt"})


@runtime_checkable
class Sandbox(Protocol):
    """A place where the target repo is checked out and commands run. No env passthrough, ever."""

    name: str
    workdir: str  # absolute path of the target dir inside the sandbox

    def ensure(self) -> None:
        """Create the sandbox if missing (idempotent)."""
        ...

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` with ``bash -lc`` semantics; ``cwd`` defaults to ``workdir``."""
        ...

    def read(self, path: str) -> str:
        """Read a file relative to ``workdir``; raises ``FileNotFoundError``."""
        ...

    def write(self, path: str, content: str) -> None:
        """Write a file relative to ``workdir``, creating parent directories."""
        ...

    def exists(self, path: str) -> bool:
        """True if ``path`` (relative to ``workdir``) exists."""
        ...

    def close(self) -> None:
        """Release the sandbox (islo rm / no-op)."""
        ...


SCRUB_PREFIXES = ("ANTHROPIC_", "GH_TOKEN", "GITHUB_TOKEN", "AWS_", "ISLO_API")


def scrub_env(env: Mapping[str, str]) -> dict[str, str]:
    """Return a copy of ``env`` without any variable starting with a ``SCRUB_PREFIXES`` entry."""
    return {k: v for k, v in env.items() if not k.startswith(SCRUB_PREFIXES)}


def _run_subprocess(
    argv: list[str], *, cwd: Path | None, env: dict[str, str] | None, timeout_s: int
) -> RunResult:
    """Run ``argv`` to completion; a timeout kills the child and yields ``timed_out=True``."""
    started = time.monotonic()
    try:
        proc = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        return RunResult(
            exit_code=TIMEOUT_EXIT_CODE,
            stdout=_as_text(e.stdout),
            stderr=_as_text(e.stderr),
            duration_s=time.monotonic() - started,
            timed_out=True,
        )
    return RunResult(
        exit_code=proc.returncode,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
        duration_s=time.monotonic() - started,
        timed_out=False,
    )


def _as_text(data: str | bytes | None) -> str:
    if data is None:
        return ""
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")
    return data


class LocalSandbox:
    """A directory on the host. Commands run through ``bash -lc`` with a scrubbed environment."""

    def __init__(self, workdir: Path) -> None:
        self.root = Path(workdir).resolve()
        self.name = f"local:{self.root.name}"
        self.workdir = str(self.root)

    def ensure(self) -> None:
        """Create the directory and initialise a git repo on ``main`` if none exists."""
        self.root.mkdir(parents=True, exist_ok=True)
        if not (self.root / ".git").exists():
            res = self.run("git init -q -b main")
            if not res.ok:
                raise StageError("sandbox", f"git init failed in {self.root}: {res.stderr.strip()}")

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` via ``bash -lc`` inside ``cwd`` (default ``workdir``)."""
        return _run_subprocess(
            ["bash", "-lc", cmd],
            cwd=Path(cwd) if cwd else self.root,
            env=scrub_env(os.environ),
            timeout_s=timeout_s,
        )

    def read(self, path: str) -> str:
        """Read a UTF-8 file relative to ``workdir``; raises ``FileNotFoundError``."""
        return self._abs(path).read_text(encoding="utf-8")

    def write(self, path: str, content: str) -> None:
        """Write a UTF-8 file relative to ``workdir``, creating parents."""
        target = self._abs(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    def exists(self, path: str) -> bool:
        """True if the path exists under ``workdir``."""
        return self._abs(path).exists()

    def close(self) -> None:
        """No-op: the local directory is left for inspection."""
        return None

    def _abs(self, path: str) -> Path:
        return self.root / path


class SrtSandbox(LocalSandbox):
    """A host directory whose commands run under the Anthropic Sandbox Runtime (``srt``).

    ``read``/``write``/``exists`` are the orchestrator's own file access (unconfined, like
    ``LocalSandbox``); only ``run()`` is confined: writes are limited to the workdir and the
    caches Claude Code / uv need, ``protected`` globs and ``.claude``/``.github`` under the
    workdir are kernel-enforced read-only, egress is limited to ``allowed_domains`` plus what
    Claude Code itself needs, and credential stores under ``$HOME`` are unreadable. srt adds
    its own mandatory rules on top: no writes to ``.git/config`` or ``.git/hooks`` (so git
    identity must travel as ``git -c user.name=...``, never ``git config``), shell rc files,
    ``.mcp.json``, ``.claude/commands``, ``.claude/agents``.

    The child environment is ``scrub_env(os.environ)`` plus the explicit ``pass_env`` allowlist
    (copied from the host only when present). Nothing else crosses the boundary.
    """

    def __init__(
        self,
        workdir: Path,
        *,
        allowed_domains: Sequence[str],
        protected: Sequence[str] = (),
        pass_env: Sequence[str] = (),
    ) -> None:
        super().__init__(workdir)
        self.name = f"srt:{self.root.name}"
        self.allowed_domains = tuple(allowed_domains)
        self.protected = tuple(protected)
        self.pass_env = tuple(pass_env)
        self.settings_path = self.root / SRT_SETTINGS_PATH

    def settings(self) -> dict:
        """The srt settings document (network allowlist + filesystem read/write policy)."""
        home = Path.home()
        allow_write = [str(self.root)]
        allow_write += [str(home / p) for p in SRT_ALLOW_WRITE_HOME]
        allow_write += list(SRT_ALLOW_WRITE_ABS)
        allow_write += [str(home / p) for p in SRT_ALLOW_WRITE_OPTIONAL if (home / p).exists()]
        deny_write = [str(self.root / d) for d in SRT_FIXED_DENY_WRITE]
        for glob in self.protected:
            prefix = _literal_prefix(glob)
            if prefix:
                deny_write.append(str(self.root / prefix))
        return {
            "network": {
                "allowedDomains": _dedupe((*self.allowed_domains, *SRT_CLAUDE_DOMAINS)),
                "deniedDomains": [],
            },
            "filesystem": {
                "denyRead": [str(home / p) for p in SRT_DENY_READ],
                "allowWrite": _dedupe(allow_write),
                "denyWrite": _dedupe(deny_write),
            },
        }

    def write_settings(self) -> Path:
        """Write the settings file under ``<workdir>/.factory`` and return its path."""
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings_path.write_text(
            json.dumps(self.settings(), indent=2) + "\n", encoding="utf-8"
        )
        return self.settings_path

    def argv(self, cmd: str, *, cwd: str | None = None) -> list[str]:
        """``srt -s <settings> -c 'cd <cwd> && <cmd>'`` (``npx`` fallback when srt is absent)."""
        script = f"cd {shlex.quote(cwd or self.workdir)} && {cmd}"
        return [*_srt_bin(), "-s", str(self.settings_path), "-c", script]

    def env(self) -> dict[str, str]:
        """Scrubbed host env plus the ``pass_env`` allowlist (only keys present on the host)."""
        env = scrub_env(os.environ)
        env.update({k: os.environ[k] for k in self.pass_env if k in os.environ})
        return env

    def ensure(self) -> None:
        """Create the directory, write the srt settings and ``git init`` (host-side).

        srt never lets a sandboxed process create ``.git`` or write ``.git/config`` /
        ``.git/hooks`` (hook-injection protection, verified with 0.0.75), so the empty-repo
        initialisation — the orchestrator's own action on an empty directory, before any
        model-written code exists — runs unconfined like ``LocalSandbox``. Everything after
        this point goes through srt.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        self.write_settings()
        if not (self.root / ".git").exists():
            res = LocalSandbox.run(self, "git init -q -b main")
            if not res.ok:
                raise StageError("sandbox", f"git init failed in {self.root}: {res.stderr.strip()}")

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` under srt; the command's exit code propagates through srt."""
        if not self.settings_path.exists():
            self.write_settings()
        return _run_subprocess(
            self.argv(cmd, cwd=cwd), cwd=self.root, env=self.env(), timeout_s=timeout_s
        )


def _srt_bin() -> list[str]:
    """``srt`` when installed, else a one-shot ``npx`` of the published package."""
    return ["srt"] if shutil.which("srt") else ["npx", "-y", SRT_NPM_PACKAGE]


def _literal_prefix(glob: str) -> str:
    """Longest directory/file prefix of ``glob`` with no wildcard: ``src/**/*.py`` -> ``src``.

    A glob with a wildcard in its first segment (``*.lock``) has no literal prefix and returns
    ``""``: srt cannot express it, so it stays covered by the ``Edit(...)`` deny rules and the
    hook only.
    """
    glob = glob.strip().strip("/")
    cut = next((i for i, ch in enumerate(glob) if ch in _GLOB_CHARS), None)
    if cut is None:
        return glob
    return glob[:cut].rpartition("/")[0]


def _dedupe(items: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(items))


class IsloSandbox:
    """An islo MicroVM created from ``--source`` (read-only clone) and addressed by a stable name.

    The same name on every call makes ``islo use`` create-if-needed the idempotency mechanism
    across Airflow tasks and workers. ``--auto-resume on_activity`` wakes a paused VM on the
    next ``islo use``; ``islo cp`` does not, so ``_cp`` retries once after ``islo resume``.
    """

    def __init__(
        self,
        name: str,
        *,
        source: str,
        gateway_profile: str,
        environment: str,
        ttl_s: int,
        idle_s: int,
        target_dir: str,
        factory_root: Path,
        snapshot: str | None = None,
    ) -> None:
        self.name = name
        self.source = source
        self.gateway_profile = gateway_profile
        self.environment = environment
        self.ttl_s = ttl_s
        self.idle_s = idle_s
        self.snapshot = snapshot
        self.factory_root = Path(factory_root)
        repo_name = _repo_name(source)
        self.workdir = f"/workspace/{repo_name}/{target_dir}".rstrip("/")

    def argv(self, cmd: str, *, cwd: str | None = None, create: bool = False) -> list[str]:
        """Build the ``islo use`` argv for ``cmd``.

        INVARIANT: the result never contains ``--env``, ``--env-file``, ``ANTHROPIC`` or
        ``GH_TOKEN``. Credentials reach the sandbox only through the islo environment/gateway.
        """
        if create:
            argv = [
                "islo",
                "use",
                self.name,
                "--source",
                self.source,
                "--gateway-profile",
                self.gateway_profile,
                "--environment",
                self.environment,
                "--init",
                "minimal",
                "--delete-after",
                str(self.ttl_s),
                "--pause-after-idle",
                str(self.idle_s),
                "--auto-resume",
                "on_activity",
            ]
            if self.snapshot:
                argv += ["--snapshot", self.snapshot]
            return [*argv, "--output", "plain", "--", "true"]
        script = f"cd {shlex.quote(cwd or self.workdir)} && {cmd}"
        return ["islo", "use", self.name, "--output", "plain", "--", "bash", "-lc", script]

    def ensure(self) -> None:
        """Create the sandbox if missing. Runs from ``factory_root`` so ``./islo.yaml`` applies."""
        res = _run_subprocess(
            self.argv("true", create=True),
            cwd=self.factory_root,
            env=None,
            timeout_s=_CONTROL_TIMEOUT_S * 4,
        )
        if not res.ok:
            raise StageError(
                "sandbox",
                f"islo use {self.name} failed (rc={res.exit_code}): {res.stderr.strip()}",
                retryable=True,
            )

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` inside the sandbox; the command's exit code propagates through islo."""
        return _run_subprocess(self.argv(cmd, cwd=cwd), cwd=None, env=None, timeout_s=timeout_s)

    def read(self, path: str) -> str:
        """Copy the file out with ``islo cp`` and return its text; missing -> FileNotFoundError."""
        remote = f"{self.name}:{self._abs(path)}"
        with tempfile.TemporaryDirectory(prefix="swf-cp-") as tmp:
            local = Path(tmp) / "file"
            res = self._cp(remote, str(local))
            if not res.ok or not local.exists():
                raise FileNotFoundError(f"{self.name}:{self._abs(path)} ({res.stderr.strip()})")
            return local.read_text(encoding="utf-8")

    def write(self, path: str, content: str) -> None:
        """Copy ``content`` into the sandbox with ``islo cp``, creating parent directories."""
        abs_path = self._abs(path)
        parent = posixpath.dirname(abs_path)
        if parent and parent != self.workdir:
            mk = self.run(f"mkdir -p {shlex.quote(parent)}", timeout_s=_CONTROL_TIMEOUT_S)
            if not mk.ok:
                raise StageError("sandbox", f"mkdir -p {parent} failed: {mk.stderr.strip()}")
        with tempfile.TemporaryDirectory(prefix="swf-cp-") as tmp:
            local = Path(tmp) / "file"
            local.write_text(content, encoding="utf-8")
            res = self._cp(str(local), f"{self.name}:{abs_path}")
        if not res.ok:
            raise StageError(
                "sandbox", f"islo cp to {abs_path} failed: {res.stderr.strip()}", retryable=True
            )

    def exists(self, path: str) -> bool:
        """True if ``test -e`` succeeds inside the sandbox."""
        res = self.run(f"test -e {shlex.quote(self._abs(path))}", timeout_s=_CONTROL_TIMEOUT_S)
        return res.exit_code == 0

    def close(self) -> None:
        """Remove the sandbox (best effort; the TTL is the backstop)."""
        _run_subprocess(
            ["islo", "rm", self.name, "--output", "plain"],
            cwd=None,
            env=None,
            timeout_s=_CONTROL_TIMEOUT_S,
        )

    def _cp(self, src: str, dst: str) -> RunResult:
        """``islo cp``; on failure resume the (possibly paused) sandbox and retry exactly once."""
        res = self._control(["islo", "cp", src, dst])
        if res.ok:
            return res
        self._control(["islo", "resume", self.name, "--output", "plain"])
        return self._control(["islo", "cp", src, dst])

    @staticmethod
    def _control(argv: list[str]) -> RunResult:
        return _run_subprocess(argv, cwd=None, env=None, timeout_s=_CONTROL_TIMEOUT_S)

    def _abs(self, path: str) -> str:
        return posixpath.normpath(posixpath.join(self.workdir, path))


def _repo_name(source: str) -> str:
    """``github://owner/name:branch`` -> ``name`` (the clone directory under /workspace)."""
    spec = source.split("://", 1)[-1]
    repo = spec.rsplit(":", 1)[0] if ":" in spec else spec
    return repo.rstrip("/").rsplit("/", 1)[-1]


def _factory_root() -> Path:
    """Directory whose ``islo.yaml`` should apply on create: the repo root of this package."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "islo.yaml").exists():
            return parent
    return Path.cwd()


def make_sandbox(
    cfg: Config,
    issue_id: str,
    *,
    protected: Sequence[str] = (),
    repo: str | None = None,
) -> Sandbox:
    """Build the sandbox selected by ``cfg.sandbox`` for the run on ``issue_id``.

    ``protected`` (the target's factory.toml globs) becomes srt's kernel-level ``denyWrite``;
    ``repo`` (owner/name) makes the islo sandbox name unique per (issue, target).
    """
    if cfg.sandbox == "local":
        return LocalSandbox(Path(cfg.workdir))
    if cfg.sandbox == "srt":
        return SrtSandbox(
            Path(cfg.workdir),
            allowed_domains=cfg.srt_allowed_domains,
            protected=protected,
            pass_env=("ANTHROPIC_API_KEY",) if cfg.agent == "claude" else (),
        )
    return IsloSandbox(
        cfg.sandbox_name(issue_id, repo),
        source=f"github://{cfg.repo}:{cfg.base_branch}",
        gateway_profile=cfg.gateway_profile,
        environment=cfg.islo_environment,
        ttl_s=cfg.sandbox_ttl_s,
        idle_s=cfg.sandbox_idle_s,
        target_dir=cfg.target_dir,
        factory_root=_factory_root(),
        snapshot=cfg.islo_snapshot,
    )
