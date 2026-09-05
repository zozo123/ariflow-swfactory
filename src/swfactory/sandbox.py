"""Sandboxes: where the target repo is checked out and where commands run.

Four implementations share one protocol:

* ``LocalSandbox`` — a directory on the orchestrator host, used by the scripted demo and tests.
  The child environment is scrubbed of every credential prefix in ``SCRUB_PREFIXES``.
* ``SrtSandbox`` — the same directory, but every ``run()`` goes through the Anthropic Sandbox
  Runtime (``srt``): OS-level write confinement (Seatbelt / bubblewrap) to the workdir plus an
  egress domain allowlist. The cloudless path for a real agent on a keyed dev box; the only
  credential it forwards is an explicit ``pass_env`` allowlist, and only to ``run_agent``
  (``ANTHROPIC_API_KEY`` when requested), never to target tests or the whole host environment.
* ``DockerSandbox`` — the same directory, bind-mounted at the SAME absolute path into a fresh
  ``docker run --rm`` container per ``run()`` (sibling containers when the orchestrator itself is
  a container with the Docker socket). A container, not a MicroVM: shares the host kernel, and
  the Docker socket is root-equivalent on the host. For local testing of the full pipeline.
* ``IsloSandbox`` — an islo MicroVM. It receives a read-only clone via ``--source`` and the
  gateway's phantom ``ANTHROPIC_API_KEY``; it never receives a GitHub write token. The argv it
  builds never carries ``--env`` / ``--env-file`` (unit-tested invariant).

No environment passthrough exists on this protocol, by design.
"""

from __future__ import annotations

import importlib
import json
import os
import posixpath
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from swfactory.config import Config
from swfactory.models import RunResult, StageError
from swfactory.paths import confined_path, confined_posix_path, normalize_relative_path
from swfactory.state import RunState

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
# Credential stores the sandboxed process must never read (relative to $HOME). ``.config`` covers
# gh/gcloud/etc.; git only warns about its unreadable XDG ignore file and carries on (probed).
SRT_DENY_READ = (".ssh", ".aws", ".config", ".gnupg", ".netrc", ".docker", ".kube")
# Host paths Claude Code / uv / npm write to (relative to $HOME unless absolute).
SRT_ALLOW_WRITE_HOME = (".claude", ".claude.json", ".cache")
SRT_ALLOW_WRITE_ABS = ("/tmp", "/private/tmp")
SRT_ALLOW_WRITE_OPTIONAL = ("Library/Caches",)  # macOS only: added when the dir exists
# Directories under the workdir the agent must never write, whatever factory.toml says.
SRT_FIXED_DENY_WRITE = (".claude", ".github")
# Sandbox Runtime's own mandatory auto-protected root paths. On Linux, missing paths are
# represented by transient bind-mount placeholders inside confinement; the host checkout never
# contains them. Existing project paths are left alone and remain tracked.
SRT_RUNTIME_PROTECTED = (
    ".bashrc",
    ".bash_profile",
    ".zshrc",
    ".zprofile",
    ".profile",
    ".gitconfig",
    ".gitmodules",
    ".ripgreprc",
    ".mcp.json",
    ".vscode",
    ".idea",
)
_GLOB_CHARS = frozenset("*?[")
# docker: $HOME of the non-root user baked into deploy/docker/sandbox.Dockerfile, the host files
# bind-mounted there for ``credentials="host"``, and the named volume that keeps uv/npm caches
# across the one-container-per-run() lifecycle.
DOCKER_HOME = "/home/swf"
DOCKER_CREDENTIAL_FILES = (".claude", ".claude.json")
DOCKER_CACHE_VOLUME = "swfactory-sandbox-cache"
DOCKER_TMP_HOME = "/tmp/swf-home"  # $HOME for arbitrary host uids (bind-mounted files stay yours)
# Bind-mounted files keep the host uid; the container user usually differs, so git must not
# refuse the checkout as "dubious ownership" (not a secret; travels as -e K=V).
DOCKER_GIT_ENV = (
    ("GIT_CONFIG_COUNT", "1"),
    ("GIT_CONFIG_KEY_0", "safe.directory"),
    ("GIT_CONFIG_VALUE_0", "*"),
)
# Sandboxes whose workdir is a directory on the orchestrator host (seeded from the target dir,
# used as LocalGitScm's base repo); islo clones the target itself.
HOST_SANDBOXES = frozenset({"local", "srt", "docker"})


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

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        """Run the model process with its explicitly scoped authentication."""
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


SCRUB_PREFIXES = (
    "ANTHROPIC_",
    "OPENAI_",
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "AWS_",
    "AZURE_",
    "GOOGLE_",
    "GCP_",
    "ISLO_API",
    "DAYTONA_",
    "E2B_",
    "TENSORLAKE_",
    "BOX_API_",
    "STRIPE_",
    "SLACK_",
    "POSTGRES_",
    "MYSQL_",
    "REDIS_",
    "TF_VAR_",
)
SCRUB_EXACT = frozenset(
    {
        "DATABASE_URL",
        "DOCKER_AUTH_CONFIG",
        "HF_TOKEN",
        "KUBECONFIG",
        "NETRC",
        "NODE_AUTH_TOKEN",
        "NPM_TOKEN",
        "SSH_AUTH_SOCK",
    }
)


def owns_sandbox(list_json: str, name: str, *, owner: str | None = None) -> bool:
    """True iff ``name`` appears in this ``islo ls --output json`` listing (own scope) and, when
    ``owner`` is given, its ``created_by`` equals ``owner``. Pure; used before every ``islo rm``."""
    try:
        data = json.loads(list_json or "")
    except ValueError:
        return False
    if isinstance(data, dict):
        data = next((v for v in data.values() if isinstance(v, list)), [])
    if not isinstance(data, list):
        return False
    for item in data:
        if not isinstance(item, dict) or item.get("name") != name:
            continue
        if item.get("status") == "deleted":
            return False
        creator = str(item.get("created_by") or "").strip().lower()
        return not owner or creator == owner.strip().lower()
    return False


def scrub_env(env: Mapping[str, str]) -> dict[str, str]:
    """Drop common credential families before a host process crosses into agent execution."""

    return {
        key: value
        for key, value in env.items()
        if key not in SCRUB_EXACT
        and not key.startswith(SCRUB_PREFIXES)
        and not key.endswith(("_API_KEY", "_ACCESS_TOKEN", "_AUTH_TOKEN", "_PASSWORD", "_SECRET"))
    }


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


def _credential_env(pass_env: Sequence[str]) -> dict[str, str]:
    """Scrubbed host env plus the explicit ``pass_env`` allowlist (only keys the host actually
    has: an absent credential is never invented). One copy so srt and docker cannot drift."""
    env = scrub_env(os.environ)
    env.update({k: os.environ[k] for k in pass_env if k in os.environ})
    return env


class LocalSandbox:
    """A directory on the host. Commands run through ``bash -lc`` with a scrubbed environment."""

    def __init__(self, workdir: Path) -> None:
        self.root = Path(workdir).resolve()
        self.name = f"local:{self.root.name}"
        self.workdir = str(self.root)

    def ensure(self) -> None:
        """Create the directory and initialise a git repo on ``main`` if none exists."""
        self.root.mkdir(parents=True, exist_ok=True)
        self._host_git_init()

    def _host_git_init(self) -> None:
        """``git init`` on the host, never through a confined shell (idempotent). Shared by all
        three host sandboxes: srt refuses to let a sandboxed process create ``.git`` or write
        ``.git/config`` (hook-injection protection) and a container would own it as another uid,
        so initialising an empty directory stays the orchestrator's own action."""
        if (self.root / ".git").exists():
            return
        res = LocalSandbox.run(self, "git init -q -b main")
        if not res.ok:
            raise StageError("sandbox", f"git init failed in {self.root}: {res.stderr.strip()}")

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` via ``bash -lc`` inside ``cwd`` (default ``workdir``)."""
        return _run_subprocess(
            ["bash", "-lc", cmd],
            cwd=self._abs(cwd) if cwd else self.root,
            env=scrub_env(os.environ),
            timeout_s=timeout_s,
        )

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        """Development-only agent path; the local escape hatch uses the host's Claude login."""

        return self.run(cmd, timeout_s=timeout_s)

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
        try:
            return confined_path(self.root, path)
        except ValueError as e:
            raise StageError("policy", str(e)) from e


class SrtSandbox(LocalSandbox):
    """A host directory whose commands run under the Anthropic Sandbox Runtime (``srt``).

    ``read``/``write``/``exists`` are the orchestrator's own file access (unconfined, like
    ``LocalSandbox``); only ``run()`` is confined: writes are limited to the workdir and the
    caches Claude Code / uv need, ``protected`` globs and ``.claude``/``.github`` under the
    workdir are kernel-enforced read-only, egress is limited to ``allowed_domains`` plus what
    Claude Code itself needs, and credential stores under ``$HOME`` are unreadable. srt adds
    its own mandatory rules on top: no writes to ``.git/config`` or ``.git/hooks`` (so git
    identity must travel as ``git -c user.name=...``, never ``git config``), shell rc files,
    ``.mcp.json``, Git metadata, shell profiles and IDE configuration.

    Ordinary commands receive ``scrub_env(os.environ)``. Only ``run_agent`` adds the explicit
    ``pass_env`` allowlist (copied from the host only when present), so target verification cannot
    inherit the model credential. Nothing else crosses the boundary.
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
        # bubblewrap (Linux) can only deny paths that exist; Seatbelt (macOS) does not care.
        deny_write = [str(self.root / d) for d in SRT_FIXED_DENY_WRITE if (self.root / d).exists()]
        for glob in self.protected:
            prefix = _literal_prefix(glob)
            if prefix and (self.root / prefix).exists():
                deny_write.append(str(self.root / prefix))
        return {
            "network": {
                "allowedDomains": _dedupe((*self.allowed_domains, *SRT_CLAUDE_DOMAINS)),
                "deniedDomains": [],
            },
            "filesystem": {
                "denyRead": [str(home / p) for p in SRT_DENY_READ if (home / p).exists()],
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

    def set_protected(self, globs: Sequence[str]) -> None:
        """Replace the kernel-level ``denyWrite`` globs (the target's factory.toml ``protected``,
        narrowed per stage by ``config.protected_for``) and rewrite the settings file so the next
        ``run()`` is confined accordingly."""
        self.protected = tuple(globs)
        self.write_settings()

    def _settings_current(self) -> bool:
        """True when the settings file on disk matches this object's policy. Every DAG task builds
        a fresh ``SrtSandbox`` over the same workdir, so a file left by an earlier task (or an
        earlier ``set_protected``) is re-synced rather than trusted."""
        try:
            return json.loads(self.settings_path.read_text(encoding="utf-8")) == self.settings()
        except (OSError, ValueError):
            return False

    def argv(self, cmd: str, *, cwd: str | None = None) -> list[str]:
        """``srt -s <settings> -c 'cd <cwd> && <cmd>'`` (``npx`` fallback when srt is absent)."""
        run_cwd = str(self._abs(cwd)) if cwd else self.workdir
        script = f"cd {shlex.quote(run_cwd)} && {cmd}"
        return [*_srt_bin(), "-s", str(self.settings_path), "-c", script]

    def env(self, *, agent: bool = False) -> dict[str, str]:
        """Scrubbed host env, with agent credentials only for the model process.

        GitHub's setup-uv exports ``UV_CACHE_DIR`` under ``$RUNNER_TEMP``. That path is outside
        the SRT write allowlist, so forwarding it makes ``uv sync`` fail read-only. Always pin uv
        to the sandbox-owned cache under ``.factory/uv-cache`` while preserving the exact credential
        allowlist.
        """
        env = _credential_env(self.pass_env if agent else ())
        env["UV_CACHE_DIR"] = str(self.root / ".factory" / "uv-cache")
        return env

    def ensure(self) -> None:
        """Create the directory, write the srt settings and ``git init`` (host-side, see
        ``_host_git_init``). Everything after this point goes through srt."""
        self.root.mkdir(parents=True, exist_ok=True)
        # bubblewrap cannot install a denyWrite mount on a missing path. Pre-create the
        # fixed protected directories on the trusted orchestrator side before the first
        # sandboxed command; empty directories are never committed.
        for rel in SRT_FIXED_DENY_WRITE:
            (self.root / rel).mkdir(parents=True, exist_ok=True)
        self.write_settings()
        self._host_git_init()

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run a credential-free command under srt; its exit code propagates."""
        if not self._settings_current():  # settings() reflects paths that appeared since
            self.write_settings()
        return _run_subprocess(
            self.argv(cmd, cwd=cwd), cwd=self.root, env=self.env(), timeout_s=timeout_s
        )

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        """Run only the model process with the explicitly allowlisted credential."""

        if not self._settings_current():
            self.write_settings()
        return _run_subprocess(
            self.argv(cmd), cwd=self.root, env=self.env(agent=True), timeout_s=timeout_s
        )


class DockerSandbox(LocalSandbox):
    """A host directory whose commands each run in a fresh ``docker run --rm`` container.

    In-place model like ``SrtSandbox``: the workdir is bind-mounted read-write at the SAME
    absolute path inside the container (so paths in patches, junit files and agent output are
    identical inside and out), ``read``/``write``/``exists`` are the orchestrator's own file
    access on the host, and ``git init`` in ``ensure`` runs host-side. Confinement is the
    container: the process sees only the workdir (plus ``.claude``/``.github`` and the
    ``protected`` globs' literal prefixes re-mounted read-only), the image, a cache volume and
    the ``network`` given (``"none"`` = no egress). The docker CLI itself runs with
    ``scrub_env(os.environ)``. Only ``run_agent`` adds the ``pass_env`` allowlist (``-e NAME``,
    value read from the CLI's env — never in argv) or, with ``credentials="host"``, mounts the
    host's ``~/.claude`` + ``~/.claude.json`` into the container ``$HOME``. Target verification
    receives neither credential mode.

    Honest limits: a container shares the host kernel and the Docker socket is root-equivalent
    on the host; no phantom tokens. Use for testing the pipeline locally, not as the production
    trust boundary (that is islo).
    """

    def __init__(
        self,
        workdir: Path,
        *,
        image: str,
        pass_env: Sequence[str] = (),
        credentials: str = "env",
        protected: Sequence[str] = (),
        network: str = "bridge",
        user: str | None = None,
        home: str = DOCKER_HOME,
    ) -> None:
        super().__init__(workdir)
        self.name = f"docker:{self.root.name}"
        self.image = image
        self.pass_env = tuple(pass_env)
        self.credentials = credentials
        self.protected = tuple(protected)
        self.network = network
        self.user = user
        self.home = home.rstrip("/") or "/"

    def set_protected(self, globs: Sequence[str]) -> None:
        """Replace the read-only globs (the target's factory.toml ``protected``, narrowed per stage
        by ``config.protected_for``); the next ``run()`` mounts them ``:ro``."""
        self.protected = tuple(globs)

    def _custom_uid(self) -> bool:
        """True when running as a uid other than the image user (root or the baked-in ``swf``)."""
        return bool(self.user) and self.user.split(":")[0] not in ("root", "0", "swf", "1000")

    def mounts(self, *, agent: bool = False) -> list[str]:
        """``-v`` pairs: workdir rw at its own path, fixed + protected prefixes ro (only those that
        exist: docker would create a missing source as a root-owned dir), the cache volume, and
        the host Claude login only for an agent invocation in ``credentials == "host"`` mode."""
        mounts = ["-v", f"{self.root}:{self.root}"]
        ro: list[str] = list(SRT_FIXED_DENY_WRITE)
        ro += [_literal_prefix(g) for g in self.protected]
        for rel in _dedupe([r for r in ro if r]):
            path = self.root / rel
            if path.exists():
                mounts += ["-v", f"{path}:{path}:ro"]
        if not self._custom_uid():
            mounts += ["-v", f"{DOCKER_CACHE_VOLUME}:{self.home}/.cache"]
        if agent and self.credentials == "host":
            host_home = Path.home()
            for rel in DOCKER_CREDENTIAL_FILES:
                src = host_home / rel
                if src.exists():
                    mounts += ["-v", f"{src}:{self.home}/{rel}"]
        return mounts

    def argv(self, cmd: str, *, cwd: str | None = None, agent: bool = False) -> list[str]:
        """``docker run --rm --init <mounts> -w <cwd> --network <net> [-e NAME]... <image> bash -lc
        <cmd>``. ``-e NAME`` (no ``=value``) makes docker copy the variable from the CLI's own
        environment (``env()``), so no secret is ever an argv token."""
        run_cwd = str(self._abs(cwd)) if cwd else self.workdir
        argv = ["docker", "run", "--rm", "--init", *self.mounts(agent=agent)]
        argv += ["-w", run_cwd, "--network", self.network]
        if self.user:
            argv += ["--user", self.user]
        if self._custom_uid():
            # An arbitrary host uid has no home in the image: give it one under /tmp (1777).
            argv += [
                "-e",
                f"HOME={DOCKER_TMP_HOME}",
                "-e",
                f"UV_CACHE_DIR={DOCKER_TMP_HOME}/.cache/uv",
            ]
            argv += ["-e", f"npm_config_cache={DOCKER_TMP_HOME}/.npm"]
            cmd = f'mkdir -p "$HOME" && {cmd}'
        for k, v in DOCKER_GIT_ENV:
            argv += ["-e", f"{k}={v}"]
        if agent:
            for k in self.pass_env:
                if k in os.environ:
                    argv += ["-e", k]
        return [*argv, self.image, "bash", "-lc", cmd]

    def env(self, *, agent: bool = False) -> dict[str, str]:
        """Docker CLI environment; model credentials exist only for ``run_agent``."""

        return _credential_env(self.pass_env if agent else ())

    def ensure(self) -> None:
        """Create the directory and ``git init`` host-side (see ``_host_git_init``); everything
        after this point runs in a container."""
        self.root.mkdir(parents=True, exist_ok=True)
        self._host_git_init()

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` in a fresh credential-free container."""
        return _run_subprocess(
            self.argv(cmd, cwd=cwd), cwd=self.root, env=self.env(), timeout_s=timeout_s
        )

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        """Run only the model process with the API key or mounted Claude login."""

        return _run_subprocess(
            self.argv(cmd, agent=True),
            cwd=self.root,
            env=self.env(agent=True),
            timeout_s=timeout_s,
        )


def default_docker_user() -> str | None:
    """On Linux run the container as the host uid:gid so bind-mounted files stay writable by the
    host user (Docker Desktop on macOS maps ownership itself, so ``None`` = the image user)."""
    if sys.platform.startswith("linux") and hasattr(os, "getuid"):
        return f"{os.getuid()}:{os.getgid()}"
    return None


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
        owner: str | None = None,
    ) -> None:
        self.owner = owner
        self.name = name
        self.source = source
        self.gateway_profile = gateway_profile
        self.environment = environment
        self.ttl_s = ttl_s
        self.idle_s = idle_s
        self.snapshot = snapshot
        self.factory_root = Path(factory_root)
        repo_name = _repo_name(source)
        self.repo_root = f"/workspace/{repo_name}"
        target = normalize_relative_path(target_dir, field="target_dir", allow_empty=True)
        self.workdir = posixpath.join(self.repo_root, target) if target else self.repo_root

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
        run_cwd = self._cwd(cwd) if cwd else self.workdir
        script = f"cd {shlex.quote(run_cwd)} && {cmd}"
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

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        """Run the model through the sandbox's gateway-backed phantom credential."""

        return self.run(cmd, timeout_s=timeout_s)

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
        """Remove the sandbox — only if it is in the caller's OWN ``islo ls`` (and, when an owner
        is configured, was created by that owner). Never removes a teammate's sandbox; the TTL
        (``--delete-after``) is the backstop for anything we refuse to touch."""
        listing = self._control(["islo", "ls", "--output", "json"])
        if not owns_sandbox(listing.stdout, self.name, owner=self.owner):
            print(f"sandbox: refusing to remove {self.name!r}: not in own listing/owner mismatch")
            return
        self._control(["islo", "rm", self.name, "--output", "plain"])

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
        try:
            value = path if path.startswith("/") else posixpath.join(self.workdir, path)
            return confined_posix_path(self.repo_root, value)
        except ValueError as e:
            raise StageError("policy", str(e)) from e

    def _cwd(self, path: str) -> str:
        try:
            value = path if path.startswith("/") else posixpath.join(self.workdir, path)
            return confined_posix_path(self.workdir, value)
        except ValueError as e:
            raise StageError("policy", str(e)) from e


def _repo_name(source: str) -> str:
    """``github://owner/name:branch`` -> ``name`` (the clone directory under /workspace)."""
    spec = source.split("://", 1)[-1]
    repo = spec.rsplit(":", 1)[0] if ":" in spec else spec
    return repo.rstrip("/").rsplit("/", 1)[-1]


def _factory_root() -> Path:
    """Directory whose ``islo.yaml`` should apply on create: the repo root of this package."""
    from swfactory.config import FACTORY_ROOT

    if (FACTORY_ROOT / "islo.yaml").is_file():
        return FACTORY_ROOT
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "islo.yaml").exists():
            return parent
    return Path.cwd()


# ---------------------------------------------------------------- upstream toolset backends

# Airflow's own sandbox abstraction (provider apache-airflow-providers-common-ai). `sbx` ships in
# the released provider; the other three are pending upstream pull requests, and the module/class
# names below are the ones those PRs actually add — checked against the diffs, not guessed.
TOOLSET_BACKENDS = {
    "sbx": ("airflow.providers.common.ai.sandbox.sbx", "SbxSandboxBackend", None),
    "islo": ("airflow.providers.common.ai.sandbox.islo", "IsloSandboxBackend", 71672),
    "opensandbox": (
        "airflow.providers.common.ai.sandbox.opensandbox",
        "OpenSandboxBackend",
        71676,
    ),
    "asciibox": ("airflow.providers.common.ai.sandbox.ascii_box", "AsciiBoxSandboxBackend", 71725),
}
TOOLSET_MAX_OUTPUT_BYTES = 1_000_000
TOOLSET_STATE_FILE = "toolset-sandbox.json"


@dataclass(frozen=True)
class ToolsetRequirements:
    """Duck-typed ``SandboxSpec`` for custom adapters used outside an Airflow install."""

    env: Mapping[str, str]
    block_network: bool
    allow_egress_to: tuple[str, ...]


def load_toolset_backend(name: str, **kwargs: object):
    """Import and construct one of Airflow's ``SandboxBackend`` implementations.

    Kept lazy and by name so swfactory never imports Airflow at module scope (the DAG-parse rule)
    and so an unreleased backend is a clear error rather than an import failure at startup.
    """
    if ":" in name:
        module_path, separator, cls_name = name.rpartition(":")
        if not separator or not module_path or not cls_name:
            raise StageError("sandbox", "custom backend must be package.module:Class")
        try:
            module = importlib.import_module(module_path)
            backend_cls = getattr(module, cls_name)
            return backend_cls(**kwargs)
        except (ImportError, AttributeError, TypeError) as e:
            raise StageError(
                "sandbox", f"custom toolset backend {name!r} could not load: {e}"
            ) from e
    try:
        module_path, cls_name, pr = TOOLSET_BACKENDS[name]
    except KeyError:
        raise StageError(
            "sandbox", f"unknown toolset backend {name!r}; have {sorted(TOOLSET_BACKENDS)}"
        ) from None
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        where = (
            f"it is still open upstream as apache/airflow#{pr} — install the provider from that "
            "branch (see scripts/airflow_main.sh)"
            if pr
            else "install apache-airflow-providers-common-ai"
        )
        raise StageError(
            "sandbox", f"toolset backend {name!r} is unavailable ({e}); {where}", retryable=False
        ) from e
    return getattr(module, cls_name)(**kwargs)


class ToolsetSandbox:
    """swfactory's ``Sandbox`` over Airflow's ``SandboxBackend`` (provider ``common.ai``).

    Airflow grew its own sandbox abstraction for agent tool-calling, and its shape is ours:
    create/destroy, run a command, read and write files. Adapting it (rather than reimplementing
    per vendor) means every backend the Airflow community ships — today ``sbx``, and the islo /
    opensandbox / asciibox backends once their pull requests land — becomes a swfactory sandbox
    for free, while the trust boundary is unchanged: the orchestrator still holds the credentials
    and still applies the patch itself.
    """

    def __init__(
        self,
        backend,
        *,
        workdir: str,
        name: str | None = None,
        env: Mapping[str, str] | None = None,
        block_network: bool = True,
        allow_egress_to: Sequence[str] = (),
        repo_root: str | None = None,
        source: str | None = None,
        base_branch: str = "main",
        state: RunState | None = None,
    ) -> None:
        self.backend = backend
        self.workdir = workdir.rstrip("/") or "/"
        self.name = name or f"toolset:{type(backend).__name__}"
        self.env = dict(env or {})
        self.block_network = block_network
        self.allow_egress_to = tuple(allow_egress_to)
        self.repo_root = (repo_root or self.workdir).rstrip("/") or "/"
        self.source = source
        self.base_branch = base_branch
        self.state = state
        self.backend_identity = f"{type(backend).__module__}.{type(backend).__qualname__}"
        self.sandbox_id: str | None = None

    def _restore_id(self) -> None:
        if self.sandbox_id is not None or self.state is None:
            return
        if not self.state.has_control(TOOLSET_STATE_FILE):
            return
        try:
            record = json.loads(self.state.read_control(TOOLSET_STATE_FILE))
            if record.get("backend") != self.backend_identity:
                raise ValueError("backend identity changed")
            sandbox_id = record["sandbox_id"]
            if not isinstance(sandbox_id, str) or not sandbox_id.strip():
                raise ValueError("sandbox id is empty")
        except (AttributeError, KeyError, TypeError, ValueError) as e:
            raise StageError("policy", f"invalid toolset sandbox state: {e}") from e
        self.sandbox_id = sandbox_id

    def _persist_id(self) -> None:
        if self.state is None or self.sandbox_id is None:
            return
        self.state.write_control(
            TOOLSET_STATE_FILE,
            json.dumps(
                {"backend": self.backend_identity, "sandbox_id": self.sandbox_id},
                separators=(",", ":"),
            )
            + "\n",
        )

    def _spec(self):
        """Concrete isolation requirements; a backend must enforce them or refuse creation."""
        try:
            from airflow.providers.common.ai.sandbox.base import SandboxSpec
        except ImportError:
            # Built-in backends cannot reach this branch: ``load_toolset_backend`` imports their
            # Airflow modules first. A standalone custom adapter still receives the exact same
            # mandatory requirements instead of a security-erasing ``spec=None`` fallback.
            return ToolsetRequirements(
                env=self.env,
                block_network=self.block_network,
                allow_egress_to=self.allow_egress_to,
            )
        return SandboxSpec(
            env=self.env,
            block_network=self.block_network,
            allow_egress_to=self.allow_egress_to,
        )

    def ensure(self) -> None:
        """Create the sandbox once, provision the checkout, and select the target directory."""
        self._restore_id()
        if self.sandbox_id is not None:
            try:
                probe = self._run_backend("true", cwd="/", timeout_s=_CONTROL_TIMEOUT_S)
            except Exception:
                probe = None
            if probe is None or not probe.ok:
                self.sandbox_id = None
        if self.sandbox_id is None:
            sandbox_id = self.backend.create(spec=self._spec())
            if not isinstance(sandbox_id, str) or not sandbox_id.strip():
                raise StageError("sandbox", "toolset backend returned an empty sandbox id")
            self.sandbox_id = sandbox_id
            self._persist_id()
        made = self._run_backend(
            f"mkdir -p {shlex.quote(self.repo_root)}", cwd="/", timeout_s=_CONTROL_TIMEOUT_S
        )
        if not made.ok:
            raise StageError("sandbox", f"toolset repository root failed: {made.stderr[-800:]}")
        repo_check = self._run_backend(
            "git rev-parse --is-inside-work-tree",
            cwd=self.repo_root,
            timeout_s=_CONTROL_TIMEOUT_S,
        )
        if self.source and not repo_check.ok:
            parent = posixpath.dirname(self.repo_root) or "/"
            clone = (
                f"git clone --depth 1 --branch {shlex.quote(self.base_branch)} "
                f"{shlex.quote(self.source)} {shlex.quote(posixpath.basename(self.repo_root))}"
            )
            result = self._run_backend(clone, cwd=parent, timeout_s=_CONTROL_TIMEOUT_S * 4)
            if not result.ok:
                raise StageError("sandbox", f"toolset checkout failed: {result.stderr[-800:]}")
        command = (
            f"test -d {shlex.quote(self.workdir)}"
            if self.source
            else f"mkdir -p {shlex.quote(self.workdir)}"
        )
        result = self._run_backend(command, cwd=self.repo_root, timeout_s=_CONTROL_TIMEOUT_S)
        if not result.ok:
            raise StageError(
                "sandbox", f"toolset target directory is unavailable: {result.stderr[-800:]}"
            )

    def _id(self) -> str:
        if self.sandbox_id is None:
            self.ensure()
        assert self.sandbox_id is not None
        return self.sandbox_id

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` in the sandbox; the backend has no cwd, so it travels in the command."""
        return self._run_backend(
            cmd, cwd=self._cwd(cwd) if cwd else self.workdir, timeout_s=timeout_s
        )

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        """Run the model process using authentication declared in the backend's sandbox spec."""

        return self.run(cmd, timeout_s=timeout_s)

    def _run_backend(self, cmd: str, *, cwd: str, timeout_s: int) -> RunResult:
        started = time.monotonic()
        script = f"cd {shlex.quote(cwd)} && {cmd}"
        res = self.backend.run_command(
            self._id(), script, timeout=float(timeout_s), max_output_bytes=TOOLSET_MAX_OUTPUT_BYTES
        )
        # The backend reports three conditions our RunResult has no field for. Dropping them
        # would let truncated output or a dead sandbox read as a clean result, so they are folded
        # into stderr (visible) and, for termination, into a non-zero exit code.
        notes = [
            n
            for n, flag in (
                ("stdout truncated", getattr(res, "stdout_truncated", False)),
                ("stderr truncated", getattr(res, "stderr_truncated", False)),
                ("sandbox terminated", getattr(res, "sandbox_terminated", False)),
            )
            if flag
        ]
        stderr = "\n".join([str(res.stderr or ""), *(f"[toolset] {n}" for n in notes)]).strip()
        terminated = bool(getattr(res, "sandbox_terminated", False))
        exit_code = res.exit_code if isinstance(res.exit_code, int) else 1
        degraded = bool(notes) or bool(res.timed_out)
        return RunResult(
            exit_code=exit_code or (1 if degraded else 0),
            stdout=str(res.stdout or ""),
            stderr=stderr,
            duration_s=round(time.monotonic() - started, 3),
            timed_out=bool(res.timed_out) or terminated,
        )

    def _abs(self, path: str) -> str:
        try:
            value = path if path.startswith("/") else posixpath.join(self.workdir, path)
            return confined_posix_path(self.repo_root, value)
        except ValueError as e:
            raise StageError("policy", str(e)) from e

    def _cwd(self, path: str) -> str:
        try:
            value = path if path.startswith("/") else posixpath.join(self.workdir, path)
            return confined_posix_path(self.workdir, value)
        except ValueError as e:
            raise StageError("policy", str(e)) from e

    def read(self, path: str) -> str:
        abs_path = self._abs(path)
        if not self.run(f"test -f {shlex.quote(abs_path)}", timeout_s=_CONTROL_TIMEOUT_S).ok:
            raise FileNotFoundError(abs_path)
        try:
            return self.backend.read_file(
                self._id(), abs_path, max_bytes=TOOLSET_MAX_OUTPUT_BYTES
            ).decode("utf-8")
        except FileNotFoundError as e:
            raise FileNotFoundError(abs_path) from e
        except Exception as e:
            raise StageError("sandbox", f"toolset could not read {abs_path}: {e}") from e

    def write(self, path: str, content: str) -> None:
        abs_path = self._abs(path)
        parent = abs_path.rsplit("/", 1)[0]
        if parent:
            result = self._run_backend(
                f"mkdir -p {shlex.quote(parent)}", cwd="/", timeout_s=_CONTROL_TIMEOUT_S
            )
            if not result.ok:
                raise StageError("sandbox", f"toolset mkdir failed: {result.stderr[-800:]}")
        self.backend.write_file(self._id(), abs_path, content.encode("utf-8"))

    def exists(self, path: str) -> bool:
        return self.run(f"test -e {shlex.quote(self._abs(path))}", timeout_s=_CONTROL_TIMEOUT_S).ok

    def close(self) -> None:
        """Destroy the sandbox; best effort, the backend's own TTL is the backstop."""
        self._restore_id()
        if self.sandbox_id is None:
            return
        try:
            self.backend.destroy(self.sandbox_id)
        except Exception:
            return
        self.sandbox_id = None
        if self.state is not None:
            self.state.clear_control(TOOLSET_STATE_FILE)


def make_sandbox(
    cfg: Config,
    issue_id: str,
    *,
    protected: Sequence[str] = (),
    repo: str | None = None,
    run_dir: Path | None = None,
) -> Sandbox:
    """Build the sandbox selected by ``cfg.sandbox`` for the run on ``issue_id``.

    ``protected`` (the target's factory.toml globs) becomes srt's kernel-level ``denyWrite`` or
    docker's read-only bind mounts (``set_protected`` changes it later, e.g. to tighten for a fix
    call); ``repo`` (owner/name) makes the islo sandbox name unique per (issue, target).
    """
    claude_env = ("ANTHROPIC_API_KEY",) if cfg.agent == "claude" else ()
    if cfg.sandbox == "local":
        return LocalSandbox(Path(cfg.workdir))
    if cfg.sandbox == "srt":
        return SrtSandbox(
            Path(cfg.workdir),
            allowed_domains=cfg.srt_allowed_domains,
            protected=protected,
            pass_env=claude_env,
        )
    if cfg.sandbox == "toolset":
        repo_root = cfg.toolset_workdir.rstrip("/") or "/workspace/repo"
        target = normalize_relative_path(cfg.target_dir, field="target_dir", allow_empty=True)
        workdir = posixpath.join(repo_root, target) if target else repo_root
        return ToolsetSandbox(
            load_toolset_backend(cfg.toolset_backend),
            workdir=workdir,
            env={k: os.environ[k] for k in claude_env if k in os.environ},
            allow_egress_to=_dedupe([*cfg.srt_allowed_domains, *SRT_CLAUDE_DOMAINS, "github.com"]),
            repo_root=repo_root,
            source=f"https://github.com/{cfg.repo}.git",
            base_branch=cfg.base_branch,
            state=RunState(run_dir) if run_dir is not None else None,
        )
    if cfg.sandbox == "docker":
        return DockerSandbox(
            Path(cfg.workdir),
            image=cfg.docker_image,
            pass_env=claude_env if cfg.docker_credentials == "env" else (),
            credentials=cfg.docker_credentials,
            protected=protected,
            network=cfg.docker_network,
            user=cfg.docker_user or default_docker_user(),
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
        owner=cfg.sandbox_owner,
    )
