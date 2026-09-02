"""Sandboxes: where the target repo is checked out and where commands run.

Two implementations share one protocol:

* ``LocalSandbox`` — a directory on the orchestrator host, used by the scripted demo and tests.
  The child environment is scrubbed of every credential prefix in ``SCRUB_PREFIXES``.
* ``IsloSandbox`` — an islo MicroVM. It receives a read-only clone via ``--source`` and the
  gateway's phantom ``ANTHROPIC_API_KEY``; it never receives a GitHub write token. The argv it
  builds never carries ``--env`` / ``--env-file`` (unit-tested invariant).

No environment passthrough exists on this protocol, by design.
"""

from __future__ import annotations

import os
import posixpath
import shlex
import subprocess
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol, runtime_checkable

from swfactory.config import Config
from swfactory.models import RunResult, StageError

# Exit code reported when a command is killed by the timeout (mirrors coreutils `timeout`).
TIMEOUT_EXIT_CODE = 124
# Bound for the short control-plane calls (`islo cp`, `islo rm`, `test -e`).
_CONTROL_TIMEOUT_S = 300


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


class IsloSandbox:
    """An islo MicroVM created from ``--source`` (read-only clone) and addressed by a stable name.

    The same name on every call makes ``islo use`` create-if-needed the idempotency mechanism
    across Airflow tasks and workers.
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
    ) -> None:
        self.name = name
        self.source = source
        self.gateway_profile = gateway_profile
        self.environment = environment
        self.ttl_s = ttl_s
        self.idle_s = idle_s
        self.factory_root = Path(factory_root)
        repo_name = _repo_name(source)
        self.workdir = f"/workspace/{repo_name}/{target_dir}".rstrip("/")

    def argv(self, cmd: str, *, cwd: str | None = None, create: bool = False) -> list[str]:
        """Build the ``islo use`` argv for ``cmd``.

        INVARIANT: the result never contains ``--env``, ``--env-file``, ``ANTHROPIC`` or
        ``GH_TOKEN``. Credentials reach the sandbox only through the islo environment/gateway.
        """
        if create:
            return [
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
                "--output",
                "plain",
                "--",
                "true",
            ]
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
        return _run_subprocess(
            ["islo", "cp", src, dst],
            cwd=None,
            env=None,
            timeout_s=_CONTROL_TIMEOUT_S,
        )

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


def make_sandbox(cfg: Config, issue_id: str) -> Sandbox:
    """Build the sandbox selected by ``cfg.sandbox`` for the run on ``issue_id``."""
    if cfg.sandbox == "local":
        return LocalSandbox(Path(cfg.workdir))
    return IsloSandbox(
        cfg.sandbox_name(issue_id),
        source=f"github://{cfg.repo}:{cfg.base_branch}",
        gateway_profile=cfg.gateway_profile,
        environment=cfg.islo_environment,
        ttl_s=cfg.sandbox_ttl_s,
        idle_s=cfg.sandbox_idle_s,
        target_dir=cfg.target_dir,
        factory_root=_factory_root(),
    )
