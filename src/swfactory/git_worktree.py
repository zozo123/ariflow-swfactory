"""Detached Git worktrees for local candidate isolation.

A candidate worktree is a disposable execution workspace rooted at one exact
recorded commit. It has no branch ref and therefore grants no publication or
promotion authority. Candidate code may commit inside the detached worktree;
the resulting HEAD becomes evidence only when the workspace is clean.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


class CandidateWorktreeError(RuntimeError):
    """A candidate worktree could not be created, verified, or removed safely."""


@dataclass(frozen=True)
class CandidateWorktree:
    candidate_id: str
    base_sha: str
    path: str
    workspace_key: str
    schema_version: int = 1

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def create_candidate_worktree(
    repo: Path,
    candidate_id: str,
    base_revision: str,
    worktree_root: Path,
) -> CandidateWorktree:
    """Create one detached workspace at an exact commit.

    The path is derived from a digest of candidate identity rather than from
    candidate-controlled text. Existing destinations are refused so a replay
    cannot accidentally inherit stale filesystem state.
    """

    repo = Path(repo).resolve()
    root = Path(worktree_root).resolve()
    if not repo.is_dir():
        raise CandidateWorktreeError(f"repository does not exist: {repo}")
    if not candidate_id.strip():
        raise CandidateWorktreeError("candidate id must be nonempty")
    if not base_revision.strip() or base_revision.startswith("-"):
        raise CandidateWorktreeError(f"invalid Git revision: {base_revision!r}")

    base_sha = _git(repo, "rev-parse", "--verify", f"{base_revision}^{{commit}}").strip()
    key = hashlib.sha256(candidate_id.encode()).hexdigest()[:24]
    root.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        root.chmod(0o700)
    path = root / f"cand-{key}"
    reservation = root / f".cand-{key}.reserve"
    if path.exists() or path.is_symlink():
        raise CandidateWorktreeError(f"candidate workspace already exists: {path}")

    try:
        fd = os.open(reservation, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise CandidateWorktreeError(f"candidate workspace is already being created: {path}") from error
    else:
        os.close(fd)

    added = False
    try:
        _git(repo, "worktree", "add", "--detach", str(path), base_sha)
        added = True
        observed = _git(path, "rev-parse", "HEAD").strip()
        if observed != base_sha:
            raise CandidateWorktreeError(f"worktree HEAD {observed} != requested base {base_sha}")
        return CandidateWorktree(
            candidate_id=candidate_id,
            base_sha=base_sha,
            path=str(path),
            workspace_key=key,
        )
    except BaseException:
        if added:
            _remove_registered(repo, path, force=True)
        raise
    finally:
        reservation.unlink(missing_ok=True)


def recorded_candidate_head(worktree: CandidateWorktree) -> str:
    """Return the exact candidate commit, refusing uncommitted workspace state."""

    path = Path(worktree.path)
    if not path.is_dir() or path.is_symlink():
        raise CandidateWorktreeError(f"candidate workspace is absent or invalid: {path}")
    status = _git(path, "status", "--porcelain=v1", "--untracked-files=all")
    if status.strip():
        raise CandidateWorktreeError(
            "candidate workspace has uncommitted state; commit or discard it before recording HEAD"
        )
    return _git(path, "rev-parse", "--verify", "HEAD^{commit}").strip()


def remove_candidate_worktree(
    repo: Path,
    worktree: CandidateWorktree,
    *,
    force: bool = False,
) -> None:
    """Remove exactly the registered candidate workspace.

    Without force Git refuses dirty worktrees. Infrastructure cleanup may use
    force=True after retaining whatever failure evidence it needs.
    """

    repo = Path(repo).resolve()
    path = Path(worktree.path).resolve()
    _remove_registered(repo, path, force=force)


def _remove_registered(repo: Path, path: Path, *, force: bool) -> None:
    registered = _registered_worktrees(repo)
    if path not in registered:
        if path.exists():
            raise CandidateWorktreeError(f"refusing to remove unregistered workspace: {path}")
        return
    args = ["worktree", "remove"]
    if force:
        args.append("--force")
    args.append(str(path))
    _git(repo, *args)
    _git(repo, "worktree", "prune")


def _registered_worktrees(repo: Path) -> set[Path]:
    output = _git(repo, "worktree", "list", "--porcelain")
    found: set[Path] = set()
    for line in output.splitlines():
        if line.startswith("worktree "):
            found.add(Path(line.removeprefix("worktree ")).resolve())
    return found


def _git(repo: Path, *args: str) -> str:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
        env=env,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise CandidateWorktreeError(f"git {' '.join(args)} failed: {detail}")
    return proc.stdout
