"""Isolated Git worktrees for parallel candidate exploration.

Each candidate receives a distinct detached worktree at the exact input commit.
A successful candidate can then be frozen under an immutable factory-owned Git
ref before its disposable worktree is removed. This turns candidate isolation
and lineage into repository facts rather than conventions.

The module does not schedule candidates, approve them, or publish them. Airflow
and the existing promotion authority remain unchanged.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


class CandidateWorktreeError(RuntimeError):
    """A candidate workspace could not be created, frozen, or removed safely."""


@dataclass(frozen=True)
class CandidateWorktree:
    candidate_id: str
    input_head: str
    path: str
    ref: str
    repo: str
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateRevision:
    candidate_id: str
    input_head: str
    output_head: str
    ref: str
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_ref(candidate_id: str) -> str:
    """Return a stable ref without putting user-controlled text in a ref name."""
    _validate_candidate_id(candidate_id)
    token = hashlib.sha256(candidate_id.encode()).hexdigest()[:24]
    return f"refs/swfactory/candidates/{token}"


def create_candidate_worktree(
    repo: Path,
    candidate_id: str,
    input_head: str,
    *,
    root: Path,
) -> CandidateWorktree:
    """Create a new detached worktree for exactly one candidate and input head."""
    _validate_candidate_id(candidate_id)
    _validate_revision(input_head)
    repo = repo.resolve()
    if not repo.is_dir():
        raise CandidateWorktreeError(f"repository does not exist: {repo}")
    commit = _git(repo, "rev-parse", "--verify", f"{input_head}^{{commit}}").strip()
    if not commit:
        raise CandidateWorktreeError(f"input head did not resolve to a commit: {input_head}")

    ref = candidate_ref(candidate_id)
    existing = _ref_head(repo, ref)
    if existing is not None:
        raise CandidateWorktreeError(f"candidate {candidate_id} is already frozen at {existing}")

    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    _restrict_dir(root)
    token = ref.rsplit("/", 1)[-1]
    destination = root / token
    if destination.exists():
        raise CandidateWorktreeError(f"candidate worktree already exists: {destination}")

    try:
        _git(repo, "worktree", "add", "--detach", str(destination), commit)
        observed = _git(destination, "rev-parse", "HEAD").strip()
        if observed != commit:
            raise CandidateWorktreeError(f"candidate worktree head {observed} != expected {commit}")
        common = Path(_git(destination, "rev-parse", "--path-format=absolute", "--git-common-dir").strip())
        expected_common = Path(_git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir").strip())
        if common.resolve() != expected_common.resolve():
            raise CandidateWorktreeError("candidate worktree is attached to a different Git repository")
    except BaseException:
        if destination.exists():
            _remove_path(repo, destination, force=True)
        raise

    return CandidateWorktree(
        candidate_id=candidate_id,
        input_head=commit,
        path=str(destination),
        ref=ref,
        repo=str(repo),
    )


def freeze_candidate_worktree(worktree: CandidateWorktree) -> CandidateRevision:
    """Freeze one clean, changed candidate HEAD under an immutable factory ref."""
    repo = Path(worktree.repo).resolve()
    raw_path = Path(worktree.path)
    if raw_path.is_symlink():
        raise CandidateWorktreeError(f"candidate worktree path is a symlink: {raw_path}")
    path = raw_path.resolve()
    _verify_membership(repo, path)

    status = _git(path, "status", "--porcelain=v1", "--untracked-files=all")
    if status.strip():
        raise CandidateWorktreeError(
            f"candidate {worktree.candidate_id} has uncommitted files; commit or discard them before freeze"
        )

    input_head = _git(repo, "rev-parse", "--verify", f"{worktree.input_head}^{{commit}}").strip()
    output_head = _git(path, "rev-parse", "--verify", "HEAD^{commit}").strip()
    if input_head != worktree.input_head:
        raise CandidateWorktreeError("candidate input head no longer resolves to its recorded commit")
    if output_head == input_head:
        raise CandidateWorktreeError("candidate did not advance beyond its input head")
    ancestor = _git_proc(repo, "merge-base", "--is-ancestor", input_head, output_head)
    if ancestor.returncode != 0:
        raise CandidateWorktreeError(
            f"candidate output {output_head} does not descend from recorded input {input_head}"
        )

    existing = _ref_head(repo, worktree.ref)
    if existing is not None:
        if existing != output_head:
            raise CandidateWorktreeError(
                f"candidate is already frozen at {existing}; refusing to rewrite it to {output_head}"
            )
        return CandidateRevision(
            worktree.candidate_id,
            input_head,
            output_head,
            worktree.ref,
        )

    proc = _git_proc(repo, "update-ref", worktree.ref, output_head, "0" * 40)
    if proc.returncode != 0:
        raced = _ref_head(repo, worktree.ref)
        if raced == output_head:
            return CandidateRevision(worktree.candidate_id, input_head, output_head, worktree.ref)
        detail = proc.stderr.strip()
        raise CandidateWorktreeError(f"could not freeze candidate ref {worktree.ref}: {detail}")

    return CandidateRevision(
        candidate_id=worktree.candidate_id,
        input_head=input_head,
        output_head=output_head,
        ref=worktree.ref,
    )


def verify_candidate_revision(repo: Path, revision: CandidateRevision) -> None:
    """Fail closed if the immutable candidate ref no longer names its recorded output."""
    repo = repo.resolve()
    observed = _ref_head(repo, revision.ref)
    if observed != revision.output_head:
        raise CandidateWorktreeError(
            f"candidate ref drift: expected {revision.output_head}, observed {observed or 'missing'}"
        )


def remove_candidate_worktree(worktree: CandidateWorktree, *, force: bool = False) -> None:
    """Remove disposable workspace state without deleting a frozen candidate ref."""
    repo = Path(worktree.repo).resolve()
    raw_path = Path(worktree.path)
    if raw_path.is_symlink():
        raise CandidateWorktreeError(f"candidate worktree path is a symlink: {raw_path}")
    path = raw_path.resolve()
    if not path.exists():
        _git(repo, "worktree", "prune")
        return
    _verify_membership(repo, path)
    _remove_path(repo, path, force=force)
    _git(repo, "worktree", "prune")


def _remove_path(repo: Path, path: Path, *, force: bool) -> None:
    args = ["worktree", "remove"]
    if force:
        args.append("--force")
    args.append(str(path))
    proc = _git_proc(repo, *args)
    if proc.returncode != 0:
        detail = proc.stderr.strip()
        raise CandidateWorktreeError(f"could not remove candidate worktree {path}: {detail}")


def _verify_membership(repo: Path, path: Path) -> None:
    if not path.is_dir():
        raise CandidateWorktreeError(f"candidate worktree is absent: {path}")
    common = Path(_git(path, "rev-parse", "--path-format=absolute", "--git-common-dir").strip())
    expected = Path(_git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir").strip())
    if common.resolve() != expected.resolve():
        raise CandidateWorktreeError(f"{path} is not a worktree of {repo}")


def _ref_head(repo: Path, ref: str) -> str | None:
    proc = _git_proc(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if proc.returncode != 0:
        return None
    value = proc.stdout.strip()
    return value or None


def _validate_candidate_id(candidate_id: str) -> None:
    if not candidate_id or not candidate_id.strip():
        raise CandidateWorktreeError("candidate id must be nonempty")
    if "\x00" in candidate_id or "\n" in candidate_id or "\r" in candidate_id:
        raise CandidateWorktreeError("candidate id contains a control character")


def _validate_revision(revision: str) -> None:
    if not revision or revision.startswith("-") or "\x00" in revision or "\n" in revision or "\r" in revision:
        raise CandidateWorktreeError(f"invalid Git revision: {revision!r}")


def _git(repo: Path, *args: str) -> str:
    proc = _git_proc(repo, *args)
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise CandidateWorktreeError(f"git {' '.join(args)} failed: {detail}")
    return proc.stdout


def _git_proc(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
        env=env,
    )


def _restrict_dir(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o700)
