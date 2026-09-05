"""Path and identifier policy shared by every trust boundary.

The factory accepts names from issue front matter, blueprint files, environment variables and
CLI flags.  Those values eventually become directories, Git refs and remote sandbox paths.  Keep
their validation in one small, dependency-free module so each adapter cannot invent subtly
different traversal rules.
"""

from __future__ import annotations

import os
import posixpath
import re
from pathlib import Path, PurePosixPath

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")
_REPO_PART_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_INVALID_REF_RE = re.compile(r"[\x00-\x20~^:?*\\[\\]]")


def validate_identifier(value: str, *, field: str = "identifier") -> str:
    """Return a filesystem/Git-safe external identifier or raise ``ValueError``.

    Dots are permitted inside issue ids for compatibility with external trackers, but separators,
    whitespace, control characters and the special ``.`` / ``..`` names are not.
    """

    if not _ID_RE.fullmatch(value) or value in {".", ".."}:
        raise ValueError(
            f"{field} must be 1-128 characters: letters, digits, '.', '_' or '-', "
            "starting with a letter or digit"
        )
    return value


def validate_run_id(value: str) -> str:
    """Return a bounded run id suitable for scratch paths and sandbox names."""

    if not _RUN_ID_RE.fullmatch(value):
        raise ValueError(
            "run_id must be 1-32 letters, digits, '_' or '-', starting with a letter or digit"
        )
    return value


def validate_repo(value: str) -> str:
    """Validate the ``owner/name`` shape used in GitHub and ``github://`` source URLs."""

    parts = value.split("/")
    if len(parts) != 2 or any(not _REPO_PART_RE.fullmatch(part) for part in parts):
        raise ValueError("repo must be an owner/name pair using GitHub-safe characters")
    if any(part in {".", ".."} or part.endswith(".lock") for part in parts):
        raise ValueError("repo contains a reserved path component")
    return value


def validate_git_ref(value: str, *, field: str = "base_branch") -> str:
    """Conservatively validate a branch/ref name without invoking Git.

    This mirrors the dangerous cases rejected by ``git check-ref-format --branch`` while keeping
    validation deterministic on workers that may not have Git installed at DAG-parse time.
    """

    parts = value.split("/")
    invalid = (
        not value
        or value == "@"
        or _INVALID_REF_RE.search(value) is not None
        or value.startswith("-")
        or value.startswith("/")
        or value.endswith(("/", "."))
        or ".." in value
        or "//" in value
        or "@{" in value
        or any(not part or part.startswith(".") or part.endswith(".lock") for part in parts)
    )
    if invalid:
        raise ValueError(f"{field} is not a safe Git ref: {value!r}")
    return value


def normalize_relative_path(
    value: str, *, field: str = "path", allow_empty: bool = False
) -> str:
    """Normalize a portable POSIX relative path and reject traversal or platform ambiguity."""

    if not isinstance(value, str) or _CONTROL_RE.search(value):
        raise ValueError(f"{field} contains control characters")
    if not value:
        if allow_empty:
            return ""
        raise ValueError(f"{field} must not be empty")
    if value != value.strip() or "\\" in value or _WINDOWS_DRIVE_RE.match(value):
        raise ValueError(f"{field} must be a clean POSIX relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field} must stay inside its root: {value!r}")
    normalized = path.as_posix()
    if normalized in {"", "."}:
        if allow_empty:
            return ""
        raise ValueError(f"{field} must name a path")
    return normalized


def normalize_absolute_posix_path(value: str, *, field: str = "path") -> str:
    """Normalize a non-root absolute path used inside a remote sandbox."""

    if not isinstance(value, str) or _CONTROL_RE.search(value):
        raise ValueError(f"{field} contains control characters")
    if value != value.strip() or "\\" in value or _WINDOWS_DRIVE_RE.match(value):
        raise ValueError(f"{field} must be a clean absolute POSIX path")
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts or path.as_posix() == "/":
        raise ValueError(f"{field} must be an absolute sandbox path below /")
    return path.as_posix().rstrip("/")


def confined_path(root: Path, value: str | os.PathLike[str]) -> Path:
    """Resolve ``value`` and require it to remain below ``root`` (symlinks included)."""

    boundary = Path(root).resolve()
    raw = Path(value)
    candidate = raw if raw.is_absolute() else boundary / raw
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(boundary):
        raise ValueError(f"path escapes sandbox root {boundary}: {value!s}")
    return resolved


def confined_posix_path(root: str, value: str) -> str:
    """POSIX equivalent of :func:`confined_path` for remote sandbox paths."""

    boundary = posixpath.normpath(root)
    candidate = posixpath.normpath(
        value if value.startswith("/") else posixpath.join(boundary, value)
    )
    try:
        inside = posixpath.commonpath((boundary, candidate)) == boundary
    except ValueError:
        inside = False
    if not inside:
        raise ValueError(f"path escapes sandbox root {boundary}: {value}")
    return candidate
