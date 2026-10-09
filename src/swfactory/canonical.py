"""Canonical digests, owner-only writes and the Git runner shared by the search and evidence contracts.

These digests are persisted and recomputed by verifiers, so their byte encoding is fixed. Modules
with a different encoding (ensure_ascii, default=str, allow_nan=False, no prefix) keep their own.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def json_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def require_sha256(value: str, *, field: str, error: type[Exception] = ValueError) -> None:
    raw = value.removeprefix("sha256:")
    if len(raw) != 64 or any(char not in "0123456789abcdef" for char in raw):
        raise error(f"{field} must be a canonical sha256 digest")


def run_git(repo: Path, *args: str, text: bool = True) -> subprocess.CompletedProcess[Any]:
    """``git -C repo args``: never prompts for a credential, bounded to 120 s, never raises on exit status."""
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=text,
        check=False,
        timeout=120,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )


def git_output(repo: Path, *args: str, error: type[Exception], text: bool = True) -> Any:
    """``git``'s stdout (``bytes`` when ``text`` is false), or ``error`` carrying git's stderr."""
    proc = run_git(repo, *args, text=text)
    if proc.returncode != 0:
        detail = proc.stderr if text else proc.stderr.decode(errors="replace")
        raise error(f"git {' '.join(args)} failed: {detail.strip()}")
    return proc.stdout


def file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(128 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def restrict_dir(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o700)


def restrict_file(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o600)


def atomic_json(path: Path, document: Mapping[str, Any]) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        temporary.write_text(json.dumps(dict(document), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        restrict_file(temporary)
        os.replace(temporary, path)
        restrict_file(path)
    finally:
        temporary.unlink(missing_ok=True)
