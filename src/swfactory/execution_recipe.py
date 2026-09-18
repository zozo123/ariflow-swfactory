"""Commit-bound execution recipes for reproducible candidate verification.

Source snapshots make input bytes immutable. This module closes the adjacent
configuration gap: command, public environment, secret names, working directory,
timeout, and resource shape are loaded from the same recorded Git commit.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any


class ExecutionRecipeError(RuntimeError):
    """A committed execution recipe is missing, unsafe, or malformed."""


@dataclass(frozen=True)
class ExecutionRecipe:
    argv: tuple[str, ...]
    cwd: str
    timeout_s: int
    cpus: int
    memory_mb: int
    environment: tuple[tuple[str, str], ...] = ()
    secret_env: tuple[str, ...] = ()
    schema_version: int = 1

    @property
    def digest(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        return hashlib.sha256(payload).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        document = asdict(self)
        document["environment"] = dict(self.environment)
        document["secret_env"] = list(self.secret_env)
        document["digest"] = self.digest
        return document


@dataclass(frozen=True)
class BoundExecutionRecipe:
    commit_sha: str
    path: str
    recipe: ExecutionRecipe
    schema_version: int = 1

    @property
    def digest(self) -> str:
        return self.recipe.digest

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "commit_sha": self.commit_sha,
            "path": self.path,
            "recipe": self.recipe.to_dict(),
            "digest": self.digest,
        }


def load_execution_recipe(
    repo: Path,
    revision: str,
    *,
    path: str = ".swfactory/candidate-run.json",
) -> BoundExecutionRecipe:
    """Load and validate a recipe from exactly one Git revision and path."""
    repo = Path(repo).resolve()
    if not repo.is_dir():
        raise ExecutionRecipeError(f"repository does not exist: {repo}")
    _validate_revision(revision)
    normalized_path = _validate_repo_path(path)
    commit = _git(repo, "rev-parse", "--verify", f"{revision}^{{commit}}").strip()
    raw = _git_bytes(repo, "show", f"{commit}:{normalized_path}")
    if len(raw) > 64 * 1024:
        raise ExecutionRecipeError("execution recipe exceeds 64 KiB")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ExecutionRecipeError(f"execution recipe is not valid UTF-8 JSON: {error}") from error
    recipe = parse_execution_recipe(document)
    return BoundExecutionRecipe(commit_sha=commit, path=normalized_path, recipe=recipe)


def parse_execution_recipe(document: Any) -> ExecutionRecipe:
    if not isinstance(document, dict):
        raise ExecutionRecipeError("execution recipe must be a JSON object")
    allowed = {
        "schema_version",
        "argv",
        "cwd",
        "timeout_s",
        "resources",
        "environment",
        "secret_env",
    }
    unknown = sorted(set(document) - allowed)
    if unknown:
        raise ExecutionRecipeError("unknown execution recipe fields: " + ", ".join(unknown))
    if document.get("schema_version") != 1:
        raise ExecutionRecipeError("execution recipe schema_version must be 1")

    argv_raw = document.get("argv")
    if not isinstance(argv_raw, list) or not argv_raw:
        raise ExecutionRecipeError("execution recipe argv must be a nonempty array")
    argv: list[str] = []
    for item in argv_raw:
        if not isinstance(item, str) or not item or "\x00" in item or "\n" in item or "\r" in item:
            raise ExecutionRecipeError("execution recipe argv contains an invalid argument")
        argv.append(item)

    cwd = _validate_repo_path(str(document.get("cwd", ".")))

    timeout_s = document.get("timeout_s")
    if not isinstance(timeout_s, int) or isinstance(timeout_s, bool) or not 1 <= timeout_s <= 86400:
        raise ExecutionRecipeError("execution recipe timeout_s must be an integer in [1, 86400]")

    resources = document.get("resources")
    if not isinstance(resources, dict) or set(resources) != {"cpus", "memory_mb"}:
        raise ExecutionRecipeError("execution recipe resources must contain exactly cpus and memory_mb")
    cpus = resources["cpus"]
    memory_mb = resources["memory_mb"]
    if not isinstance(cpus, int) or isinstance(cpus, bool) or not 1 <= cpus <= 128:
        raise ExecutionRecipeError("execution recipe cpus must be an integer in [1, 128]")
    if not isinstance(memory_mb, int) or isinstance(memory_mb, bool) or not 128 <= memory_mb <= 1048576:
        raise ExecutionRecipeError("execution recipe memory_mb must be an integer in [128, 1048576]")

    environment_raw = document.get("environment", {})
    if not isinstance(environment_raw, dict):
        raise ExecutionRecipeError("execution recipe environment must be an object")
    environment: list[tuple[str, str]] = []
    for key, value in environment_raw.items():
        _validate_env_name(key)
        if not isinstance(value, str) or "\x00" in value:
            raise ExecutionRecipeError(f"environment value for {key!r} must be a string")
        if _looks_secret(key):
            raise ExecutionRecipeError(f"environment key {key!r} looks secret; record only its name in secret_env")
        environment.append((key, value))

    secret_raw = document.get("secret_env", [])
    if not isinstance(secret_raw, list):
        raise ExecutionRecipeError("execution recipe secret_env must be an array")
    secret_env: list[str] = []
    for key in secret_raw:
        _validate_env_name(key)
        secret_env.append(key)
    if len(secret_env) != len(set(secret_env)):
        raise ExecutionRecipeError("execution recipe secret_env contains duplicates")
    overlap = set(secret_env).intersection(key for key, _ in environment)
    if overlap:
        raise ExecutionRecipeError("environment and secret_env overlap: " + ", ".join(sorted(overlap)))

    return ExecutionRecipe(
        argv=tuple(argv),
        cwd=cwd,
        timeout_s=timeout_s,
        cpus=cpus,
        memory_mb=memory_mb,
        environment=tuple(sorted(environment)),
        secret_env=tuple(sorted(secret_env)),
    )


def _validate_repo_path(path: str) -> str:
    if not path or "\x00" in path or "\n" in path or "\r" in path:
        raise ExecutionRecipeError("execution recipe path must be nonempty")
    candidate = PurePosixPath(path.replace("\\", "/"))
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ExecutionRecipeError(f"execution recipe path escapes repository: {path!r}")
    normalized = str(candidate)
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized or "."


def _validate_revision(revision: str) -> None:
    if not revision or revision.startswith("-") or any(ch in revision for ch in ("\x00", "\n", "\r")):
        raise ExecutionRecipeError(f"invalid Git revision: {revision!r}")


def _validate_env_name(value: Any) -> None:
    if not isinstance(value, str) or not value:
        raise ExecutionRecipeError("environment names must be nonempty strings")
    if not (value[0].isalpha() or value[0] == "_"):
        raise ExecutionRecipeError(f"invalid environment name: {value!r}")
    if not all(ch.isalnum() or ch == "_" for ch in value):
        raise ExecutionRecipeError(f"invalid environment name: {value!r}")


def _looks_secret(name: str) -> bool:
    upper = name.upper()
    return any(token in upper for token in ("TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL", "API_KEY"))


def _git(repo: Path, *args: str) -> str:
    return _git_bytes(repo, *args).decode()


def _git_bytes(repo: Path, *args: str) -> bytes:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        check=False,
        timeout=120,
        env=env,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode(errors="replace").strip()
        raise ExecutionRecipeError(f"git {' '.join(args)} failed: {detail}")
    return proc.stdout
