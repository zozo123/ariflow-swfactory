"""Commit-bound candidate execution recipe contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from support import git, make_repo

from swfactory.execution_recipe import ExecutionRecipeError, load_execution_recipe

RECIPE = {
    "schema_version": 1,
    "argv": ["uv", "run", "pytest", "-q"],
    "cwd": ".",
    "timeout_s": 1800,
    "resources": {"cpus": 4, "memory_mb": 8192},
    "environment": {"PYTHONHASHSEED": "0"},
    "secret_env": [],
}


def _repo(tmp_path: Path) -> tuple[Path, str]:
    return make_repo(tmp_path, {".swfactory/candidate-run.json": json.dumps(RECIPE) + "\n"})


def _commit_recipe(repo: Path, **changes: Any) -> str:
    """Commit ``RECIPE`` with ``changes`` over its top-level fields; the new commit's sha."""
    (repo / ".swfactory" / "candidate-run.json").write_text(json.dumps({**RECIPE, **changes}) + "\n", encoding="utf-8")
    git(repo, "commit", "-qam", "recipe")
    return git(repo, "rev-parse", "HEAD")


def test_recipe_is_read_from_recorded_commit_not_dirty_checkout(tmp_path: Path) -> None:
    repo, commit = _repo(tmp_path)
    recipe_path = repo / ".swfactory" / "candidate-run.json"
    recipe_path.write_text('{"schema_version":1,"argv":["evil"]}\n', encoding="utf-8")

    bound = load_execution_recipe(repo, commit)

    assert bound.commit_sha == commit
    assert bound.recipe.argv == ("uv", "run", "pytest", "-q")
    assert bound.recipe.cpus == 4
    assert bound.recipe.memory_mb == 8192
    assert bound.recipe.environment == (("PYTHONHASHSEED", "0"),)
    assert bound.recipe.secret_env == ()
    assert len(bound.digest) == 64


def test_recipe_digest_changes_when_committed_execution_contract_changes(tmp_path: Path) -> None:
    repo, first_commit = _repo(tmp_path)
    first = load_execution_recipe(repo, first_commit)
    second = load_execution_recipe(repo, _commit_recipe(repo, resources={**RECIPE["resources"], "cpus": 8}))

    assert second.digest != first.digest
    assert second.commit_sha != first.commit_sha


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        pytest.param(
            {"environment": {**RECIPE["environment"], "API_KEY": "do-not-commit-this"}},
            "looks secret",
            id="secret_value_in_public_environment",
        ),
        pytest.param({"cwd": "/tmp"}, "escapes repository", id="cwd_escapes-/tmp"),
        pytest.param({"cwd": "../outside"}, "escapes repository", id="cwd_escapes-../outside"),
        pytest.param({"cwd": "safe/../../outside"}, "escapes repository", id="cwd_escapes-safe/../../outside"),
        pytest.param({"cwd": 123}, "cwd must be a string", id="non_string_cwd"),
        pytest.param(
            {"privileged": True}, "unknown execution recipe fields", id="unknown_field_is_not_silently_ignored"
        ),
        pytest.param({"secret_env": ["GH_TOKEN"]}, "secret_env is retired", id="secret_env_injection"),
    ],
)
def test_committed_recipe_is_refused(tmp_path: Path, changes: dict[str, Any], match: str) -> None:
    repo, _ = _repo(tmp_path)
    _commit_recipe(repo, **changes)
    with pytest.raises(ExecutionRecipeError, match=match):
        load_execution_recipe(repo, "HEAD")


def test_option_like_revision_and_escaping_recipe_path_are_refused(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    with pytest.raises(ExecutionRecipeError, match="invalid Git revision"):
        load_execution_recipe(repo, "--help")
    with pytest.raises(ExecutionRecipeError, match="escapes repository"):
        load_execution_recipe(repo, "HEAD", path="../candidate-run.json")
