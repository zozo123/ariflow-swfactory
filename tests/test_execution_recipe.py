"""Commit-bound candidate execution recipe contracts."""

from __future__ import annotations

import json
from pathlib import Path

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
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["resources"]["cpus"] = 8
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "more cpu")
    second_commit = git(repo, "rev-parse", "HEAD")
    second = load_execution_recipe(repo, second_commit)

    assert second.digest != first.digest
    assert second.commit_sha != first.commit_sha


def test_secret_values_are_refused_from_committed_public_environment(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["environment"]["API_KEY"] = "do-not-commit-this"
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "bad secret")

    with pytest.raises(ExecutionRecipeError, match="looks secret"):
        load_execution_recipe(repo, "HEAD")


@pytest.mark.parametrize("cwd", ["/tmp", "../outside", "safe/../../outside"])
def test_recipe_cwd_cannot_escape_repository(tmp_path: Path, cwd: str) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["cwd"] = cwd
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "bad cwd")

    with pytest.raises(ExecutionRecipeError, match="escapes repository"):
        load_execution_recipe(repo, "HEAD")


def test_recipe_rejects_non_string_cwd(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    recipe_path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(recipe_path.read_text(encoding="utf-8"))
    document["cwd"] = 123
    recipe_path.write_text(json.dumps(document), encoding="utf-8")
    git(repo, "add", ".swfactory/candidate-run.json")
    git(repo, "commit", "-q", "-m", "bad cwd type")

    with pytest.raises(ExecutionRecipeError, match="cwd must be a string"):
        load_execution_recipe(repo, "HEAD")


def test_recipe_rejects_unknown_fields_instead_of_silently_ignoring_policy(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["privileged"] = True
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "unknown policy")

    with pytest.raises(ExecutionRecipeError, match="unknown execution recipe fields"):
        load_execution_recipe(repo, "HEAD")


def test_option_like_revision_and_escaping_recipe_path_are_refused(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    with pytest.raises(ExecutionRecipeError, match="invalid Git revision"):
        load_execution_recipe(repo, "--help")
    with pytest.raises(ExecutionRecipeError, match="escapes repository"):
        load_execution_recipe(repo, "HEAD", path="../candidate-run.json")


def test_public_recipe_rejects_secret_env_injection(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["secret_env"] = ["GH_TOKEN"]
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "forbidden secret injection")

    with pytest.raises(ExecutionRecipeError, match="secret_env is retired"):
        load_execution_recipe(repo, "HEAD")
