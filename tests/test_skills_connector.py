import subprocess

import pytest

from swfactory.models import StageError
from swfactory.skills_connector import (
    SKILLS_CLI,
    SKILLS_CLI_VERSION,
    SWFACTORY_HARNESS_SKILL,
    SWFACTORY_SKILL,
    VERCEL_FIND_SKILLS,
    catalog,
    find_argv,
    install_argv,
    list_argv,
    run,
)


def test_canonical_skill_packages_are_explicit() -> None:
    assert ("npx", "-y", f"skills@{SKILLS_CLI_VERSION}") == SKILLS_CLI
    assert "latest" not in SKILLS_CLI
    assert SWFACTORY_SKILL.source == "zozo123/ariflow-swfactory@airflow-software-factory"
    assert SWFACTORY_SKILL.catalog_url.endswith("/zozo123/ariflow-swfactory/airflow-software-factory")
    assert SWFACTORY_HARNESS_SKILL.source == "zozo123/ariflow-swfactory@swfactory"
    assert VERCEL_FIND_SKILLS.source == "vercel-labs/skills@find-skills"
    assert catalog()["skills_cli_version"] == SKILLS_CLI_VERSION
    assert catalog()["factory_catalog"] == SWFACTORY_SKILL.catalog_url
    assert catalog()["harness_catalog"] == SWFACTORY_HARNESS_SKILL.catalog_url
    assert catalog()["vercel_repository"] == "https://github.com/vercel-labs/skills"


def test_find_argv_is_shell_free_and_can_scope_to_vercel() -> None:
    assert find_argv("airflow agents", owner="vercel-labs") == (
        *SKILLS_CLI,
        "find",
        "airflow agents",
        "--owner",
        "vercel-labs",
    )


def test_install_argv_is_explicit_and_non_interactive() -> None:
    assert install_argv(VERCEL_FIND_SKILLS, global_scope=True, agent="codex", copy=True) == (
        *SKILLS_CLI,
        "add",
        "vercel-labs/skills@find-skills",
        "-y",
        "-g",
        "--agent",
        "codex",
        "--copy",
    )


def test_harness_install_is_an_explicit_package() -> None:
    assert install_argv(SWFACTORY_HARNESS_SKILL) == (
        *SKILLS_CLI,
        "add",
        "zozo123/ariflow-swfactory@swfactory",
        "-y",
    )


def test_list_argv_targets_repository_without_installing() -> None:
    assert list_argv() == (*SKILLS_CLI, "add", "zozo123/ariflow-swfactory", "--list")


@pytest.mark.parametrize("value", ["", "owner with space", "owner;rm", "../owner"])
def test_owner_validation_rejects_ambiguous_values(value: str) -> None:
    if value == "":
        with pytest.raises(ValueError):
            find_argv("x", owner=value)
    else:
        with pytest.raises(ValueError):
            find_argv("x", owner=value)


def test_run_surfaces_nonzero_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["npx"], 2, stdout="", stderr="bad source")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(StageError, match="skills CLI failed"):
        run(("npx", "skills"))
