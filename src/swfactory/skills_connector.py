"""Orchestrator-side connector to the open skills.sh ecosystem.

This module deliberately does not participate in lifecycle scheduling. It is an operator utility
around Vercel's ``skills`` CLI: discover packages, install an explicitly selected skill, and expose
canonical package identities for the factory and Vercel's ``find-skills`` skill.

Networked acquisition stays outside coding workers so installing a skill cannot bypass Airflow,
Factory Cell fencing, sandbox policy, evidence gates, or publication authority. The CLI version is
pinned because this code executes it on the trusted operator host; upgrades are deliberate source
changes rather than an implicit ``latest`` dependency.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass

from swfactory.models import StageError

_TOKEN = re.compile(r"^[A-Za-z0-9_.-]+$")
SKILLS_CLI_VERSION = "1.5.25"
SKILLS_CLI: tuple[str, ...] = ("npx", "-y", f"skills@{SKILLS_CLI_VERSION}")
SKILLS_SH_URL = "https://skills.sh"
VERCEL_SKILLS_REPO = "https://github.com/vercel-labs/skills"


@dataclass(frozen=True)
class SkillPackage:
    owner: str
    repo: str
    skill: str

    def __post_init__(self) -> None:
        for label, value in (("owner", self.owner), ("repo", self.repo), ("skill", self.skill)):
            if not _TOKEN.fullmatch(value):
                raise ValueError(f"invalid skill {label}: {value!r}")

    @property
    def source(self) -> str:
        return f"{self.owner}/{self.repo}@{self.skill}"

    @property
    def repository(self) -> str:
        return f"{self.owner}/{self.repo}"

    @property
    def catalog_url(self) -> str:
        return f"{SKILLS_SH_URL}/{self.owner}/{self.repo}/{self.skill}"


SWFACTORY_SKILL = SkillPackage("zozo123", "ariflow-swfactory", "airflow-software-factory")
SWFACTORY_HARNESS_SKILL = SkillPackage("zozo123", "ariflow-swfactory", "swfactory")
VERCEL_FIND_SKILLS = SkillPackage("vercel-labs", "skills", "find-skills")


def find_argv(query: str, *, owner: str | None = None) -> tuple[str, ...]:
    """Build a shell-free ``skills find`` command."""

    query = query.strip()
    if not query:
        raise ValueError("skill query must not be empty")
    argv = [*SKILLS_CLI, "find", query]
    if owner is not None:
        if not _TOKEN.fullmatch(owner):
            raise ValueError(f"invalid skill owner: {owner!r}")
        argv += ["--owner", owner]
    return tuple(argv)


def install_argv(
    package: SkillPackage,
    *,
    global_scope: bool = False,
    agent: str | None = None,
    copy: bool = False,
) -> tuple[str, ...]:
    """Build an explicit, non-interactive ``skills add`` command."""

    argv = [*SKILLS_CLI, "add", package.source, "-y"]
    if global_scope:
        argv.append("-g")
    if agent is not None:
        if not _TOKEN.fullmatch(agent):
            raise ValueError(f"invalid agent name: {agent!r}")
        argv += ["--agent", agent]
    if copy:
        argv.append("--copy")
    return tuple(argv)


def list_argv(repository: str = SWFACTORY_SKILL.repository) -> tuple[str, ...]:
    """Build a command that asks the Vercel CLI to enumerate skills in one repository."""

    parts = repository.split("/", 1)
    if len(parts) != 2 or any(not _TOKEN.fullmatch(part) for part in parts):
        raise ValueError(f"invalid skill repository: {repository!r}")
    return (*SKILLS_CLI, "add", repository, "--list")


def run(argv: Sequence[str], *, timeout_s: int = 120) -> str:
    """Run the skills CLI on the trusted orchestrator/operator host and return stdout.

    No shell is used. A missing ``npx``, timeout, or non-zero result is surfaced as a non-retryable
    operator error rather than silently falling back to another package source.
    """

    try:
        proc = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_s,
        )
    except FileNotFoundError as error:
        raise StageError("skills", "npx not found on operator PATH", retryable=False) from error
    except subprocess.TimeoutExpired as error:
        raise StageError("skills", "skills CLI timed out", retryable=False) from error
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()[-2000:]
        raise StageError("skills", f"skills CLI failed (rc={proc.returncode}): {detail}", retryable=False)
    return proc.stdout


def catalog() -> dict[str, str]:
    """Return canonical package identities and the executable distribution pin."""

    return {
        "factory": SWFACTORY_SKILL.source,
        "factory_catalog": SWFACTORY_SKILL.catalog_url,
        "harness": SWFACTORY_HARNESS_SKILL.source,
        "harness_catalog": SWFACTORY_HARNESS_SKILL.catalog_url,
        "skills_cli": f"skills@{SKILLS_CLI_VERSION}",
        "skills_cli_version": SKILLS_CLI_VERSION,
        "vercel_find_skills": VERCEL_FIND_SKILLS.source,
        "vercel_repository": VERCEL_SKILLS_REPO,
        "skills_sh": SKILLS_SH_URL,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Small operator CLI: ``python -m swfactory.skills_connector ...``."""

    parser = argparse.ArgumentParser(description="skills.sh/Vercel Skills operator connector")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("catalog", help="print canonical skill package identities")
    commands.add_parser("list", help="list skills exported by this repository")

    find = commands.add_parser("find", help="search skills through Vercel's skills CLI")
    find.add_argument("query")
    find.add_argument("--owner", default="vercel-labs")

    bootstrap = commands.add_parser("bootstrap", help="install Vercel's find-skills package")
    bootstrap.add_argument("--global", dest="global_scope", action="store_true")
    bootstrap.add_argument("--agent")
    bootstrap.add_argument("--copy", action="store_true")

    install_self = commands.add_parser("install-self", help="install the factory skill")
    install_self.add_argument("--global", dest="global_scope", action="store_true")
    install_self.add_argument("--agent")
    install_self.add_argument("--copy", action="store_true")

    install_harness = commands.add_parser("install-harness", help="install the outer-harness skill")
    install_harness.add_argument("--global", dest="global_scope", action="store_true")
    install_harness.add_argument("--agent")
    install_harness.add_argument("--copy", action="store_true")

    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.command == "catalog":
        print(json.dumps(catalog(), indent=2, sort_keys=True))
        return 0
    if args.command == "list":
        print(run(list_argv()), end="")
        return 0
    if args.command == "find":
        print(run(find_argv(args.query, owner=args.owner)), end="")
        return 0

    if args.command == "bootstrap":
        package = VERCEL_FIND_SKILLS
    elif args.command == "install-harness":
        package = SWFACTORY_HARNESS_SKILL
    else:
        package = SWFACTORY_SKILL
    print(
        run(
            install_argv(
                package,
                global_scope=args.global_scope,
                agent=args.agent,
                copy=args.copy,
            )
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
