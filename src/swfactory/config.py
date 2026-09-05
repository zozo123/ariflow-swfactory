"""Factory configuration. Every knob is an env var SWF_<NAME> or a CLI flag."""

from __future__ import annotations

import re
import tomllib
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from swfactory.assets import ASSET_ROOT
from swfactory.paths import (
    normalize_absolute_posix_path,
    normalize_relative_path,
    validate_git_ref,
    validate_identifier,
    validate_repo,
    validate_run_id,
)

if TYPE_CHECKING:
    from swfactory.sandbox import Sandbox


SRT_DEFAULT_DOMAINS = ("api.anthropic.com", "pypi.org", "files.pythonhosted.org", "astral.sh")
DOCKER_DEFAULT_IMAGE = "ghcr.io/zozo123/swfactory-sandbox:latest"
IDENTITY_SETTINGS = frozenset({"issue", "repo", "target_dir", "base_branch", "run_id", "blueprint"})
# Compatibility name used throughout the runtime. This is the checkout root in development and
# the bundled asset root in an installed wheel.
FACTORY_ROOT = ASSET_ROOT


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SWF_", extra="ignore", allow_inf_nan=False)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Operational SWF_* values override blueprint defaults.

        ``runtime.job_config`` reapplies issue/repository/run identity after settings load so an
        ambient worker environment cannot collapse separate mapped jobs onto one workspace.
        """

        def operational(source: PydanticBaseSettingsSource):
            def load() -> dict[str, object]:
                return {
                    key: value for key, value in source().items() if key not in IDENTITY_SETTINGS
                }

            return load

        return (
            operational(env_settings),  # type: ignore[return-value]
            init_settings,
            operational(dotenv_settings),  # type: ignore[return-value]
            operational(file_secret_settings),  # type: ignore[return-value]
        )

    # -- target: which issue, which repo, which subtree
    issue: str  # issue number, or path to a front-matter .md (demo)
    repo: str = "zozo123/ariflow-swfactory"  # owner/name of the TARGET repo
    target_dir: str = "demo/target"  # subdir the factory operates on ("" = repo root)
    base_branch: str = "main"
    blueprint: str = "factory"  # blueprints/<name>.toml that produced this config

    # -- execution: which implementation of each seam runs
    sandbox: Literal["local", "islo", "srt", "docker", "toolset"] = "local"
    agent: Literal["claude", "scripted"] = "scripted"
    scm: Literal["local", "github"] = "local"
    approve: Literal["auto", "prompt"] = "prompt"
    tests: Literal["sandbox", "crabbox"] = "sandbox"  # where the test command executes
    crabbox_provider: str = "local-container"  # islo needs ISLO_API_KEY, which scrub_env strips

    # -- limits: loop bounds and cost ceilings (stages enforce them, not the prompts)
    max_build_iterations: int = Field(default=3, ge=1)
    max_review_fixes: int = Field(default=1, ge=0)
    max_turns: int = Field(default=40, ge=1)  # per agent invocation
    max_budget_usd_per_stage: float = Field(default=2.0, gt=0)
    max_budget_usd: float = Field(default=8.0, gt=0)  # run ceiling

    # -- gates and scheduling: read by the DAG factory
    gate_timeout_h: int = Field(default=24, ge=0)  # ApprovalOperator response_timeout
    stage_timeout_h: int = Field(default=3, ge=1)  # execution_timeout of every stage task
    max_parallel_jobs: int = Field(default=4, ge=1)  # concurrent jobs == concurrent sandboxes

    # -- islo sandbox: gateway/environment identity and MicroVM lifecycle
    gateway_profile: str = "swfactory"
    islo_environment: str = "swfactory"
    sandbox_ttl_s: int = Field(default=172_800, ge=1)  # must exceed gate_timeout
    sandbox_idle_s: int = Field(default=900, ge=1)
    islo_snapshot: str | None = None  # --snapshot warm start (islo only)
    # Airflow's own sandbox abstraction (provider common.ai): sbx ships released; islo,
    # opensandbox and asciibox are pending upstream PRs (apache/airflow #71672/#71676/#71725).
    toolset_backend: str = "sbx"
    toolset_workdir: str = "/workspace/repo"  # repository root inside a SandboxBackend
    sandbox_owner: str | None = None  # SWF_SANDBOX_OWNER: only this creator's sandboxes may be rm'd

    # -- srt / docker sandboxes: egress allowlist, image, credential mode
    srt_allowed_domains: list[str] = Field(default_factory=lambda: list(SRT_DEFAULT_DOMAINS))
    docker_image: str = DOCKER_DEFAULT_IMAGE  # image every run() executes in (bind-mounted workdir)
    # env => pass ANTHROPIC_API_KEY (agent=claude); host => bind-mount ~/.claude + ~/.claude.json
    docker_credentials: Literal["env", "host"] = "env"
    docker_network: str = "bridge"  # `--network` of every sandbox container ("none" = no egress)
    docker_user: str | None = None  # `--user` override (uid[:gid]); None = the image's user

    # -- demo, recording and dev escape hatches
    fixtures_dir: str = "demo/scripted"
    workdir: str = ".factory/work"  # LocalSandbox root
    run_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:8])
    record_dir: str | None = None  # dump real agent outputs as fixtures
    allow_local_agent: bool = False  # DEV ESCAPE HATCH: run the real agent outside islo

    @field_validator("repo")
    @classmethod
    def _repo(cls, value: str) -> str:
        return validate_repo(value)

    @field_validator("target_dir")
    @classmethod
    def _target_dir(cls, value: str) -> str:
        return normalize_relative_path(value, field="target_dir", allow_empty=True)

    @field_validator("base_branch")
    @classmethod
    def _base_branch(cls, value: str) -> str:
        return validate_git_ref(value)

    @field_validator("run_id")
    @classmethod
    def _run_id(cls, value: str) -> str:
        return validate_run_id(value)

    @field_validator("toolset_workdir")
    @classmethod
    def _toolset_workdir(cls, value: str) -> str:
        return normalize_absolute_posix_path(value, field="toolset_workdir")

    def sandbox_name(self, issue_id: str, repo: str | None = None) -> str:
        """One sandbox per (issue, target); stable per run so `islo use` is idempotent."""
        validate_identifier(issue_id, field="issue_id")
        repo_part = "" if repo is None else f"-{_slug(validate_repo(repo).rsplit('/', 1)[-1])[:20]}"
        suffix = f"-{self.run_id}"
        stem = f"swf-{_slug(issue_id)[:24]}{repo_part}"
        return f"{stem[: 63 - len(suffix)].rstrip('-_')}{suffix}"

    @staticmethod
    def artifacts_dir(issue_id: str) -> str:
        """Committed artifact chain lives here, inside the target dir."""
        return f"docs/factory/{validate_identifier(issue_id, field='issue_id')}"

    @model_validator(mode="after")
    def _trust_boundary(self) -> Config:
        if self.agent == "claude" and self.sandbox == "local" and not self.allow_local_agent:
            raise ValueError(
                "agent=claude requires sandbox=islo, srt, docker or toolset "
                "(model-generated code must not "
                "execute unconfined on the orchestrator). Set SWF_ALLOW_LOCAL_AGENT=1 / "
                "--allow-local-agent to override for development."
            )
        if self.tests == "crabbox" and self.sandbox != "local":
            raise ValueError("tests=crabbox is only valid with sandbox=local (no nested boxes)")
        if self.sandbox_ttl_s <= self.gate_timeout_h * 3600:
            raise ValueError("sandbox_ttl_s must exceed gate_timeout_h")
        return self


class TargetContract(BaseModel):
    """Parsed `factory.toml` of the target: the factory never guesses commands."""

    model_config = ConfigDict(extra="forbid")

    test: str = Field(min_length=1)
    lint: str | None = None
    junit: str = ".factory/junit.xml"
    source: str = "src"
    tests_dir: str = "tests"
    protected: list[str] = Field(default_factory=list)

    @field_validator("test", "lint")
    @classmethod
    def _command(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or "\x00" in value:
            raise ValueError("commands must be non-empty shell strings without NUL bytes")
        return value

    @field_validator("junit")
    @classmethod
    def _junit(cls, value: str) -> str:
        path = normalize_relative_path(value, field="paths.junit")
        if not path.startswith(".factory/"):
            raise ValueError("paths.junit must live below .factory/")
        return path

    @field_validator("source", "tests_dir")
    @classmethod
    def _directory(cls, value: str, info) -> str:
        return normalize_relative_path(value, field=f"paths.{info.field_name}")

    @field_validator("protected")
    @classmethod
    def _protected_paths(cls, value: list[str]) -> list[str]:
        paths = [normalize_relative_path(path, field="paths.protected") for path in value]
        return list(dict.fromkeys(paths))

    @classmethod
    def parse(cls, text: str) -> TargetContract:
        data = tomllib.loads(text)
        cmds = data.get("commands", {})
        paths = data.get("paths", {})
        if not isinstance(cmds, dict) or not isinstance(paths, dict):
            raise ValueError("factory.toml [commands] and [paths] must be tables")
        if "test" not in cmds:
            raise ValueError("factory.toml must define [commands].test")
        protected = paths.get("protected", [])
        if not isinstance(protected, list) or any(not isinstance(p, str) for p in protected):
            raise ValueError("factory.toml paths.protected must be an array of strings")
        return cls(
            test=cmds["test"],
            lint=cmds.get("lint"),
            junit=paths.get("junit", ".factory/junit.xml"),
            source=paths.get("source", "src"),
            tests_dir=paths.get("tests", "tests"),
            protected=protected,
        )


def protected_for(contract: TargetContract, stage: str) -> list[str]:
    """Paths the agent may not edit in ``stage``.

    The playbook protects tests during *fix* tasks (fix the code, not the gate). Build must be
    able to write tests, so the tests dir is dropped from the list for every stage but ``fix``.
    """
    if stage == "fix":
        return list(dict.fromkeys([*contract.protected, contract.tests_dir]))
    tests = contract.tests_dir.rstrip("/")
    return [p for p in contract.protected if p.rstrip("/") != tests]


def load_target_contract(sb: Sandbox) -> TargetContract:
    try:
        return TargetContract.parse(sb.read("factory.toml"))
    except FileNotFoundError as e:
        raise ValueError("target has no factory.toml; the factory refuses to guess commands") from e


def protected_globs(workdir: Path, stage: str = "build") -> list[str]:
    """``protected_for`` of the ``factory.toml`` in a seeded HOST workdir (local/srt sandboxes).

    Read before the sandbox exists so srt can make the globs kernel-level ``denyWrite`` from the
    first command; ``stage="build"`` keeps the tests dir writable (build must add tests) and
    ``stages._agent`` tightens to ``protected_for(contract, "fix")`` for fix calls. A missing or
    invalid contract yields ``[]``: ``stages.setup`` refuses the target later with a clear error.
    """
    try:
        text = (Path(workdir) / "factory.toml").read_text(encoding="utf-8")
        return protected_for(TargetContract.parse(text), stage)
    except (OSError, ValueError):
        return []


def _slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", s).strip("-").lower()[:40] or "issue"
