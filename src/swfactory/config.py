"""Factory configuration. Every knob is an env var SWF_<NAME> or a CLI flag."""

from __future__ import annotations

import re
import tomllib
import uuid
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

if TYPE_CHECKING:
    from swfactory.sandbox import Sandbox


SRT_DEFAULT_DOMAINS = ("api.anthropic.com", "pypi.org", "files.pythonhosted.org", "astral.sh")


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SWF_", extra="ignore")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """SWF_* env vars override values a blueprint passes at init (dev/smoke escape hatch)."""
        return (env_settings, init_settings, dotenv_settings, file_secret_settings)

    issue: str  # issue number, or path to a front-matter .md (demo)
    repo: str = "zozo123/ariflow-swfactory"  # owner/name of the TARGET repo
    target_dir: str = "demo/target"  # subdir the factory operates on ("" = repo root)
    base_branch: str = "main"
    blueprint: str = "factory"  # blueprints/<name>.toml that produced this config
    sandbox: Literal["local", "islo", "srt"] = "local"
    agent: Literal["claude", "scripted"] = "scripted"
    scm: Literal["local", "github"] = "local"
    approve: Literal["auto", "prompt"] = "prompt"
    tests: Literal["sandbox", "crabbox"] = "sandbox"  # where the test command executes
    crabbox_provider: str = "local-container"  # islo needs ISLO_API_KEY, which scrub_env strips
    max_build_iterations: int = 3
    max_review_fixes: int = 1
    max_turns: int = 40  # per agent invocation
    max_budget_usd_per_stage: float = 2.0
    max_budget_usd: float = 8.0  # run ceiling
    gateway_profile: str = "swfactory"
    islo_environment: str = "swfactory"
    sandbox_ttl_s: int = 172_800  # --delete-after; must exceed gate_timeout
    sandbox_idle_s: int = 900  # --pause-after-idle
    gate_timeout_h: int = 24  # ApprovalOperator response_timeout
    stage_timeout_h: int = 3  # execution_timeout of every stage task
    max_parallel_jobs: int = 4  # concurrent (issue x target) jobs == concurrent sandboxes
    islo_snapshot: str | None = None  # --snapshot warm start (islo only)
    srt_allowed_domains: list[str] = Field(default_factory=lambda: list(SRT_DEFAULT_DOMAINS))
    fixtures_dir: str = "demo/scripted"
    workdir: str = ".factory/work"  # LocalSandbox root
    run_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:8])
    record_dir: str | None = None  # dump real agent outputs as fixtures
    allow_local_agent: bool = False  # DEV ESCAPE HATCH: run the real agent outside islo

    def sandbox_name(self, issue_id: str, repo: str | None = None) -> str:
        """One sandbox per (issue, target); stable per run so `islo use` is idempotent."""
        repo_part = "" if repo is None else f"-{_slug(repo.rsplit('/', 1)[-1])[:20]}"
        name = f"swf-{_slug(issue_id)[:24]}{repo_part}-{self.run_id}"
        assert re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}", name), name
        return name

    @staticmethod
    def artifacts_dir(issue_id: str) -> str:
        """Committed artifact chain lives here, inside the target dir."""
        return f"docs/factory/{issue_id}"

    @model_validator(mode="after")
    def _trust_boundary(self) -> Config:
        if self.agent == "claude" and self.sandbox == "local" and not self.allow_local_agent:
            raise ValueError(
                "agent=claude requires sandbox=islo or sandbox=srt (model-generated code must not "
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

    test: str
    lint: str | None = None
    junit: str = ".factory/junit.xml"
    source: str = "src"
    tests_dir: str = "tests"
    protected: list[str] = Field(default_factory=list)

    @classmethod
    def parse(cls, text: str) -> TargetContract:
        data = tomllib.loads(text)
        cmds = data.get("commands", {})
        paths = data.get("paths", {})
        if "test" not in cmds:
            raise ValueError("factory.toml must define [commands].test")
        return cls(
            test=cmds["test"],
            lint=cmds.get("lint"),
            junit=paths.get("junit", ".factory/junit.xml"),
            source=paths.get("source", "src"),
            tests_dir=paths.get("tests", "tests"),
            protected=list(paths.get("protected", [])),
        )


def load_target_contract(sb: Sandbox) -> TargetContract:
    try:
        return TargetContract.parse(sb.read("factory.toml"))
    except FileNotFoundError as e:
        raise ValueError("target has no factory.toml; the factory refuses to guess commands") from e


def _slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", s).strip("-").lower()[:40] or "issue"
