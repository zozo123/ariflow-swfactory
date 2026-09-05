"""Astronomer Blueprint bridge: compose a governed factory line as one reusable DAG step.

Astronomer Blueprint owns the outer, declarative workflow. The triggered swfactory DAG keeps the
inner state machine, dynamic issue/target mapping, HITL tasks, sandbox lifecycle, and delivery
credential. Keeping those DAGs separate also gives every factory run its own addressable history.
"""

from __future__ import annotations

from blueprint import BaseModel, Blueprint, TaskOrGroup
from pydantic import ConfigDict, Field, field_validator

from swfactory.paths import normalize_relative_path, validate_identifier, validate_repo


class SoftwareFactoryConfig(BaseModel):
    """Build-time configuration exposed in Blueprint YAML and the Astro IDE."""

    model_config = ConfigDict(extra="forbid")

    line: str = Field(
        default="factory",
        description="Existing swfactory line DAG to trigger.",
    )
    issues: list[str] = Field(
        min_length=1,
        description="GitHub issue numbers or repository-relative Markdown issue files.",
    )
    targets: list[str] = Field(
        default_factory=list,
        description="Optional owner/name filter over the line's declared targets.",
    )
    wait_for_completion: bool = Field(
        default=True,
        description="Defer this step until the governed child DAG finishes.",
    )
    poke_interval_s: int = Field(default=60, ge=5, le=3600)

    @field_validator("line")
    @classmethod
    def _line(cls, value: str) -> str:
        return validate_identifier(value, field="line")

    @field_validator("issues")
    @classmethod
    def _issues(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw in values:
            value = raw.strip()
            if value.isdigit():
                normalized.append(value)
            else:
                normalized.append(normalize_relative_path(value, field="issues"))
        return list(dict.fromkeys(normalized))

    @field_validator("targets")
    @classmethod
    def _targets(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(validate_repo(value) for value in values))


class SoftwareFactory(Blueprint[SoftwareFactoryConfig]):
    """Trigger one governed swfactory line without moving its authority into the parent DAG."""

    name = "software_factory"
    version = 1

    def render(self, config: SoftwareFactoryConfig) -> TaskOrGroup:
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

        conf: dict[str, object] = {"issues": config.issues}
        if config.targets:
            conf["targets"] = config.targets
        return TriggerDagRunOperator(
            task_id=self.step_id,
            trigger_dag_id=config.line,
            conf=conf,
            wait_for_completion=config.wait_for_completion,
            poke_interval=config.poke_interval_s,
            # A line can wait at a human gate for hours. Release the worker while the parent waits.
            deferrable=config.wait_for_completion,
            fail_when_dag_is_paused=True,
        )
