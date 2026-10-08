"""Native Linear source contracts. Admission and dispatch stay in the existing backend."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from swfactory.linear_source import LinearPreview, _uuid, validate_fields
from swfactory.models import Issue
from swfactory.paths import validate_identifier


class LinearWorkSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["linear"]
    workspace_id: str
    project_id: str

    @field_validator("workspace_id", "project_id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        return _uuid(value)


class LinearWorkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    kind: Literal["linear"]
    issue_id: str
    intent_digest: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    attempt: str = Field(default="initial", min_length=1, max_length=128)

    @field_validator("issue_id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        return _uuid(value)

    @field_validator("attempt")
    @classmethod
    def validate_attempt(cls, value: str) -> str:
        return validate_identifier(value, field="work_source.attempt")

    def issue_ref(self, source: LinearWorkSource) -> str:
        return "linear_" + source.workspace_id.replace("-", "") + "_" + self.issue_id.replace("-", "")


def accepted_source(preview: LinearPreview) -> dict[str, Any]:
    return {"schema_version": 1, "kind": "linear", "intent_digest": preview.intent_digest, "snapshot": asdict(preview)}


def source_issue(document: dict[str, Any], *, expected_ref: str) -> Issue:
    if document.get("schema_version") != 1 or document.get("kind") != "linear":
        raise ValueError("unsupported accepted Linear source schema")
    try:
        row = document["snapshot"]
        # The snapshot is a LinearPreview, which keeps no workspace URL key: the stored URL names it.
        preview = validate_fields(
            {**row, "workspace_slug": urlsplit(row["url"]).path.split("/")[1]},
            workspace_id=row["workspace_id"],
            project_id=row["project_id"],
            issue_id=row["issue_id"],
        )
        source = LinearWorkSource(kind="linear", workspace_id=preview.workspace_id, project_id=preview.project_id)
        request = LinearWorkRequest(
            schema_version=1, kind="linear", issue_id=preview.issue_id, intent_digest=preview.intent_digest
        )
        if request.issue_ref(source) != expected_ref or preview.intent_digest != document["intent_digest"]:
            raise ValueError("accepted Linear source identity or digest differs from its receipt")
        if preview.terminal:
            raise ValueError("accepted Linear source was not eligible for work")
        return Issue(id=expected_ref, title=preview.title, body=preview.description, url=preview.url)
    except (KeyError, TypeError, IndexError):
        raise ValueError("accepted Linear source is incomplete") from None
