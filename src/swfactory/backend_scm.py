"""SCM adapter for backend-managed Airflow jobs.

The worker can read local issue files itself, but numeric GitHub issue reads and all GitHub
writes go through the authenticated Python backend. No GH_TOKEN/GITHUB_TOKEN is needed in the
worker process.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

from swfactory.backend_http import ResponseTooLarge, post_json, valid_backend_token
from swfactory.models import Issue, StageError
from swfactory.publication_identity import PublicationIdentity
from swfactory.scm import parse_issue_file


class BackendScm:
    kind: Literal["local", "github"] = "github"

    def __init__(
        self,
        *,
        repo: str,
        base_branch: str,
        backend_url: str,
        backend_token: str,
        cell_id: str,
        epoch: int,
        policy_digest: str,
        actor: str = "airflow-worker",
        source_blueprint_digest: str | None = None,
    ) -> None:
        self.autonomous_evidence: dict | None = None
        self.repo = repo
        self.base_branch = base_branch
        self.backend_url = backend_url.rstrip("/")
        self.backend_token = backend_token
        self.cell_id = cell_id
        self.epoch = epoch
        self.policy_digest = policy_digest
        self.actor = actor
        self.source_blueprint_digest = source_blueprint_digest
        if not self.backend_url:
            raise StageError("policy", "managed GitHub SCM requires SWF_BACKEND_URL")
        if not valid_backend_token(self.backend_token):
            raise StageError("policy", "managed GitHub SCM requires a valid SWF_BACKEND_TOKEN")
        if not policy_digest.startswith("policy:"):
            raise StageError("policy", "managed GitHub SCM requires a Factory Cell policy digest")

    def fetch_issue(self, ref: str) -> Issue:
        if ref.startswith("linear_"):
            value = self._post(
                "/scm/linear-source",
                {
                    **self._identity("linear-source-read"),
                    "ref": ref,
                    "blueprint_digest": self.source_blueprint_digest,
                },
            )
            try:
                return Issue.model_validate(value)
            except Exception as error:
                raise StageError("scm", "backend returned invalid accepted Linear source") from error
        if not ref.strip().isdigit():
            return parse_issue_file(Path(ref))
        value = self._post("/scm/issue", {"ref": ref.strip(), "base_branch": self.base_branch})
        try:
            return Issue.model_validate(value)
        except Exception as error:
            raise StageError("scm", "backend returned invalid GitHub issue data") from error

    def publish(
        self,
        *,
        branch: str,
        patch: bytes,
        title: str,
        body: str,
        labels: Sequence[str],
        allowed_prefixes: Sequence[str] | None = None,
        identity: PublicationIdentity | None = None,
    ) -> str:
        # `identity` is not sent: the commits in `patch` already carry the `Factory-Instance`
        # trailer, and the boundary authenticates the Cell it acts for. Nothing a worker claims
        # about its own identity crosses the wire.
        del identity
        patch_digest = hashlib.sha256(patch).hexdigest()
        operation_key = f"github_publish:{branch}:{patch_digest[:24]}"
        value = self._post(
            "/scm/publish",
            {
                **self._identity(operation_key),
                "base_branch": self.base_branch,
                "branch": branch,
                "revision": self.autonomous_evidence.get("revision") if self.autonomous_evidence else None,
                "autonomous_evidence": self.autonomous_evidence,
                "patch_b64": base64.b64encode(patch).decode("ascii"),
                "title": title,
                "body": body,
                "labels": list(labels),
                "allowed_prefixes": list(allowed_prefixes) if allowed_prefixes is not None else None,
            },
            timeout=180,
        )
        url = value.get("url") if isinstance(value, dict) else None
        if not isinstance(url, str) or not url.startswith("https://"):
            raise StageError("scm", "backend publication returned no HTTPS pull-request URL")
        return url

    def autonomous_gate(self, **evidence: Any) -> dict:
        gate = str(evidence["gate"])
        return self._post("/scm/policy-gate", {**self._identity(f"policy_gate:{gate}"), **evidence})

    def autonomous_merge(self, revision: str) -> dict:
        return self._post(
            "/scm/merge", {**self._identity("autonomous_merge"), "revision": revision, "base_branch": self.base_branch}
        )

    def open_issue(self, *, title: str, body: str, labels: Sequence[str]) -> str:
        digest = hashlib.sha256((title + "\0" + body).encode()).hexdigest()[:24]
        value = self._post(
            "/scm/open-issue",
            {
                **self._identity(f"github_issue:{digest}"),
                "base_branch": self.base_branch,
                "title": title,
                "body": body,
                "labels": list(labels),
            },
            timeout=60,
        )
        url = value.get("url") if isinstance(value, dict) else None
        if not isinstance(url, str) or not url.startswith("https://"):
            raise StageError("scm", "backend issue creation returned no HTTPS URL")
        return url

    def _identity(self, operation_key: str) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "epoch": self.epoch,
            "policy_digest": self.policy_digest,
            "operation_key": operation_key,
            "actor": self.actor,
        }

    def _post(self, path: str, body: dict[str, Any], *, timeout: int = 30) -> Any:
        try:
            status, value = post_json(
                self.backend_url, self.backend_token, "/v1" + path, body, timeout=timeout, limit=20 * 1024 * 1024
            )
        except ResponseTooLarge:
            raise StageError("scm", "factory backend SCM response exceeds limit") from None
        except ValueError as error:
            raise StageError("scm", "factory backend SCM returned invalid JSON") from error
        except OSError as error:
            raise StageError(
                "scm",
                f"factory backend unavailable during managed SCM operation: {error}",
                retryable=True,
            ) from error
        if status >= 300:
            raise StageError("scm", f"factory backend SCM refused operation: {value}", retryable=status >= 500)
        return value
