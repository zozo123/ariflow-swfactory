"""SCM adapter for backend-managed Airflow jobs.

The worker can read local issue files itself, but numeric GitHub issue reads and all GitHub
writes go through the authenticated Python backend. No GH_TOKEN/GITHUB_TOKEN is needed in the
worker process.
"""

from __future__ import annotations

import base64
import hashlib
import json
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

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
    ) -> None:
        self.repo = repo
        self.base_branch = base_branch
        self.backend_url = backend_url.rstrip("/")
        self.backend_token = backend_token
        self.cell_id = cell_id
        self.epoch = epoch
        self.policy_digest = policy_digest
        self.actor = actor
        if not self.backend_url:
            raise StageError("policy", "managed GitHub SCM requires SWF_BACKEND_URL")
        if len(self.backend_token) < 32 or any(c.isspace() for c in self.backend_token):
            raise StageError("policy", "managed GitHub SCM requires a valid SWF_BACKEND_TOKEN")
        if not policy_digest.startswith("policy:"):
            raise StageError("policy", "managed GitHub SCM requires a Factory Cell policy digest")

    def fetch_issue(self, ref: str) -> Issue:
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
        payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        request = urllib.request.Request(
            self.backend_url + "/v1" + path,
            data=payload,
            method="POST",
            headers={
                "Authorization": "Bearer " + self.backend_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read(20 * 1024 * 1024 + 1)
                status = response.status
        except urllib.error.HTTPError as error:
            raw = error.read(8192)
            status = error.code
        except OSError as error:
            raise StageError(
                "scm",
                f"factory backend unavailable during managed SCM operation: {error}",
                retryable=True,
            ) from error
        if len(raw) > 20 * 1024 * 1024:
            raise StageError("scm", "factory backend SCM response exceeds limit")
        try:
            value = json.loads(raw) if raw else {}
        except ValueError as error:
            raise StageError("scm", "factory backend SCM returned invalid JSON") from error
        if status >= 300:
            detail = value.get("detail", f"HTTP {status}") if isinstance(value, dict) else f"HTTP {status}"
            raise StageError(
                "scm",
                f"factory backend SCM refused operation: {detail}",
                retryable=status >= 500,
            )
        return value
