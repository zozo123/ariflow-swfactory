"""Backend-owned source-control mutations for managed Factory Cells.

Airflow workers may construct a patch, but they never receive GitHub publication credentials. The
backend validates the current cell epoch/policy, journals the logical publication and reconciles an
ambiguous retry by proving the desired patch digest is already attached to the deterministic PR.
"""

from __future__ import annotations

import base64
import hashlib
from typing import Any

from swfactory.idempotency import MutationOutcome, OperationRef
from swfactory.lifecycle_evidence import TraceContext
from swfactory.scm import GitHubScm
from swfactory.security_contract import MutationEnvelope

from .service import Factory, Refused, text

MAX_PATCH_BYTES = 12 * 1024 * 1024
_MARKER_PREFIX = "<!-- swfactory-patch-sha256:"


def operation(factory: Factory, path: str, body: dict[str, Any]) -> Any:
    if not factory.repo:
        raise Refused(503, "SWF_REPO is not configured on the backend")
    scm = GitHubScm(factory.repo, text({"base": body.get("base_branch", "main")}, "base"))
    if path == "/scm/issue":
        ref = text(body, "ref", max_len=128)
        issue = scm.fetch_issue(ref)
        return issue.model_dump(mode="json")
    if path == "/scm/publish":
        return _publish(factory, scm, body)
    if path == "/scm/open-issue":
        return _open_issue(factory, scm, body)
    raise Refused(404, "unknown backend SCM operation")


def _managed_identity(factory: Factory, body: dict[str, Any]) -> tuple[dict[str, Any], MutationEnvelope]:
    cell_id = text(body, "cell_id")
    epoch = body.get("epoch")
    if type(epoch) is not int or epoch < 1:
        raise ValueError("epoch must be a positive integer")
    policy_digest = text(body, "policy_digest")
    operation_key = text(body, "operation_key", max_len=256)
    actor = text({"actor": body.get("actor", "airflow-worker")}, "actor", max_len=128)
    cell = factory._cell(cell_id)
    if int(cell["epoch"]) != epoch:
        raise Refused(409, f"stale Factory Cell epoch {epoch}; current epoch is {cell['epoch']}")
    if cell.get("policy_digest") != policy_digest:
        raise Refused(409, "Factory Cell policy digest changed; publication is stale")
    trace = TraceContext.for_cell(cell_id, epoch, "github_publish", operation_key)
    envelope = MutationEnvelope(
        cell_id=cell_id,
        epoch=epoch,
        operation_key=operation_key,
        policy_digest=policy_digest,
        trace_id=trace.trace_id,
        actor=actor,
    )
    envelope.validate()
    return cell, envelope


def _publish(factory: Factory, scm: GitHubScm, body: dict[str, Any]) -> dict[str, Any]:
    _cell, envelope = _managed_identity(factory, body)
    branch = text(body, "branch")
    title = text(body, "title", max_len=512)
    pr_body = body.get("body")
    if not isinstance(pr_body, str) or len(pr_body) > 2 * 1024 * 1024:
        raise ValueError("body must be a string of at most 2 MiB")
    labels = body.get("labels") or []
    if not isinstance(labels, list) or any(not isinstance(v, str) or len(v) > 128 for v in labels):
        raise ValueError("labels must be an array of bounded strings")
    allowed = body.get("allowed_prefixes")
    if allowed is not None and (
        not isinstance(allowed, list) or any(not isinstance(v, str) for v in allowed)
    ):
        raise ValueError("allowed_prefixes must be an array of strings or null")
    encoded = body.get("patch_b64")
    if not isinstance(encoded, str):
        raise ValueError("patch_b64 is required")
    try:
        patch = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise ValueError("patch_b64 is invalid base64") from error
    if len(patch) > MAX_PATCH_BYTES:
        raise ValueError("patch exceeds backend publication limit")
    patch_digest = hashlib.sha256(patch).hexdigest()
    marker = f"{_MARKER_PREFIX}{patch_digest} -->"
    publish_body = pr_body.rstrip() + "\n\n" + marker + "\n"
    ref = OperationRef(
        envelope.cell_id,
        envelope.epoch,
        "github_publish",
        envelope.operation_key,
    )

    def publish() -> dict[str, Any]:
        url = scm.publish(
            branch=branch,
            patch=patch,
            title=title,
            body=publish_body,
            labels=labels,
            allowed_prefixes=allowed,
        )
        return {"url": url, "branch": branch, "patch_sha256": patch_digest}

    def reconcile() -> MutationOutcome:
        rows = factory._gh(
            [
                "pr", "list", "--head", branch, "--state", "open", "--limit", "1",
                "--json", "url,body,headRefOid",
            ]
        )
        if not rows:
            return MutationOutcome(
                "definitely_absent",
                None,
                {"branch": branch},
                "no open PR exists for deterministic branch",
            )
        row = rows[0]
        observed_body = str(row.get("body") or "")
        observed_url = str(row.get("url") or "")
        if marker in observed_body:
            return MutationOutcome(
                "committed",
                {"url": observed_url, "branch": branch, "patch_sha256": patch_digest},
                {"branch": branch, "url": observed_url, "head": row.get("headRefOid")},
                "PR carries the desired patch digest marker",
            )
        return MutationOutcome(
            "divergent",
            None,
            {"branch": branch, "url": observed_url, "head": row.get("headRefOid")},
            "an open PR exists but does not prove the desired patch digest",
        )

    result = factory.control.mutate(ref, publish, replay_safe=True, reconcile=reconcile)
    factory.evidence.mutation(
        envelope,
        kind="github_publication",
        payload={
            "branch": branch,
            "url": result["url"],
            "patch_sha256": patch_digest,
            "labels": labels,
        },
    )
    return result


def _open_issue(factory: Factory, scm: GitHubScm, body: dict[str, Any]) -> dict[str, Any]:
    _cell, envelope = _managed_identity(factory, body)
    title = text(body, "title", max_len=512)
    issue_body = body.get("body")
    labels = body.get("labels") or []
    if not isinstance(issue_body, str) or len(issue_body) > 2 * 1024 * 1024:
        raise ValueError("body must be a string of at most 2 MiB")
    if not isinstance(labels, list) or any(not isinstance(v, str) or len(v) > 128 for v in labels):
        raise ValueError("labels must be an array of bounded strings")
    ref = OperationRef(
        envelope.cell_id,
        envelope.epoch,
        "github_issue",
        envelope.operation_key,
    )

    def create() -> dict[str, Any]:
        return {"url": scm.open_issue(title=title, body=issue_body, labels=labels)}

    result = factory.control.mutate(ref, create, replay_safe=False, reconcile=None)
    factory.evidence.mutation(
        envelope,
        kind="github_issue",
        payload={"url": result["url"], "title": title},
    )
    return result
