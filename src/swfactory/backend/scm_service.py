"""Backend-owned source-control mutations for managed Factory Cells.

Airflow workers may construct a patch, but they never receive GitHub publication credentials. The
backend validates the current Cell epoch, bound Airflow run, policy, capability and immutable
operation intent before a write. Ambiguous retries observe the remote before replay: a publication
is judged from the branch and pull-request lifecycle bound to immutable git content, an issue from
its deterministic marker, so both share one durable mutation/evidence path.
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import asdict
from typing import Any

from swfactory.authority import ResourceKind
from swfactory.core_capabilities import CoreMutationRequest
from swfactory.idempotency import MutationOutcome
from swfactory.liquid_security_runtime import Capability, SecurityContext
from swfactory.models import StageError
from swfactory.recovery_accounting import (
    Observation,
    Outcome,
    PublicationReceipt,
    RecoveryAction,
    RemoteIdentity,
    classify_observation,
)
from swfactory.scm import GitHubScm, patch_content_digest

from .core_service import airflow_binding, ensure_core, intent_digest
from .service import Factory, Refused, text

MAX_PATCH_BYTES = 12 * 1024 * 1024
_MARKER_PREFIX = "<!-- swfactory-patch-sha256:"
_ISSUE_MARKER_PREFIX = "<!-- swfactory-operation:"


def operation(factory: Factory, path: str, body: dict[str, Any]) -> Any:
    if not factory.repo:
        raise Refused(503, "SWF_REPO is not configured on the backend")
    scm = GitHubScm(factory.repo, text({"base": body.get("base_branch", "main")}, "base"))
    if path == "/scm/issue":
        # Filesystem issue refs are a local-demo affordance. The backend holds publication and
        # Airflow credentials, so a network caller may resolve GitHub issue numbers only.
        ref = text(body, "ref", max_len=128).strip()
        if not ref.isdigit():
            raise Refused(400, "ref must be an issue number over the API; a path is local-only")
        issue = scm.fetch_issue(ref)
        return issue.model_dump(mode="json")
    if path == "/scm/publish":
        return _publish(factory, scm, body)
    if path == "/scm/open-issue":
        return _open_issue(factory, scm, body)
    raise Refused(404, "unknown backend SCM operation")


def _managed_identity(
    factory: Factory,
    body: dict[str, Any],
) -> tuple[dict[str, Any], SecurityContext, str, str]:
    cell_id = text(body, "cell_id")
    epoch = body.get("epoch")
    if type(epoch) is not int or epoch < 1:
        raise ValueError("epoch must be a positive integer")
    policy_digest = text(body, "policy_digest")
    operation_key = text(body, "operation_key", max_len=256)
    initiating_actor = text({"actor": body.get("actor", "airflow-worker")}, "actor", max_len=128)
    cell = factory._cell(cell_id)
    if int(cell["epoch"]) != epoch:
        raise Refused(409, f"stale Factory Cell epoch {epoch}; current epoch is {cell['epoch']}")
    if cell.get("policy_digest") != policy_digest:
        raise Refused(409, "Factory Cell policy digest changed; publication is stale")
    airflow_binding(factory, cell_id, epoch)  # fail closed before constructing the capability
    ensure_core(factory)
    security = SecurityContext(
        tenant=factory.repo,
        cell_id=cell_id,
        epoch=epoch,
        role="publisher",
        capabilities=frozenset({Capability.PUBLISH_GIT}),
    )
    return cell, security, operation_key, initiating_actor


def _request(
    factory: Factory,
    *,
    cell: dict[str, Any],
    security: SecurityContext,
    operation_key: str,
    kind: str,
    digest: str,
    replay_safe: bool,
    parts: tuple[str, ...],
) -> CoreMutationRequest:
    return CoreMutationRequest(
        airflow=airflow_binding(factory, str(cell["cell_id"]), int(cell["epoch"])),
        security=security,
        resource=ResourceKind.GITHUB_PUBLICATION,
        capability=Capability.PUBLISH_GIT,
        actor="python-backend",
        kind=kind,
        expected_policy_digest=str(cell["policy_digest"]),
        target_tenant=factory.repo,
        parts=parts,
        replay_safe=replay_safe,
        external_operation_key=operation_key,
        intent_digest=digest,
    )


def _publish(factory: Factory, scm: GitHubScm, body: dict[str, Any]) -> dict[str, Any]:
    cell, security, operation_key, initiating_actor = _managed_identity(factory, body)
    branch = text(body, "branch")
    title = text(body, "title", max_len=512)
    pr_body = body.get("body")
    if not isinstance(pr_body, str) or len(pr_body) > 2 * 1024 * 1024:
        raise ValueError("body must be a string of at most 2 MiB")
    labels = body.get("labels") or []
    if not isinstance(labels, list) or any(not isinstance(value, str) or len(value) > 128 for value in labels):
        raise ValueError("labels must be an array of bounded strings")
    allowed = body.get("allowed_prefixes")
    if allowed is not None and (not isinstance(allowed, list) or any(not isinstance(value, str) for value in allowed)):
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
    # The marker is a lookup hint for people reading the PR. It is not the proof: anyone with write
    # access can edit a body, so a retry is judged on git content (`patch_content_digest`) instead.
    marker = f"{_MARKER_PREFIX}{patch_digest} -->"
    publish_body = pr_body.rstrip() + "\n\n" + marker + "\n"
    base_branch = str(body.get("base_branch") or "main")
    digest = intent_digest(
        {
            "kind": "github_publish",
            "repo": factory.repo,
            "base_branch": base_branch,
            "branch": branch,
            "title": title,
            "body": pr_body,
            "labels": labels,
            "allowed_prefixes": allowed,
            "patch_sha256": patch_digest,
            "initiating_actor": initiating_actor,
        }
    )
    request = _request(
        factory,
        cell=cell,
        security=security,
        operation_key=operation_key,
        kind="github_publish",
        digest=digest,
        replay_safe=True,
        parts=(branch, patch_digest),
    )
    expected = _publication_identity(factory.repo, base_branch, branch, patch_content_digest(patch))

    def judge(receipt: PublicationReceipt | None) -> MutationOutcome:
        """One verdict, from the remote alone, for the first attempt and for every reconcile."""
        if receipt is None:
            return MutationOutcome(
                "definitely_absent", None, {"branch": branch}, "neither a pull request nor the branch exists"
            )
        evidence = {"patch_sha256": patch_digest, **asdict(receipt)}
        observed = Observation(
            Outcome.COMMITTED if receipt.verify(expected) else Outcome.REFUSED,
            _publication_identity(
                receipt.repository, receipt.base_revision, receipt.branch, receipt.content_digest
            ).digest,
            evidence,
        )
        if classify_observation(expected, observed) is not RecoveryAction.ADOPT:
            # A marker-bearing PR whose branch was rewritten, a branch another writer owns, or a PR
            # against another base/repository: not this publication, and not ours to overwrite.
            return MutationOutcome(
                "divergent",
                None,
                evidence,
                f"{receipt.pr_state} remote state for {branch} does not prove the intended git content",
            )
        if receipt.pr_state == "branch_only":
            # Died between the push and `gh pr create`: the ref already holds the intended commits,
            # so the replay finishes the publication on it rather than judging it absent.
            return MutationOutcome(
                "definitely_absent", None, evidence, "the branch carries the intended commits but no PR exists"
            )
        return MutationOutcome(
            "committed",
            evidence,
            evidence,
            f"{receipt.pr_state} pull request carries the intended git content",
        )

    def publish() -> dict[str, Any]:
        scm.publish(
            branch=branch,
            patch=patch,
            title=title,
            body=publish_body,
            labels=labels,
            allowed_prefixes=allowed,
        )
        # The receipt is read back from GitHub the way a reconcile reads it, so the journal never
        # holds a claim the remote cannot repeat (and a later replay compares like with like).
        outcome = judge(scm.observe_publication(branch))
        if outcome.status != "committed":
            raise StageError("scm", f"publication did not verify on the remote: {outcome.detail}", retryable=True)
        return dict(outcome.result)

    def reconcile() -> MutationOutcome:
        try:
            return judge(scm.observe_publication(branch))
        except StageError as error:
            return MutationOutcome("ambiguous", None, {"branch": branch}, f"remote observation failed: {error}")

    return factory.control.mutate_core(request, publish, reconcile=reconcile).result


def _publication_identity(repo: str, base: str, branch: str, content_digest: str) -> RemoteIdentity:
    """Repository, base, branch and git content: the four things a publication receipt must bind."""
    return RemoteIdentity("github_publish", repo, f"{base}:{branch}", content_digest)


def _open_issue(factory: Factory, scm: GitHubScm, body: dict[str, Any]) -> dict[str, Any]:
    cell, security, operation_key, initiating_actor = _managed_identity(factory, body)
    title = text(body, "title", max_len=512)
    issue_body = body.get("body")
    labels = body.get("labels") or []
    if not isinstance(issue_body, str) or len(issue_body) > 2 * 1024 * 1024:
        raise ValueError("body must be a string of at most 2 MiB")
    if not isinstance(labels, list) or any(not isinstance(value, str) or len(value) > 128 for value in labels):
        raise ValueError("labels must be an array of bounded strings")
    digest = intent_digest(
        {
            "kind": "github_issue",
            "repo": factory.repo,
            "title": title,
            "body": issue_body,
            "labels": labels,
            "initiating_actor": initiating_actor,
        }
    )
    marker = f"{_ISSUE_MARKER_PREFIX}{operation_key} {digest} -->"
    marked_body = issue_body.rstrip() + "\n\n" + marker + "\n"
    request = _request(
        factory,
        cell=cell,
        security=security,
        operation_key=operation_key,
        kind="github_issue",
        digest=digest,
        replay_safe=True,
        parts=(title, digest),
    )

    def create() -> dict[str, Any]:
        return {"url": scm.open_issue(title=title, body=marked_body, labels=labels)}

    def reconcile() -> MutationOutcome:
        rows = factory._gh(
            [
                "issue",
                "list",
                "--state",
                "all",
                "--search",
                operation_key,
                "--limit",
                "20",
                "--json",
                "url,body,title",
            ]
        )
        matches = [row for row in rows if marker in str(row.get("body") or "")]
        if len(matches) == 1 and str(matches[0].get("title") or "") == title:
            return MutationOutcome(
                "committed",
                {"url": matches[0]["url"]},
                {"url": matches[0]["url"], "operation_key": operation_key},
                "issue carries the immutable operation marker",
            )
        if not matches:
            return MutationOutcome(
                "definitely_absent",
                None,
                {"operation_key": operation_key},
                "no issue carries the immutable operation marker",
            )
        return MutationOutcome(
            "divergent",
            None,
            {"matches": [row.get("url") for row in matches]},
            "multiple or divergent issues claim the same operation identity",
        )

    return factory.control.mutate_core(request, create, reconcile=reconcile).result
