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
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from swfactory.authority import ResourceKind
from swfactory.core_capabilities import CoreMutationRequest
from swfactory.credential_lease import LeaseBinding
from swfactory.durable_admission import request_digest
from swfactory.idempotency import MutationOutcome
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
from swfactory.security_contract import Capability, SecurityContext

from .service import Factory, Refused, _attempt, airflow_binding, leased, text

MAX_PATCH_BYTES = 12 * 1024 * 1024
_MARKER_PREFIX = "<!-- swfactory-patch-sha256:"
_ISSUE_MARKER_PREFIX = "<!-- swfactory-operation:"


def autonomy_status(factory: Factory, body: dict[str, Any]) -> Any:
    from swfactory.autonomy_status import status

    limit = body.get("limit", 50)
    if type(limit) is not int:
        raise ValueError("limit must be an integer")
    return status(factory.state_root, limit=limit)


def fetch_issue(factory: Factory, body: dict[str, Any], base_branch: str) -> dict[str, Any]:
    # Filesystem issue refs are a local-demo affordance. The backend holds publication and
    # Airflow credentials, so a network caller may resolve GitHub issue numbers only.
    ref = text(body, "ref", max_len=128).strip()
    if not ref.isdigit():
        raise Refused(400, "ref must be an issue number over the API; a path is local-only")
    return GitHubScm(factory.repo, base_branch).fetch_issue(ref).model_dump(mode="json")


def linear_source(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    from swfactory.linear_intake import source_issue

    ref = text(body, "ref", max_len=128)
    cell = factory._cell(text(body, "cell_id"))
    epoch = body.get("epoch")
    if type(epoch) is not int or epoch != cell["epoch"]:
        raise Refused(409, "accepted Linear source requires the current Cell epoch")
    if body.get("policy_digest") != cell.get("policy_digest") or not cell.get("policy_digest"):
        raise Refused(409, "accepted Linear source requires the current Cell policy")
    if cell.get("issue") != ref:
        raise Refused(403, "accepted Linear source differs from this Cell's issue")
    airflow_binding(cell)
    record = factory.control.admission.work_order_for_cell(str(cell["cell_id"]), epoch)
    order = record.payload
    if request_digest(order) != record.request_digest:
        raise Refused(409, "accepted Linear work order failed integrity verification")
    if "accepted_source" not in order:
        raise Refused(409, "Cell has no accepted Linear source")
    if body.get("blueprint_digest") != order["blueprint"]["identity"]:
        raise Refused(409, "worker blueprint differs from the accepted Linear work order")
    try:
        issue = source_issue(order["accepted_source"], expected_ref=ref)
    except ValueError:
        raise Refused(409, "accepted Linear source failed integrity verification") from None
    if order["accepted_source"]["intent_digest"] != order["work_source"]["intent_digest"]:
        raise Refused(409, "accepted Linear source differs from its work order")
    return issue.model_dump(mode="json")


def _request(
    factory: Factory,
    *,
    cell: dict[str, Any],
    operation_key: str,
    kind: str,
    digest: str,
    replay_safe: bool,
    parts: tuple[str, ...],
) -> CoreMutationRequest:
    return CoreMutationRequest(
        airflow=airflow_binding(cell),
        security=SecurityContext(
            tenant=factory.repo,
            cell_id=str(cell["cell_id"]),
            epoch=int(cell["epoch"]),
            role="publisher",
            capabilities=frozenset({Capability.PUBLISH_GIT}),
        ),
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


def _lease_binding(
    factory: Factory,
    cell: dict[str, Any],
    operation_key: str,
    *,
    stage_id: str,
) -> LeaseBinding:
    run_id = str(cell.get("airflow_run_id") or "unbound-run")
    compute = cell.get("compute")
    compute = compute if isinstance(compute, dict) else {}
    sandbox_id = str(compute.get("sandbox_id") or compute.get("name") or compute.get("handle") or "backend-publication")
    return LeaseBinding(
        factory_run_id=f"{cell.get('airflow_dag_id') or 'factory'}:{run_id}:{cell.get('map_index', 0)}",
        dag_run_id=run_id,
        task_instance_id=f"job[{cell.get('map_index', 0)}].{stage_id}",
        stage_id=stage_id,
        sandbox_id=sandbox_id,
        attempt_number=_attempt(factory, operation_key),
        cell_id=str(cell["cell_id"]),
        epoch=int(cell["epoch"]),
        operation_key=operation_key,
        policy_digest=str(cell["policy_digest"]),
    )


def _with_github_lease[T](
    factory: Factory,
    cell: dict[str, Any],
    operation_key: str,
    *,
    base_branch: str,
    capability: str,
    purpose: str,
    action: Callable[[GitHubScm], T],
) -> T:
    binding = _lease_binding(factory, cell, operation_key, stage_id="deliver")
    with leased(factory, binding, capability, purpose, ttl_s=120.0) as token:
        return action(GitHubScm(factory.repo, base_branch, token=token))


def _bounded_body_and_labels(body: dict[str, Any]) -> tuple[str, list[str]]:
    content = body.get("body")
    if not isinstance(content, str) or len(content) > 2 * 1024 * 1024:
        raise ValueError("body must be a string of at most 2 MiB")
    labels = body.get("labels") or []
    if not isinstance(labels, list) or any(not isinstance(value, str) or len(value) > 128 for value in labels):
        raise ValueError("labels must be an array of bounded strings")
    return content, labels


def publish(factory: Factory, body: dict[str, Any], base_branch: str) -> dict[str, Any]:
    initiating_actor = text(body, "actor", default="airflow-worker", max_len=128)
    cell, _, _, operation_key = factory.fenced(body, what="publication")
    branch = text(body, "branch")
    title = text(body, "title", max_len=512)
    pr_body, labels = _bounded_body_and_labels(body)
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
    from .autonomous_service import validate_publication

    try:
        autonomous = validate_publication(factory, body, patch)
    except StageError as error:
        raise Refused(403, str(error)) from error
    patch_digest = hashlib.sha256(patch).hexdigest()
    # The marker is a lookup hint for people reading the PR. It is not the proof: anyone with write
    # access can edit a body, so a retry is judged on git content (`patch_content_digest`) instead.
    marker = f"{_MARKER_PREFIX}{patch_digest} -->"
    publish_body = pr_body.rstrip() + "\n\n" + marker + "\n"
    base_branch = str(base_branch)
    digest = request_digest(
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
            "autonomous": autonomous,
        }
    )
    request = _request(
        factory,
        cell=cell,
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

    def create() -> dict[str, Any]:
        def apply(scoped: GitHubScm) -> dict[str, Any]:
            # Re-observe inside the write lease before mutating. Reconciliation may have happened
            # under a previous read lease; another publisher can win between those two moments.
            before = judge(scoped.observe_publication(branch))
            if before.status == "committed":
                return dict(before.result)
            if before.status not in {"definitely_absent"}:
                raise StageError(
                    "scm",
                    f"publication preflight refused remote state: {before.detail}",
                    retryable=before.status == "ambiguous",
                )
            scoped.publish(
                branch=branch,
                patch=patch,
                title=title,
                body=publish_body,
                labels=labels,
                allowed_prefixes=allowed,
            )
            # Read the receipt back under the same one-shot trusted lease; the raw credential never
            # crosses into Airflow/XCom/sandbox state.
            outcome = judge(scoped.observe_publication(branch))
            if outcome.status != "committed":
                raise StageError(
                    "scm",
                    f"publication did not verify on the remote: {outcome.detail}",
                    retryable=True,
                )
            return dict(outcome.result)

        return _with_github_lease(
            factory,
            cell,
            operation_key,
            base_branch=base_branch,
            capability="github.publish",
            purpose="publish-pull-request",
            action=apply,
        )

    def reconcile() -> MutationOutcome:
        try:
            receipt = _with_github_lease(
                factory,
                cell,
                operation_key,
                base_branch=base_branch,
                capability="github.read",
                purpose="observe-publication",
                action=lambda scoped: scoped.observe_publication(branch),
            )
            return judge(receipt)
        except StageError as error:
            return MutationOutcome(
                "ambiguous",
                None,
                {"branch": branch},
                f"remote observation failed: {error}",
            )

    result = factory.control.mutate_core(request, create, reconcile=reconcile).result
    if autonomous is not None:
        from .autonomous_service import remember_publication

        remember_publication(factory, cell, autonomous, result)
    return result


def _publication_identity(repo: str, base: str, branch: str, content_digest: str) -> RemoteIdentity:
    """Repository, base, branch and git content: the four things a publication receipt must bind."""
    return RemoteIdentity("github_publish", repo, f"{base}:{branch}", content_digest)


def open_issue(factory: Factory, body: dict[str, Any], base_branch: str) -> dict[str, Any]:
    initiating_actor = text(body, "actor", default="airflow-worker", max_len=128)
    cell, _, _, operation_key = factory.fenced(body, what="publication")
    if str(cell.get("issue", "")).startswith("linear_"):
        raise Refused(403, "Linear work cannot create GitHub issues; native incident projection is not implemented")
    title = text(body, "title", max_len=512)
    issue_body, labels = _bounded_body_and_labels(body)
    digest = request_digest(
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
        operation_key=operation_key,
        kind="github_issue",
        digest=digest,
        replay_safe=True,
        parts=(title, digest),
    )

    def create() -> dict[str, Any]:
        return _with_github_lease(
            factory,
            cell,
            operation_key,
            base_branch=base_branch,
            capability="github.publish",
            purpose="create-issue",
            action=lambda scoped: {"url": scoped.open_issue(title=title, body=marked_body, labels=labels)},
        )

    def reconcile() -> MutationOutcome:
        rows = _with_github_lease(
            factory,
            cell,
            operation_key,
            base_branch=base_branch,
            capability="github.read",
            purpose="observe-issue",
            action=lambda scoped: scoped.search_issues(operation_key, limit=20),
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
