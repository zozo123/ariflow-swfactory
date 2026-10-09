"""Backend-owned autonomous decisions and SHA-fenced merge; agents have no mutation authority."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime

from swfactory.autonomy import LINE, AutonomousPolicy, AutonomyStore, gate_key, load_policy, publication_key
from swfactory.durable_admission import request_digest
from swfactory.idempotency import MutationOutcome
from swfactory.models import Approval, StageError
from swfactory.scm import GitHubScm, patch_content_digest, patch_paths

from .service import LIVE_CELL_STATES, Factory, Refused, text


def store(factory: Factory) -> AutonomyStore:
    return AutonomyStore(factory.state_root / "autonomy.sqlite3")


def authority(factory: Factory, body: dict):
    actor = text(body, "actor", default="airflow-worker", max_len=128)
    cell, _, _, operation_key = factory.fenced(body, what="publication")
    policy = load_policy()
    if not policy.enabled or factory.repo != policy.repository or cell.get("airflow_dag_id") != LINE:
        raise Refused(403, "Cell is outside autonomous policy")
    if cell.get("state") not in LIVE_CELL_STATES:
        raise Refused(409, "autonomous Cell is no longer live")
    from swfactory.cell_runtime import identity_for_job

    job = {"repo": factory.repo, "dir": "", "base_branch": policy.base_branch, "issue": cell["issue"]}
    if identity_for_job(job).stable_id() != cell["cell_id"]:
        raise Refused(403, "autonomous target is outside policy")
    if cell["policy_digest"] != policy.job_policy_digest(factory.repo) or body.get("revision") != policy.revision:
        raise Refused(409, "autonomous policy revision moved; a new Cell epoch is required")
    if body.get("base_branch", policy.base_branch) != policy.base_branch:
        raise Refused(403, "autonomous base branch is outside policy")
    return policy, cell, operation_key, actor


def _require_eligible(policy: AutonomousPolicy, scm: GitHubScm, ref: str, repo: str, verb: str) -> None:
    """Refuse an issue the checked-in policy does not admit, naming the step it blocks."""
    if reason := policy.issue_reason(scm.fetch_issue(ref), repo):
        raise Refused(403, f"autonomous {verb} blocked: {reason}")


def policy_gate(factory: Factory, body: dict) -> dict:
    """``approve``, leaving a blocked-gate record behind every refusal."""
    try:
        return approve(factory, body)
    except (Refused, StageError) as error:
        digest = hashlib.sha256(repr(sorted(body.items())).encode()).hexdigest()
        store(factory).bind(f"blocked-gate:{digest}", {"state": "blocked", "reason": str(error)})
        raise


def triage(factory: Factory, body: dict) -> dict:
    policy = load_policy()
    ref = text(body, "issue", max_len=20)
    if not ref.isdigit():
        raise ValueError("triage requires a GitHub issue number")
    issue = GitHubScm(factory.repo, policy.base_branch).fetch_issue(ref)
    reason = policy.issue_reason(issue, factory.repo)
    digest = hashlib.sha256(issue.model_dump_json().encode()).hexdigest()
    outcome = {
        "state": "blocked" if reason else "eligible",
        "reason": reason or "policy_allows",
        "revision": policy.revision,
    }
    store(factory).bind(f"triage:{factory.repo}:{ref}:{policy.revision}:{digest}", outcome)
    if reason:
        return outcome
    return {**factory.submit({"line": LINE, "issues": [ref], "actor": "github"}), "revision": policy.revision}


def approve(factory: Factory, body: dict) -> dict:
    policy, cell, _, _ = authority(factory, body)
    gate = body.get("gate")
    if gate not in {"intent", "plan"}:
        raise ValueError("policy gate must be intent or plan")
    _require_eligible(policy, GitHubScm(factory.repo, policy.base_branch), str(cell["issue"]), factory.repo, "triage")
    cost, budget = body.get("cost_usd"), body.get("budget_usd")
    if type(cost) not in (int, float) or type(budget) not in (int, float):
        raise ValueError("cost_usd and budget_usd must be numbers")
    policy.check_budget(cost, budget)
    paths = body.get("paths")
    if not isinstance(paths, list) or any(not isinstance(path, str) for path in paths):
        raise ValueError("paths must be strings")
    if gate == "plan":
        intent = store(factory).get(gate_key(cell["cell_id"], cell["epoch"], "intent"))
        if not intent or intent["approval"]["inputs_digest"] != body.get("inputs_digest"):
            raise Refused(403, "plan gate requires intent approval for the same accepted inputs")
        policy.check_paths(paths)
    now = datetime.now(UTC)
    approval = Approval(
        gate=gate,
        decision="approve",
        actor="policy:" + policy.revision,
        mode="policy",
        at=now,
        artifact_sha256=body.get("artifact_sha256"),
        inputs_digest=body.get("inputs_digest"),
        cell_id=cell["cell_id"],
        cell_epoch=cell["epoch"],
    )
    if approval.artifact_sha256 is None or approval.inputs_digest is None:
        raise ValueError("policy decisions require artifact and accepted-input digests")
    plan_sha256 = body.get("plan_sha256")
    if gate == "plan" and (not isinstance(plan_sha256, str) or not re.fullmatch("[a-f0-9]{64}", plan_sha256)):
        raise ValueError("plan approval requires the structured plan digest")
    decision = {
        "approval": approval.model_dump(mode="json"),
        "paths": paths,
        "budget_usd": budget,
        "plan_sha256": plan_sha256,
    }
    key = gate_key(cell["cell_id"], cell["epoch"], gate)
    previous = store(factory).get(key)
    if previous:
        decision["approval"]["at"] = previous["approval"]["at"]
    store(factory).bind(key, decision)
    return decision["approval"]


def validate_publication(factory: Factory, body: dict, patch: bytes) -> dict | None:
    """Validate host-generated evidence against backend-owned approvals before publication."""
    cell = factory._cell(text(body, "cell_id"))
    if cell.get("airflow_dag_id") != LINE:
        return None
    policy, cell, _, _ = authority(factory, body)
    _require_eligible(
        policy, GitHubScm(factory.repo, policy.base_branch), str(cell["issue"]), factory.repo, "publication"
    )
    evidence = body.get("autonomous_evidence")
    if not isinstance(evidence, dict):
        raise Refused(403, "autonomous publication lacks host evidence")
    if evidence.get("agent") != "claude":
        raise Refused(403, "scripted replay cannot authorize autonomous publication or merge")
    if (
        evidence.get("tests_passed") is not True
        or evidence.get("review_verdict") != "approve"
        or evidence.get("blockers") != 0
    ):
        raise Refused(403, "autonomous tests or review did not pass")
    policy.check_budget(evidence.get("cost_usd", float("inf")), evidence.get("budget_usd", float("inf")))
    expected = []
    decisions = store(factory)
    for gate in ("intent", "plan"):
        decision = decisions.get(gate_key(cell["cell_id"], cell["epoch"], gate))
        if not decision:
            raise Refused(403, f"autonomous {gate} gate is unanswered")
        expected.append(decision["approval"])
    if evidence.get("approvals") != expected:
        raise Refused(409, "publication does not carry backend-owned gate decisions")
    if expected[0]["inputs_digest"] != expected[1]["inputs_digest"]:
        raise Refused(409, "autonomous gates approved different accepted inputs")
    artifact_prefix = "docs/factory/" + str(cell["issue"])
    changed = patch_paths(patch)
    policy.check_paths(changed, artifact_prefix=artifact_prefix)
    planned = set(decision["paths"])
    if any(path not in planned and not path.startswith(artifact_prefix + "/") for path in changed):
        raise Refused(403, "published diff contains unapproved plan paths")
    for name in ("intent.md", "plan.md", "plan.json", "review.json", "metrics.json", "approvals.json"):
        digest = evidence.get("artifact_digests", {}).get(name)
        if not isinstance(digest, str) or not re.fullmatch("[a-f0-9]{64}", digest):
            raise ValueError("publication requires artifact digests")
    if evidence["artifact_digests"]["plan.json"] != decision["plan_sha256"]:
        raise Refused(409, "structured plan moved after approval")
    for approval, name in zip(expected, ("intent.md", "plan.md"), strict=True):
        if evidence["artifact_digests"][name] != approval["artifact_sha256"]:
            raise Refused(409, "approved artifact moved before publication")
    decision = {"revision": policy.revision, "evidence": evidence, "paths": changed}
    previous = decisions.get(publication_key(cell["cell_id"], cell["epoch"]))
    if previous and (
        any(previous[name] != decision[name] for name in decision)
        or previous["receipt"].get("branch") != body.get("branch")
        or previous["receipt"].get("content_digest") != patch_content_digest(patch)
    ):
        raise Refused(409, "publication is already sealed for this Cell epoch")
    return decision


def remember_publication(factory: Factory, cell: dict, decision: dict, receipt: dict) -> None:
    store(factory).bind(publication_key(cell["cell_id"], cell["epoch"]), {**decision, "receipt": receipt})


def merge(factory: Factory, body: dict) -> dict:
    from .scm_service import _request, _with_github_lease

    policy, cell, key, actor = authority(factory, body)
    recorded = store(factory).get(publication_key(cell["cell_id"], cell["epoch"]))
    if not recorded or recorded["revision"] != policy.revision:
        raise Refused(403, "no policy-authorized publication exists for this Cell epoch")
    receipt = recorded["receipt"]
    sha, number = receipt["head_revision"], receipt["pr_number"]
    if not re.fullmatch("[a-f0-9]{40}", sha) or type(number) is not int:
        raise Refused(409, "publication lacks an immutable GitHub head")
    prefix = "docs/factory/" + str(cell["issue"])

    def inspect(scoped):
        snapshot = scoped.autonomous_snapshot(number, sha, prefix, recorded["evidence"]["artifact_digests"])
        pr = snapshot["pr"]
        if pr.get("head", {}).get("sha") != sha or pr.get("base", {}).get("ref") != policy.base_branch:
            raise Refused(409, "PR moved after publication or targets another branch")
        for side in ("head", "base"):
            if pr.get(side, {}).get("repo", {}).get("full_name") != factory.repo:
                raise Refused(403, "PR belongs to another repository")
        if pr.get("merged"):
            return {"state": "merged", "sha": sha, "pr_number": number, "policy_revision": policy.revision}
        if pr.get("state") != "open" or pr.get("draft"):
            raise Refused(403, "PR is not open and ready for merge")
        _require_eligible(policy, scoped, str(cell["issue"]), factory.repo, "merge")
        policy.check_paths(snapshot["paths"], artifact_prefix=prefix)
        if set(snapshot["paths"]) != set(recorded["paths"]):
            raise Refused(409, "PR diff moved after publication")
        checks = snapshot["checks"]
        for name in policy.required_checks:
            matches = [
                check
                for check in checks
                if check.get("name") == name and check.get("app", {}).get("id") == policy.checks_app_id
            ]
            if not matches:
                return {"state": "pending", "reason": f"missing check: {name}"}
            # Re-runs supersede older results; never select an earlier green over a later red.
            check = max(matches, key=lambda value: value.get("id", 0))
            if check.get("head_sha") != sha:
                raise Refused(409, "test evidence belongs to another SHA")
            if check.get("status") != "completed":
                return {"state": "pending", "reason": f"check running: {name}"}
            if check.get("conclusion") != "success":
                raise Refused(403, f"required check did not pass: {name}")
        decisive = {}
        for review in sorted(snapshot["reviews"], key=lambda value: value.get("id", 0)):
            if review.get("state") in {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}:
                decisive[review.get("user", {}).get("id", "unknown")] = review["state"]
        if "CHANGES_REQUESTED" in decisive.values():
            raise Refused(403, "GitHub review has unresolved blockers")
        return {"state": "ready", "sha": sha, "pr_number": number, "policy_revision": policy.revision}

    def leased(capability, action):
        return _with_github_lease(
            factory,
            cell,
            key,
            base_branch=policy.base_branch,
            capability=capability,
            purpose="autonomous-merge",
            action=action,
        )

    initial = leased("github.read", inspect)
    if initial["state"] == "pending":
        return initial
    request = _request(
        factory,
        cell=cell,
        operation_key=key,
        kind="github_merge",
        digest=request_digest({"sha": sha, "pr": number, "revision": policy.revision, "actor": actor}),
        replay_safe=True,
        parts=(str(number), sha),
    )

    def apply(scoped):
        current = inspect(scoped)
        if current["state"] == "merged":
            return current
        if current["state"] != "ready":
            raise Refused(409, "merge evidence changed before mutation")
        scoped.merge_verified(number, sha)
        result = inspect(scoped)
        if result["state"] != "merged":
            raise StageError("scm", "merge response is unverified", retryable=True)
        return result

    def reconcile():
        try:
            observed = leased("github.read", inspect)
            if observed["state"] == "merged":
                return MutationOutcome("committed", observed, observed, "same-SHA merge observed")
            return MutationOutcome("definitely_absent", None, observed, "verified PR is not merged")
        except (StageError, Refused):
            return MutationOutcome("ambiguous", None, {}, "merge evidence unavailable or divergent")

    return factory.control.mutate_core(request, lambda: leased("github.publish", apply), reconcile=reconcile).result
