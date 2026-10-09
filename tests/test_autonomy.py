"""Unattended authority must fail closed at every boundary, including retry and drift."""

from __future__ import annotations

import copy

import pytest
import test_backend_service as shared

from swfactory.autonomy import AutonomyStore, load_policy
from swfactory.backend import autonomous_service as autonomous
from swfactory.backend import scm_service
from swfactory.cell_runtime import identity_for_job
from swfactory.config import FACTORY_ROOT
from swfactory.models import Issue, StageError
from swfactory.security_contract import CanonicalPolicy

REPO = shared.REPO
airflow = shared.airflow
env = shared.env
factory = shared.factory

SHA = "a" * 40


@pytest.fixture
def managed(factory, monkeypatch):
    policy = load_policy()
    job = {"repo": REPO, "issue": "101", "dir": "", "base_branch": "main"}
    cell = factory.cell_store.activate(identity_for_job(job), "backend:test")
    cell = factory.cell_store.patch(
        cell["cell_id"],
        cell["epoch"],
        "bind-test",
        state="running",
        airflow_dag_id="autonomous",
        airflow_run_id="test-run",
        policy_digest=CanonicalPolicy.for_factory_job("autonomous", job).digest(),
    )
    monkeypatch.setenv("GH_TOKEN", "hermetic-test-token")
    body = {
        "cell_id": cell["cell_id"],
        "epoch": cell["epoch"],
        "policy_digest": cell["policy_digest"],
        "operation_key": "test-operation",
        "revision": policy.revision,
        "base_branch": "main",
    }
    return factory, cell, policy, body


@pytest.mark.parametrize(
    "path",
    [
        "config/autonomous.toml",
        "factory.toml",
        "REVIEW.md",
        "blueprints/autonomous.toml",
        "src/swfactory/autonomy.py",
        "src/swfactory/backend/autonomous_service.py",
        "src/swfactory/approval_policy.py",
        "src/swfactory/security_contract.py",
        ".github/workflows/ci.yml",
        "scripts/promotion_policy.py",
        "docs/factory/999/plan.md",
        "../outside.py",
        "/etc/passwd",
        "docs/../src/swfactory/x.py",
    ],
)
def test_policy_cannot_rewrite_its_authority_or_escape_paths(path):
    with pytest.raises((StageError, ValueError)):
        load_policy().check_paths([path])


def test_allowed_paths_and_host_artifact_chain_are_bounded():
    policy = load_policy()
    policy.check_paths(["docs/webhooks.md"])
    policy.check_paths(["docs/factory/101/plan.md"], artifact_prefix="docs/factory/101")
    with pytest.raises(StageError):
        policy.check_paths(["docs/factory/102/plan.md"], artifact_prefix="docs/factory/101")
    with pytest.raises(StageError):
        policy.check_paths([f"docs/{i}.md" for i in range(31)])


@pytest.mark.parametrize("cost,ceiling", [(9, 8), (1, 40), (float("nan"), 8), (-1, 8)])
def test_budget_cannot_expand_from_a_signal_or_agent(cost, ceiling):
    with pytest.raises(StageError):
        load_policy().check_budget(cost, ceiling)


@pytest.mark.parametrize(
    "labels,reason",
    [
        ([], "required_labels_missing"),
        (["factory:autonomous", "human-only"], "denied_label"),
        (["factory:autonomous"], None),
    ],
)
def test_issue_triage_is_policy_selected(labels, reason):
    issue = Issue(id="101", title="Repair a bug", body="Acceptance: regression passes", labels=labels)
    assert load_policy().issue_reason(issue, REPO) == reason


def test_decisions_cannot_be_rebound_inside_an_epoch(tmp_path):
    decisions = AutonomyStore(tmp_path / "decisions.sqlite3")
    decisions.bind("cell:1:plan", {"digest": "original"})
    assert decisions.bind("cell:1:plan", {"digest": "original"}) == {"digest": "original"}
    with pytest.raises(StageError):
        decisions.bind("cell:1:plan", {"digest": "changed"})
    decisions.bind("cell:2:plan", {"digest": "changed"})


def test_gate_approval_has_policy_revision_and_cannot_follow_a_changed_artifact(managed, monkeypatch):
    from swfactory import scm

    factory, cell, policy, body = managed
    monkeypatch.setattr(
        scm.GitHubScm,
        "fetch_issue",
        lambda self, ref: Issue(id=ref, title="Fix", body="Acceptance: works", labels=["factory:autonomous"]),
    )
    evidence = {
        **body,
        "gate": "plan",
        "plan_sha256": "e" * 64,
        "paths": ["docs/webhooks.md"],
        "cost_usd": 1,
        "budget_usd": 8,
        "artifact_sha256": "b" * 64,
        "inputs_digest": "inputs:" + "c" * 64,
    }
    autonomous.approve(factory, {**evidence, "gate": "intent", "paths": []})
    first = autonomous.approve(factory, evidence)
    assert first["actor"] == "policy:" + policy.revision and first["mode"] == "policy"
    assert first["cell_epoch"] == cell["epoch"]
    assert autonomous.approve(factory, evidence) == first
    with pytest.raises(StageError, match="decision changed"):
        autonomous.approve(factory, {**evidence, "artifact_sha256": "d" * 64})
    with pytest.raises(ValueError, match="revision moved"):
        autonomous.approve(factory, {**evidence, "revision": "e" * 64})
    with pytest.raises(ValueError, match="stale"):
        autonomous.approve(factory, {**evidence, "epoch": cell["epoch"] + 1})


class Remote:
    def __init__(self, repo, base, **kwargs):
        self.repo = repo

    snapshot = {}
    writes = []
    lose_response = False

    def fetch_issue(self, ref):
        return Issue(id=ref, title="Fix", body="Acceptance: works", labels=["factory:autonomous"])

    def autonomous_snapshot(self, *args):
        return copy.deepcopy(type(self).snapshot)

    def merge_verified(self, number, sha):
        type(self).writes.append((number, sha))
        type(self).snapshot["pr"]["merged"] = True
        if type(self).lose_response:
            type(self).lose_response = False
            raise StageError("scm", "lost merge response", retryable=True)


@pytest.fixture
def published(managed, monkeypatch):
    factory, cell, policy, body = managed
    receipt = {"head_revision": SHA, "pr_number": 1}
    remote = {
        "pr": {
            "head": {"sha": SHA, "repo": {"full_name": REPO}},
            "base": {"ref": "main", "repo": {"full_name": REPO}},
            "state": "open",
            "draft": False,
            "merged": False,
        },
        "paths": ["docs/webhooks.md"],
        "reviews": [],
        "checks": [
            {
                "id": i,
                "name": name,
                "app": {"id": 15368},
                "head_sha": SHA,
                "status": "completed",
                "conclusion": "success",
            }
            for i, name in enumerate(policy.required_checks)
        ],
    }
    Remote.snapshot, Remote.writes, Remote.lose_response = remote, [], False
    monkeypatch.setattr(scm_service, "GitHubScm", Remote)
    autonomous.remember_publication(
        factory,
        cell,
        {
            "revision": policy.revision,
            "evidence": {"artifact_digests": {"review.json": "b" * 64}},
            "paths": ["docs/webhooks.md"],
        },
        receipt,
    )
    return factory, cell, policy, body


def test_merge_is_backend_owned_sha_fenced_and_idempotent(published):
    factory, _, policy, body = published
    result = autonomous.merge(factory, body)
    assert result == {"state": "merged", "sha": SHA, "pr_number": 1, "policy_revision": policy.revision}
    assert Remote.writes == [(1, SHA)]
    assert autonomous.merge(factory, body) == result
    assert Remote.writes == [(1, SHA)]
    recorded = factory.control.operations.get(body["operation_key"])
    assert recorded["state"] == "committed"


@pytest.mark.parametrize("revocation", ["unlabeled", "blocked", "closed"])
def test_revoked_issue_cannot_merge_even_with_green_checks(published, monkeypatch, revocation):
    factory, _, _, body = published
    issue = Issue(id="101", title="Fix", body="Acceptance: works", labels=["factory:autonomous"])
    if revocation == "unlabeled":
        issue.labels = []
    elif revocation == "blocked":
        issue.labels.append("factory:blocked")
    else:
        issue.state = "closed"
    monkeypatch.setattr(Remote, "fetch_issue", lambda self, ref: issue)
    with pytest.raises(autonomous.Refused, match="autonomous merge blocked"):
        autonomous.merge(factory, body)
    assert Remote.writes == []


def test_issue_revoked_between_merge_read_and_write_is_refused(published, monkeypatch):
    factory, _, _, body = published
    reads = []

    def fetch_issue(self, ref):
        reads.append(ref)
        labels = ["factory:autonomous"] if len(reads) == 1 else []
        return Issue(id=ref, title="Fix", body="Acceptance: works", labels=labels)

    monkeypatch.setattr(Remote, "fetch_issue", fetch_issue)
    with pytest.raises(autonomous.Refused, match="autonomous merge blocked"):
        autonomous.merge(factory, body)
    assert len(reads) >= 2
    assert Remote.writes == []


@pytest.mark.parametrize(
    "mutation", ["head", "branch", "repo", "paths", "failed", "cancelled", "skipped", "spoofed", "review", "draft"]
)
def test_merge_refuses_stale_or_failed_evidence(published, mutation):
    factory, _, _, body = published
    snapshot = Remote.snapshot
    if mutation == "head":
        snapshot["pr"]["head"]["sha"] = "f" * 40
    elif mutation == "branch":
        snapshot["pr"]["base"]["ref"] = "other"
    elif mutation == "repo":
        snapshot["pr"]["head"]["repo"]["full_name"] = "other/repo"
    elif mutation == "paths":
        snapshot["paths"].append("config/autonomous.toml")
    elif mutation in {"failed", "cancelled", "skipped"}:
        snapshot["checks"][0]["conclusion"] = mutation
    elif mutation == "spoofed":
        snapshot["checks"][0]["head_sha"] = "f" * 40
    elif mutation == "review":
        snapshot["reviews"] = [{"state": "CHANGES_REQUESTED"}]
    else:
        snapshot["pr"]["draft"] = True
    with pytest.raises((ValueError, StageError)):
        autonomous.merge(factory, body)
    assert Remote.writes == []


@pytest.mark.parametrize("condition", ["missing", "running", "untrusted-app"])
def test_missing_or_pending_tests_wait_without_merging(published, condition):
    factory, _, _, body = published
    if condition == "missing":
        Remote.snapshot["checks"] = []
    elif condition == "running":
        Remote.snapshot["checks"][0]["status"] = "in_progress"
    else:
        Remote.snapshot["checks"][0]["app"]["id"] = 1
    assert autonomous.merge(factory, body)["state"] == "pending"
    assert Remote.writes == []


def test_later_red_check_supersedes_an_earlier_green(published):
    factory, _, _, body = published
    later = copy.deepcopy(Remote.snapshot["checks"][0])
    later.update(id=100, conclusion="failure")
    Remote.snapshot["checks"].append(later)
    with pytest.raises(ValueError, match="did not pass"):
        autonomous.merge(factory, body)
    assert Remote.writes == []


def test_lost_merge_response_is_reconciled_without_a_second_write(published):
    factory, _, _, body = published
    Remote.lose_response = True
    with pytest.raises(StageError):
        autonomous.merge(factory, body)
    assert autonomous.merge(factory, body)["state"] == "merged"
    assert Remote.writes == [(1, SHA)]


def test_policy_revision_binds_review_skill_and_protection_contract(tmp_path):
    import shutil

    for name in ("config/autonomous.toml", "factory.toml", "REVIEW.md", "blueprints/autonomous.toml"):
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FACTORY_ROOT / name, target)
    first = load_policy(tmp_path).revision
    with (tmp_path / "REVIEW.md").open("a") as file:
        file.write("\nChanged review authority\n")
    assert load_policy(tmp_path).revision != first


@pytest.fixture
def approved(managed, monkeypatch):
    from swfactory import scm

    factory, cell, policy, body = managed
    monkeypatch.setattr(
        scm.GitHubScm,
        "fetch_issue",
        lambda self, ref: Issue(id=ref, title="Fix", body="Acceptance: works", labels=["factory:autonomous"]),
    )
    approvals = []
    for gate in ("intent", "plan"):
        approvals.append(
            autonomous.approve(
                factory,
                {
                    **body,
                    "gate": gate,
                    "plan_sha256": "e" * 64 if gate == "plan" else None,
                    "paths": ["docs/webhooks.md"] if gate == "plan" else [],
                    "cost_usd": 1,
                    "budget_usd": 8,
                    "artifact_sha256": ("b" if gate == "intent" else "c") * 64,
                    "inputs_digest": "inputs:" + "d" * 64,
                },
            )
        )
    evidence = {
        "agent": "claude",
        "tests_passed": True,
        "review_verdict": "approve",
        "blockers": 0,
        "cost_usd": 2,
        "budget_usd": 8,
        "approvals": approvals,
        "artifact_digests": {
            name: "e" * 64
            for name in ("intent.md", "plan.md", "plan.json", "review.json", "metrics.json", "approvals.json")
        },
    }
    evidence["artifact_digests"]["intent.md"] = "b" * 64
    evidence["artifact_digests"]["plan.md"] = "c" * 64
    patch = (
        b"diff --git a/docs/webhooks.md b/docs/webhooks.md\n"
        b"--- a/docs/webhooks.md\n+++ b/docs/webhooks.md\n@@ -1 +1 @@\n-old\n+new\n"
    )
    return factory, cell, policy, {**body, "autonomous_evidence": evidence}, patch


def test_publication_validates_backend_gate_chain_and_actual_patch(approved):
    factory, _, policy, body, patch = approved
    decision = autonomous.validate_publication(factory, body, patch)
    assert decision["revision"] == policy.revision
    assert decision["paths"] == ["docs/webhooks.md"]


@pytest.mark.parametrize("revocation", ["unlabeled", "blocked", "closed"])
def test_revoked_issue_cannot_publish_after_plan_approval(approved, monkeypatch, revocation):
    from swfactory.scm import GitHubScm

    factory, _, _, body, patch = approved
    issue = Issue(id="101", title="Fix", body="Acceptance: works", labels=["factory:autonomous"])
    if revocation == "unlabeled":
        issue.labels = []
    elif revocation == "blocked":
        issue.labels.append("factory:blocked")
    else:
        issue.state = "closed"
    monkeypatch.setattr(GitHubScm, "fetch_issue", lambda self, ref: issue)
    with pytest.raises(autonomous.Refused, match="autonomous publication blocked"):
        autonomous.validate_publication(factory, body, patch)


@pytest.mark.parametrize(
    "mutation", ["digest", "structured-plan", "scripted", "gate", "budget", "tests", "review", "protected", "unplanned"]
)
def test_publication_cannot_launder_an_unapproved_change(approved, mutation):
    factory, _, _, body, patch = approved
    evidence = body["autonomous_evidence"]
    if mutation == "digest":
        evidence["artifact_digests"]["plan.md"] = "f" * 64
    elif mutation == "structured-plan":
        evidence["artifact_digests"]["plan.json"] = "f" * 64
    elif mutation == "scripted":
        evidence["agent"] = "scripted"
    elif mutation == "gate":
        evidence["approvals"][0]["actor"] = "agent"
    elif mutation == "budget":
        evidence["budget_usd"] = 40
    elif mutation == "tests":
        evidence["tests_passed"] = False
    elif mutation == "review":
        evidence["blockers"] = 1
    elif mutation == "protected":
        patch = patch.replace(b"docs/webhooks.md", b"config/autonomous.toml")
    else:
        patch = patch.replace(b"docs/webhooks.md", b"docs/other.md")
    with pytest.raises((ValueError, StageError)):
        autonomous.validate_publication(factory, body, patch)


def test_triage_records_blocked_outcome_without_spending_or_submitting(managed, monkeypatch):
    from swfactory import scm

    factory, _, _, _ = managed
    monkeypatch.setattr(scm.GitHubScm, "fetch_issue", lambda self, ref: Issue(id=ref, title="Fix", body="Acceptance"))
    monkeypatch.setattr(factory, "submit", lambda body: pytest.fail("blocked issue must not enter execution"))
    result = autonomous.triage(factory, {"issue": "101"})
    assert result["state"] == "blocked" and result["reason"] == "required_labels_missing"
    with autonomous.store(factory).connect() as db:
        rows = db.execute("SELECT value FROM decisions").fetchall()
    assert any("required_labels_missing" in row[0] for row in rows)


def test_triage_admits_eligible_issue_without_manual_submit(managed, monkeypatch):
    from swfactory import scm

    factory, _, _, _ = managed
    monkeypatch.setattr(
        scm.GitHubScm,
        "fetch_issue",
        lambda self, ref: Issue(id=ref, title="Fix", body="Acceptance", labels=["factory:autonomous"]),
    )
    submitted = []
    monkeypatch.setattr(factory, "submit", lambda body: submitted.append(body) or {"state": "queued"})
    result = autonomous.triage(factory, {"issue": "101"})
    assert result["state"] == "queued"
    assert submitted == [{"line": "autonomous", "issues": ["101"], "actor": "github"}]


def test_github_adapter_reads_artifacts_at_sha_and_uses_atomic_merge(monkeypatch):
    import hashlib

    from swfactory.scm import GitHubScm

    adapter = GitHubScm(REPO, "main")
    calls = []
    raw = "host-owned review\n"

    def api(argv):
        calls.append(argv)
        endpoint = next(value for value in argv if value.startswith("repos/"))
        if "/merge" in endpoint:
            return {"merged": True}
        if "/files?" in endpoint:
            return [[{"filename": "docs/webhooks.md", "status": "modified"}]]
        if "/check-runs?" in endpoint:
            return [{"check_runs": []}]
        if "/reviews?" in endpoint:
            return [[]]
        return {"changed_files": 1}

    monkeypatch.setattr(adapter, "_gh_json", api)
    monkeypatch.setattr(adapter, "_exec", lambda argv, cwd: calls.append(argv) or raw)
    adapter.autonomous_snapshot(1, SHA, "docs/factory/101", {"review.json": hashlib.sha256(raw.encode()).hexdigest()})
    assert any(f"ref={SHA}" in " ".join(argv) for argv in calls)
    adapter.merge_verified(1, SHA)
    assert f"sha={SHA}" in calls[-1]
    with pytest.raises(StageError, match="artifact differs"):
        adapter.autonomous_snapshot(1, SHA, "docs/factory/101", {"review.json": "f" * 64})


def test_approved_patch_publishes_then_merges_through_managed_mutations(approved, monkeypatch):
    import base64

    from swfactory.recovery_accounting import PublicationReceipt
    from swfactory.scm import patch_content_digest

    backend, cell, policy, body, patch = approved

    class PublishingRemote(Remote):
        receipt = None
        publications = 0

        def observe_publication(self, branch):
            return type(self).receipt

        def publish(self, **kwargs):
            type(self).publications += 1
            type(self).receipt = PublicationReceipt(
                repository=REPO,
                base_revision="main",
                head_revision=SHA,
                content_digest=patch_content_digest(kwargs["patch"]),
                branch=kwargs["branch"],
                pr_number=1,
                pr_state="open",
                url="https://github.com/example/pull/1",
            )
            return type(self).receipt.url

    PublishingRemote.snapshot = {
        "pr": {
            "head": {"sha": SHA, "repo": {"full_name": REPO}},
            "base": {"ref": "main", "repo": {"full_name": REPO}},
            "state": "open",
            "draft": False,
            "merged": False,
        },
        "paths": ["docs/webhooks.md"],
        "reviews": [],
        "checks": [
            {
                "id": i,
                "name": name,
                "app": {"id": 15368},
                "head_sha": SHA,
                "status": "completed",
                "conclusion": "success",
            }
            for i, name in enumerate(policy.required_checks)
        ],
    }
    PublishingRemote.writes = []
    monkeypatch.setattr(scm_service, "GitHubScm", PublishingRemote)
    publication = {
        **body,
        "operation_key": "publish-integrated",
        "branch": "swf-101-autonomous",
        "title": "Fix documentation",
        "body": "Managed evidence",
        "labels": ["autonomous"],
        "allowed_prefixes": [""],
        "patch_b64": base64.b64encode(patch).decode(),
    }
    receipt = backend.operation("/scm/publish", publication)
    assert receipt["head_revision"] == SHA
    assert PublishingRemote.publications == 1
    assert backend.operation("/scm/publish", publication) == receipt
    assert PublishingRemote.publications == 1
    with pytest.raises(ValueError, match="already sealed"):
        backend.operation("/scm/publish", {**publication, "branch": "different-branch"})
    assert PublishingRemote.publications == 1
    stored = autonomous.store(backend).get(f"{cell['cell_id']}:{cell['epoch']}:publication")
    assert stored["receipt"] == receipt
    result = backend.operation("/scm/merge", {**body, "operation_key": "merge-integrated"})
    assert result["state"] == "merged"
    assert PublishingRemote.writes == [(1, SHA)]
    assert backend.control.operations.get("publish-integrated")["state"] == "committed"
    assert backend.control.operations.get("merge-integrated")["state"] == "committed"


@pytest.mark.parametrize("mutation", ["job-budget", "stage-budget", "target", "revision", "unmanaged"])
def test_widened_runtime_settings_are_refused_before_first_stage(mutation):
    from types import SimpleNamespace

    from swfactory.autonomy import enforce_runtime_policy
    from swfactory.blueprint import load

    cfg = SimpleNamespace(
        repo=REPO, target_dir="", base_branch="main", scm="github", max_budget_usd=8, max_budget_usd_per_stage=2
    )
    binding = {
        "managed": True,
        "policy_digest": CanonicalPolicy.for_factory_job(
            "autonomous", {"repo": REPO, "dir": "", "base_branch": "main"}
        ).digest(),
    }
    enforce_runtime_policy(cfg, load("autonomous"), binding)
    if mutation == "job-budget":
        cfg.max_budget_usd = 40
    elif mutation == "stage-budget":
        cfg.max_budget_usd_per_stage = 40
    elif mutation == "target":
        cfg.target_dir = "other-product"
    elif mutation == "revision":
        binding["policy_digest"] = "policy:" + "f" * 64
    else:
        binding["managed"] = False
    with pytest.raises(StageError):
        enforce_runtime_policy(cfg, load("autonomous"), binding)


def test_removed_intake_label_blocks_before_sandbox_or_agent_creation(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from swfactory import runtime
    from swfactory.blueprint import load
    from swfactory.config import Config

    cfg = Config(
        blueprint="autonomous",
        issue="101",
        target_dir="",
        run_id="removed-label",
        agent="claude",
        sandbox="islo",
        scm="github",
    )
    binding = {
        "managed": True,
        "cell_id": "cell_" + "a" * 24,
        "epoch": 1,
        "policy_digest": CanonicalPolicy.for_factory_job(
            "autonomous", {"repo": REPO, "dir": "", "base_branch": "main"}
        ).digest(),
    }
    scm = SimpleNamespace(fetch_issue=lambda ref: Issue(id=ref, title="Fix", body="Acceptance"))
    monkeypatch.setattr(runtime, "make_sandbox", lambda *args, **kwargs: pytest.fail("must not create compute"))
    with pytest.raises(StageError, match="required_labels_missing"):
        runtime.ctx_for(
            cfg, blueprint=load("autonomous"), run_dir=tmp_path / "run", scm_override=scm, cell_binding=binding
        )
