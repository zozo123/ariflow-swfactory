#!/usr/bin/env python3
"""Create 400 concrete Liquid400 issues and 400 disjoint PR-ready branches.

GitHub Actions is intentionally used only for issue and branch creation. Repository policy blocks
Actions from opening pull requests; the authenticated GitHub connector opens the PRs after this
script has materialized every branch. The script is idempotent by exact issue title and branch.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPO = os.environ.get("GITHUB_REPOSITORY", "zozo123/ariflow-swfactory")
BASE = os.environ.get("LIQUID400_BASE", "spectrum/400-pr-wave")
CHUNK = 10
MAX_STALL_ROUNDS = 12

ROLES = ("authority", "airflow", "workgraph", "recovery", "security", "evidence", "operator")

DOMAINS: tuple[tuple[str, str, str], ...] = (
    ("authority-transfer", "Authority transfer", "Make takeover and reactivation explicit, fenced, auditable and reversible."),
    ("epoch-fencing", "Epoch fencing", "Reject stale writers across every mutation boundary, including delayed retries."),
    ("airflow-recovery", "Airflow recovery", "Recover lifecycle state after scheduler, worker and metadata-database restarts."),
    ("mapped-task-cancel", "Mapped task cancellation", "Propagate cancellation through mapped jobs without orphaning resources."),
    ("approval-durability", "Approval durability", "Persist human gates so restarts never silently approve, reject or duplicate them."),
    ("sandbox-provision", "Sandbox provisioning", "Provision disposable compute deterministically from durable Cell intent."),
    ("sandbox-reclaim", "Sandbox reclamation", "Reclaim leaked or half-destroyed compute without resurrecting stale incarnations."),
    ("provider-drift", "Provider drift", "Detect capability and behavioral drift before provider changes corrupt lifecycle semantics."),
    ("operation-replay", "Operation replay", "Replay idempotent external mutations safely after crashes and ambiguous responses."),
    ("mutation-observation", "Mutation observation", "Observe external state before retrying side effects whose outcome is uncertain."),
    ("cleanup-debt", "Cleanup debt", "Track, age, prioritize and repair incomplete cleanup as first-class operational debt."),
    ("admission-fairness", "Admission fairness", "Keep weighted admission fair across repos, actors, blueprints and priorities."),
    ("queue-starvation", "Queue starvation", "Prove bounded waiting and expose starvation before it becomes invisible backlog."),
    ("repo-races", "Repository races", "Coordinate concurrent jobs against moving repository heads without silent overwrite."),
    ("git-publication", "Git publication", "Publish branches and pull requests exactly once with deterministic identities."),
    ("webhook-replay", "Webhook replay", "Deduplicate, order and replay webhook delivery without duplicate lifecycle starts."),
    ("intake-dedupe", "Intake deduplication", "Collapse duplicate work orders while preserving source provenance and intent."),
    ("workspace-isolation", "Workspace isolation", "Prevent cross-job filesystem leakage under high concurrency and retries."),
    ("artifact-integrity", "Artifact integrity", "Seal build, test and review artifacts so consumers can detect tampering or drift."),
    ("cache-consistency", "Cache consistency", "Share caches without stale contamination across repos, providers and generations."),
    ("storage-migration", "Storage migration", "Migrate durable state online while preserving compatibility and rollback."),
    ("evidence-sealing", "Evidence sealing", "Build append-only evidence chains that explain every decision and side effect."),
    ("provenance-sbom", "Provenance and SBOM", "Bind releases to source, dependencies, policy, tests and build provenance."),
    ("otel-correlation", "OpenTelemetry correlation", "Correlate Airflow, Cell, sandbox, mutation and publication traces end to end."),
    ("slo-budget", "SLO budgets", "Define measurable availability, latency, recovery and cleanup SLOs with actionable burn alerts."),
    ("cost-attribution", "Cost attribution", "Attribute compute, model, storage and retry cost to Cells, repos and generations."),
    ("secret-scope", "Secret scope", "Broker secrets by role and operation while preventing sandbox persistence and lateral reuse."),
    ("policy-drift", "Policy drift", "Detect policy-version divergence across backend, scheduler, workers and operator surfaces."),
    ("tenant-isolation", "Tenant isolation", "Enforce tenant boundaries in state, compute, credentials, queues and evidence."),
    ("release-promotion", "Release promotion", "Promote only evidence-backed generations with deterministic gates and rollback points."),
    ("rollback", "Rollback", "Restore a known-good generation without reviving invalid state or stale external operations."),
    ("disaster-recovery", "Disaster recovery", "Reconstruct durable authority, operations and evidence after regional state loss."),
    ("cli-contract", "CLI contract", "Keep scripting-safe command grammar, exit codes and JSON envelopes stable across releases."),
    ("tui-parity", "TUI parity", "Ensure TUI decisions and data come from the same application contracts as the CLI and API."),
    ("backend-versioning", "Backend versioning", "Negotiate API and schema compatibility during rolling upgrades without split authority."),
    ("fault-injection", "Fault injection", "Inject realistic scheduler, provider, network, storage and publication failures deterministically."),
    ("benchmarks", "Benchmarks", "Measure throughput, tail latency, recovery and cost reproducibly with retained evidence."),
    ("fuzzing", "Fuzzing and property tests", "Explore state-machine, parser and recovery invariants beyond hand-written scenarios."),
    ("factory-generations", "Factory generations", "Bound child-factory experimentation and forbid unevaluated self-promotion."),
    ("stabilization-health", "Stabilization health", "Measure entropy collapse, duplicate abstraction removal and release readiness."),
)

CONCERNS: tuple[tuple[str, str, str], ...] = (
    ("C01", "canonical invariant", "Define stable identities, ownership and a machine-checkable invariant."),
    ("C02", "durable persistence", "Persist state atomically with migration, crash recovery and compatibility semantics."),
    ("C03", "versioned API", "Expose the capability through a versioned backend contract with refusal behavior."),
    ("C04", "runtime integration", "Wire the capability into the real Airflow-to-Cell execution path without another scheduler."),
    ("C05", "operator surface", "Expose deterministic CLI/TUI/API inspection and repair without hidden business logic."),
    ("C06", "security hardening", "Apply least privilege, trust-zone boundaries and explicit secret/policy handling."),
    ("C07", "failure recovery", "Specify cancellation, timeout, stale-writer, retry, ambiguous-result and restart behavior."),
    ("C08", "scale pressure", "Prove bounded behavior under concurrency, backlog, large repositories and resource pressure."),
    ("C09", "evidence and SLO", "Retain provenance, metrics and evidence sufficient to explain correctness and cost."),
    ("C10", "E2E entropy collapse", "Close the loop with E2E acceptance and deletion of superseded abstractions."),
)


@dataclass(frozen=True)
class Item:
    domain_index: int
    concern_index: int
    slug: str
    domain: str
    objective: str
    role: str
    concern_code: str
    concern: str
    deliverable: str

    @property
    def key(self) -> str:
        return f"D{self.domain_index:02d}-{self.concern_code}"

    @property
    def title(self) -> str:
        return f"[Liquid400/{self.key}] {self.domain}: {self.concern}"

    @property
    def branch(self) -> str:
        return f"liquid400/d{self.domain_index:02d}-c{self.concern_index:02d}-{self.slug}"

    @property
    def body(self) -> str:
        return (
            f"**Domain:** {self.domain}\n\n"
            f"**Objective:** {self.objective}\n\n"
            f"**Owner lane:** `{self.role}`\n\n"
            f"**Slice:** {self.deliverable}\n\n"
            "### Acceptance\n"
            "- Preserve Apache Airflow as the only lifecycle scheduling authority.\n"
            "- Bind work to a durable Factory Cell identity and positive epoch.\n"
            "- Fence or make idempotent every external mutation touched by this slice.\n"
            "- Make cancellation, stale writer, crash, retry and restart behavior explicit.\n"
            "- Keep backend, Rust CLI/TUI and evidence views contract-compatible where visible.\n"
            "- Retain evidence for success, refusal, repair, latency and cost.\n"
            "- Add tests or machine-checkable acceptance for the invariant.\n"
            "- Stabilization must delete superseded abstractions rather than stack duplicates.\n\n"
            "Architecture anchors: #65 and seven-worker contract #247."
        )


def items() -> tuple[Item, ...]:
    rows: list[Item] = []
    for di, (slug, domain, objective) in enumerate(DOMAINS, start=1):
        role = ROLES[(di - 1) % len(ROLES)]
        for ci, (code, concern, deliverable) in enumerate(CONCERNS, start=1):
            rows.append(Item(di, ci, slug, domain, objective, role, code, concern, deliverable))
    assert len(rows) == 400
    assert len({row.title for row in rows}) == 400
    assert len({row.branch for row in rows}) == 400
    return tuple(rows)


def run(*args: str, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=ROOT,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(args)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def gh_json(*args: str) -> Any:
    return json.loads(run("gh", *args).stdout or "null")


def graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables})
    value = json.loads(run("gh", "api", "graphql", "--input", "-", input_text=payload).stdout)
    if value.get("errors"):
        raise RuntimeError(json.dumps(value["errors"], indent=2))
    return value["data"]


def repo_id() -> str:
    owner, name = REPO.split("/", 1)
    data = graphql(
        "query($owner:String!,$name:String!){repository(owner:$owner,name:$name){id}}",
        {"owner": owner, "name": name},
    )
    return data["repository"]["id"]


def existing_issues() -> dict[str, dict[str, Any]]:
    rows = gh_json(
        "issue", "list", "--repo", REPO, "--state", "all", "--limit", "2000",
        "--json", "number,title,url,state"
    )
    return {row["title"]: row for row in rows if row and row.get("title")}


def create_batch(repository_id: str, batch: list[Item]) -> list[dict[str, Any]]:
    decl = ["$repo:ID!"]
    fields: list[str] = []
    variables: dict[str, Any] = {"repo": repository_id}
    for i, item in enumerate(batch):
        decl.extend((f"$t{i}:String!", f"$b{i}:String!"))
        fields.append(
            f"i{i}:createIssue(input:{{repositoryId:$repo,title:$t{i},body:$b{i}}})"
            "{issue{number title url state}}"
        )
        variables[f"t{i}"] = item.title
        variables[f"b{i}"] = item.body
    data = graphql(f"mutation({','.join(decl)}){{{' '.join(fields)}}}", variables)
    created: list[dict[str, Any]] = []
    for i in range(len(batch)):
        issue = (data.get(f"i{i}") or {}).get("issue")
        if issue:
            created.append(issue)
    return created


def ensure_issues(all_items: tuple[Item, ...]) -> dict[str, dict[str, Any]]:
    rows = existing_issues()
    repository_id = repo_id()
    stall = 0
    print(f"Liquid400 issues already present: {sum(i.title in rows for i in all_items)}/400", flush=True)
    while True:
        missing = [item for item in all_items if item.title not in rows]
        if not missing:
            return rows
        batch = missing[:CHUNK]
        before = sum(item.title in rows for item in all_items)
        try:
            for row in create_batch(repository_id, batch):
                rows[row["title"]] = row
        except RuntimeError as exc:
            print(f"issue batch deferred: {exc}", flush=True)
        rows.update(existing_issues())
        after = sum(item.title in rows for item in all_items)
        resolved = after - before
        if any(item.title not in rows for item in batch):
            stall += 1
            if stall > MAX_STALL_ROUNDS:
                raise RuntimeError(f"Liquid400 issue creation stalled at {after}/400")
            delay = min(60, stall * 5)
            print(f"Liquid400 partial batch: {after}/400; retry in {delay}s", flush=True)
            time.sleep(delay)
        else:
            stall = 0
            print(f"Liquid400 issues present: {after}/400", flush=True)
            time.sleep(8 if resolved else 2)


def branch_exists(branch: str) -> bool:
    return run("git", "ls-remote", "--exit-code", "--heads", "origin", branch, check=False).returncode == 0


def ensure_branch(item: Item, issue: dict[str, Any], base_sha: str) -> None:
    if branch_exists(item.branch):
        return
    run("git", "switch", "-C", item.branch, base_sha)
    path = ROOT / "contracts" / "liquid400" / f"{item.key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "key": item.key,
        "issue": {"number": issue["number"], "title": issue["title"], "url": issue["url"]},
        "branch": item.branch,
        "domain": item.domain,
        "objective": item.objective,
        "concern": item.concern,
        "owner_role": item.role,
        "scheduler": "airflow",
        "worker_limit": 7,
        "cell_epoch_required": True,
        "fan_in_rule": "stabilize, verify evidence, remove superseded abstractions",
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    run("git", "add", str(path.relative_to(ROOT)))
    run("git", "commit", "-m", f"liquid400: map {item.key} {item.concern}")
    run("git", "push", "origin", f"HEAD:refs/heads/{item.branch}")


def write_index(all_items: tuple[Item, ...], issues: dict[str, dict[str, Any]]) -> None:
    run("git", "switch", BASE)
    run("git", "fetch", "origin", BASE)
    run("git", "reset", "--hard", f"origin/{BASE}")
    payload = {
        "schema_version": 1,
        "issue_count": 400,
        "pr_ready_branch_count": 400,
        "scheduler": "airflow",
        "worker_limit": 7,
        "items": [
            {
                "key": item.key,
                "issue_number": issues[item.title]["number"],
                "issue_url": issues[item.title]["url"],
                "title": item.title,
                "branch": item.branch,
                "owner_role": item.role,
            }
            for item in all_items
        ],
    }
    path = ROOT / "contracts" / "liquid400" / "index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    run("git", "add", str(path.relative_to(ROOT)))
    if run("git", "diff", "--cached", "--quiet", check=False).returncode != 0:
        run("git", "commit", "-m", "liquid400: seal 400 issue-to-branch index")
        run("git", "push", "origin", f"HEAD:refs/heads/{BASE}")


def main() -> None:
    run("git", "config", "user.name", "swfactory-liquid400")
    run("git", "config", "user.email", "actions@users.noreply.github.com")
    run("git", "fetch", "origin", BASE)
    run("git", "switch", BASE)
    run("git", "reset", "--hard", f"origin/{BASE}")
    base_sha = run("git", "rev-parse", "HEAD").stdout.strip()
    all_items = items()
    issues = ensure_issues(all_items)
    for offset, item in enumerate(all_items, start=1):
        ensure_branch(item, issues[item.title], base_sha)
        if offset % 20 == 0:
            print(f"Liquid400 branches present: {offset}/400", flush=True)
    write_index(all_items, issues)
    print(json.dumps({"issues": 400, "pr_ready_branches": 400, "base": BASE}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
