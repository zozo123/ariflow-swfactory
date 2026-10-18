"""Bounded executable experiments inside Airflow's existing build task.

Four roles start from one immutable bundle in separate credential-free checkouts.
The controller freezes changes, invokes the pinned target checks, synthesizes,
retains counterexamples, and adopts only freshly verified bytes. No worker commits
or publishes. Native process-memory fork and remote checkpoint transport are not
claimed; unsupported sandbox/agent combinations fail before spending money.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from swfactory import stages
from swfactory.call_accounting import CallAttempt
from swfactory.campaign_decision import (
    CampaignDecisionManifest,
    DecisionCandidate,
    DecisionSelection,
    write_campaign_decision,
)
from swfactory.campaign_fusion import (
    FIXED_PROTECTED,
    CampaignFusionError,
    _changes,
    _git,
    _is_protected,
    build_fusion_proposal,
)
from swfactory.candidate_evidence import build_candidate_evidence_bundle
from swfactory.candidate_worktree import CandidateRevision, candidate_ref
from swfactory.canonical import json_digest
from swfactory.cells import CellIdentity
from swfactory.config import protected_for
from swfactory.models import BuildSummary, Plan, StageError, StageResult
from swfactory.sandbox import LocalSandbox, make_sandbox
from swfactory.source_snapshot import create_source_snapshot

ROLES = ("repair-a", "repair-b", "diagnostic", "challenger")
CONTROL = "replica-fusion.json"
REPORT = "replica-fusion-report.json"
CAMPAIGN_STAGE = "replica_fusion"  # the parent ledger's one campaign reservation, not a provider call


def worker_run_dirs(run_dir: Path) -> list[Path]:
    """Each fusion worker's own run directory, sorted. A worker is a separate ``Ctx`` with its own
    ``RunState``, so its provider calls sit in its own ledger, not the parent run's."""
    root = Path(run_dir) / "fusion" / "workers"
    return sorted(path for path in root.iterdir() if path.is_dir()) if root.is_dir() else []


class FusionNotes(BaseModel):
    model_config = ConfigDict(extra="forbid")
    observations: list[str] = Field(default_factory=list, max_length=8)


class FusionExperiment:
    def __init__(self, ctx: stages.Ctx) -> None:
        self.ctx = ctx
        self.spec = ctx.blueprint.fusion
        self.root = ctx.run_dir / "fusion"
        self.lock = threading.RLock()
        self.plan = Plan.model_validate_json(ctx.read_artifact(f"{ctx.art}/plan.json"))
        self.contract = stages._contract(ctx)
        # Rendered (or replayed) here, on the parent and before any fan-out thread exists, so no two
        # roles race to persist or mirror the card.
        self.dialect = stages.target_dialect(ctx)
        self.binding = ctx.state.read_control("cell.json") if ctx.state.has_control("cell.json") else None
        self.pin = json_digest(
            {
                "spec": self.spec.model_dump(),
                "plan": self.plan.model_dump(),
                "contract": self.contract.model_dump(),
                "cell": self.binding,
                "inputs": stages.accepted_inputs.digest_of(ctx.state),
            }
        )
        if ctx.state.has_control(CONTROL):
            self.progress = json.loads(ctx.state.read_control(CONTROL))
            if self.progress["pin"] != self.pin:
                raise StageError("policy", "fusion inputs changed; open a new epoch")
            if any(row["state"] == "in-flight" for row in self.progress["attempts"].values()):
                raise StageError("policy", "fusion attempt is in doubt; reconcile it before retrying")
        else:
            self.progress = {
                "schema_version": 1,
                "pin": self.pin,
                "base": stages._assert_workspace_head(ctx, "fusion"),
                "attempts": {},
                "rounds": [],
                "verifications": 0,
                "phase": "search",
            }
            self.save()
        self.root.mkdir(parents=True, exist_ok=True)
        self.pool = self.root / "pool"
        self.initial = self.progress["base"]
        if not self.pool.exists():
            bundle = self.root / "initial.bundle"
            _git(Path(ctx.sb.workdir), "bundle", "create", str(bundle.resolve()), self.initial, "HEAD")
            _git(self.root, "clone", "--quiet", "--no-hardlinks", str(bundle.resolve()), str(self.pool.resolve()))
        self.cap = min(ctx.cfg.max_budget_usd_per_stage, self.spec.max_cost_usd / (5 * self.spec.max_rounds))

    def save(self) -> None:
        with self.lock:
            self.ctx.state.write_control(CONTROL, json.dumps(self.progress, sort_keys=True) + "\n")

    def fence(self) -> None:
        if self.ctx.state.has_control("fusion-cancelled"):
            raise StageError("policy", "fusion cancelled; draining work without adoption")
        current = self.ctx.state.read_control("cell.json") if self.ctx.state.has_control("cell.json") else None
        if current != self.binding:
            raise StageError("policy", "fusion Cell epoch changed")
        if current is not None:
            cell = json.loads(current)
            if cell.get("managed"):
                from swfactory.cell_callback import post

                live = post("/cells/inspect", {"cell_id": cell["cell_id"]})
                if (
                    live.get("epoch") != cell["epoch"]
                    or live.get("policy_digest") != cell["policy_digest"]
                    or live.get("state") not in {"running", "queued"}
                ):
                    raise StageError("policy", "fusion no longer owns an active Cell epoch")
        stages._assert_workspace_head(self.ctx, "fusion fence")

    def workspace(self, identity: str, base: str) -> stages.Ctx:
        directory = self.root / "workers" / identity
        work = directory / "work"
        if work.exists():
            raise StageError("policy", "unexplained fusion workspace already exists")
        directory.mkdir(parents=True, exist_ok=True)
        _git(directory, "clone", "--quiet", "--no-hardlinks", str(self.pool.resolve()), str(work.resolve()))
        _git(work, "checkout", "--quiet", "--detach", base)
        _git(work, "remote", "remove", "origin")
        cfg = self.ctx.cfg.model_copy(
            update={
                "workdir": str(work),
                "run_id": "fusion-" + json_digest({"pin": self.pin, "identity": identity}).removeprefix("sha256:")[:24],
                "max_budget_usd": self.cap,
                "max_budget_usd_per_stage": self.cap,
                "record_dir": None,
            }
        )
        sb = make_sandbox(cfg, self.ctx.issue.id, protected=protected_for(self.contract, "build"), run_dir=directory)
        child = dataclasses.replace(
            self.ctx,
            cfg=cfg,
            sb=sb,
            run_dir=directory,
            contract=self.contract,
            stages=[],
            spent_usd=0.0,
            budget_seeded=False,
        )
        if self.binding is not None:
            child.state.write_control("cell.json", self.binding)
        stages._seed_exclude(child)
        stages._record_workspace_head(child, base)
        if sb.exists("pyproject.toml"):
            stages._sh(child, "uv sync --locked --group dev", timeout_s=self.spec.timeout_s)
        return child

    def verify(self, child: stages.Ctx, identity: str) -> dict[str, Any]:
        self.fence()
        with self.lock:
            if self.progress["verifications"] >= 4 * self.spec.max_rounds + 1:
                raise StageError("policy", "fusion verifier budget exhausted")
            self.progress["verifications"] += 1
            self.save()
        head = stages._assert_workspace_head(child, "fusion verification")
        lint = child.sb.run(self.contract.lint, timeout_s=self.spec.timeout_s) if self.contract.lint else None
        sandbox = child.sb
        timeout = self.spec.timeout_s

        class VerificationSandbox:
            def __getattr__(self, name):
                return getattr(sandbox, name)

            def run(self, command, **kwargs):
                return sandbox.run(command, **{**kwargs, "timeout_s": min(timeout, kwargs.get("timeout_s", timeout))})

        tests, failures = stages.run_tests(dataclasses.replace(child, sb=VerificationSandbox()))
        tree = _git(Path(child.sb.workdir), "rev-parse", "HEAD^{tree}").decode().strip()
        recipe = json_digest(
            {"contract": self.contract.model_dump(), "sandbox": self.ctx.cfg.sandbox, "timeout_s": self.spec.timeout_s}
        )
        receipt = {
            "identity": identity,
            "head": head,
            "tree": tree,
            "recipe_digest": recipe,
            "cell": self.binding,
            "passed": tests.ok and (lint is None or lint.ok),
            "tests": tests.model_dump(),
            "failures": failures[-8000:],
            "lint_exit_code": lint.exit_code if lint is not None else None,
            "lint_failures": ((lint.stdout + "\n" + lint.stderr)[-2000:] if lint is not None and not lint.ok else ""),
        }
        self.ctx.state.write_control(f"fusion-verification/{identity}.json", json.dumps(receipt, sort_keys=True) + "\n")
        return receipt

    def scope(self, child: stages.Ctx, base: str, head: str, stage: str = "build") -> None:
        if head == base:
            return
        for path, _, _ in _changes(Path(child.sb.workdir), base, head):
            if path not in self.plan.files or _is_protected(
                path, (*FIXED_PROTECTED, *protected_for(self.contract, stage))
            ):
                raise StageError("policy", f"fusion candidate escaped approved file scope: {path}")

    def retain(self, child: stages.Ctx, identity: str, head: str) -> None:
        # Concurrent fetches otherwise contend for FETCH_HEAD. Retain every
        # verified continuation under a create-only ref, including failed checks,
        # so GC cannot erase the evidence needed by a second round or retry.
        with self.lock:
            _git(
                self.pool,
                "fetch",
                "--quiet",
                "--no-tags",
                "--no-write-fetch-head",
                str(Path(child.sb.workdir).resolve()),
                head,
            )
            _git(self.pool, "update-ref", candidate_ref(identity), head, "0" * 40)

    def attempt(
        self, round_number: int, role: str, base: str, failures: str, evidence: str, starting_patch: bytes | None = None
    ) -> dict[str, Any]:
        identity = f"round-{round_number}-{role}"
        with self.lock:
            previous = self.progress["attempts"].get(identity)
            if previous is not None:
                return previous
            self.fence()
            self.progress["attempts"][identity] = {"state": "in-flight", "identity": identity}
            self.save()
        child = None
        row = {"identity": identity, "role": role, "base": base, "state": "failed", "cost_usd": 0.0}
        started = time.monotonic()
        try:
            child = self.workspace(identity, base)
            if starting_patch:
                child.sb.write(".factory/fusion.patch.b64", base64.b64encode(starting_patch).decode())
                stages._sh(child, "base64 -d < .factory/fusion.patch.b64 | git apply --index")
            stage = (
                ("build" if round_number == 1 else "fix") if role in {"repair-a", "repair-b", "synthesis"} else "review"
            )
            policy = dataclasses.replace(
                stages._policy(child, stage),
                timeout_s=self.spec.timeout_s,
                model=self.spec.models.get(role, stages._policy(child, stage).model),
            )
            prompt = stages.render_prompt(
                "fusion",
                role=role,
                spec=stages._read_or(self.ctx, f"{self.ctx.art}/spec.md"),
                plan=self.ctx.read_artifact(f"{self.ctx.art}/plan.md"),
                failures=failures,
                evidence=evidence,
                protected=stages._protected(child, stage),
                dialect=self.dialect,
            )
            # _agent owns accounting and guard installation; a role-specific policy
            # is projected through a copy, never by mutating the shared agent policy.
            child.blueprint = self.ctx.blueprint.model_copy(update={"policy": {**self.ctx.blueprint.policy}})
            original = child.agent

            class BoundAgent:
                kind = original.kind

                def run(self, sb, **kwargs):
                    return original.run(sb, **{**kwargs, "policy": policy})

            child.agent = BoundAgent()
            self.fence()  # Dependency preparation may have outlived a cancellation or epoch change.
            result = stages._agent(
                child,
                stage,
                round_number * 10 + ROLES.index(role) + 1 if role in ROLES else round_number * 10 + 5,
                prompt,
                BuildSummary if policy.writes else FusionNotes,
            )
            if policy.writes:
                head = stages.commit(child, stage="fusion", msg=f"fusion {identity}")
                self.scope(child, base, head, stage)
                row["head"] = head
                row["verification"] = self.verify(child, identity)
                self.retain(child, identity, head)
            else:
                stages._ensure_clean(child, "fusion diagnostic", allow_artifacts=True)
                stages._assert_workspace_head(child, "fusion diagnostic")
                row["observations"] = FusionNotes.model_validate(result.data).observations
                if any(len(value) > 4096 for value in row["observations"]):
                    raise StageError("policy", "fusion diagnostic observation exceeds limit")
            row["state"] = "answered"
        except StageError as error:
            row["error"] = str(error)[:2000]
        finally:
            if child is not None:
                ledger = stages.CallLedger(child.state)
                row["cost_usd"] = ledger.charged_usd()
                row["usage_unknown"] = bool(ledger.unreconciled())
                child.sb.close()
                shutil.rmtree(child.sb.workdir)
            with self.lock:
                row["duration_s"] = time.monotonic() - started
                self.progress["attempts"][identity] = row
                self.save()
        return row

    def compose(self, number: int, base: str, rows: list[dict]) -> tuple[bytes | None, str]:
        repairs = [row for row in rows if row.get("head") and row["head"] != base and row["state"] == "answered"]
        if len(repairs) < 2:
            return None, "insufficient retained code candidates"
        directory = self.root / f"round-{number}"
        directory.mkdir(exist_ok=True)
        source = create_source_snapshot(self.pool, base, self.root / "sources")
        candidates, paths = [], {}
        for row in repairs:
            identity, head = row["identity"], row["head"]
            ref = candidate_ref(identity)
            try:
                observed = _git(self.pool, "rev-parse", "--verify", ref).decode().strip()
            except CampaignFusionError:
                _git(self.pool, "update-ref", ref, head, "0" * 40)
            else:
                if observed != head:
                    raise StageError("policy", "retained fusion contributor ref changed")
            paths[identity] = directory / identity
            if paths[identity].exists():
                from swfactory.candidate_evidence import verify_candidate_evidence_bundle

                bundle = verify_candidate_evidence_bundle(paths[identity], repo=self.pool)
            else:
                bundle = build_candidate_evidence_bundle(
                    self.pool,
                    CandidateRevision(identity, base, head, ref),
                    source,
                    artifacts={},
                    destination=paths[identity],
                )
            candidates.append(
                DecisionCandidate(
                    identity, row["role"], "ok", base, head, ref, bundle.digest(), (), float(row["cost_usd"]), 0.0
                )
            )
        ids = tuple(sorted(paths))
        binding = json.loads(self.binding) if self.binding is not None else {}
        cell_id = (
            binding.get("cell_id")
            or CellIdentity(self.ctx.cfg.repo, self.ctx.cfg.target_dir, self.ctx.issue.id).stable_id()
        )
        decision = CampaignDecisionManifest(
            f"fusion-{number}",
            cell_id,
            int(binding.get("epoch", 0)),
            base,
            tuple(candidates),
            DecisionSelection(None, "composition inputs", ids, ()),
            (),
            False,
        )
        decision_path = directory / "decision.json"
        write_campaign_decision(decision_path, decision)
        observations = []
        for row in rows:
            for statement in row.get("observations", []):
                observations.append(
                    {
                        "evidence_id": json_digest({"statement": statement, "subject": base}),
                        "producer_id": row["identity"],
                        "kind": "hypothesis",
                        "statement": statement,
                        "subject_head": base,
                    }
                )
        observation_path = directory / "observations.json"
        observation_path.write_text(
            json.dumps({"campaign_decision_digest": decision.digest(), "observations": observations})
        )
        try:
            if (directory / "proposal").exists():
                from swfactory.campaign_fusion import verify_fusion_proposal

                verify_fusion_proposal(
                    self.pool, decision_path, paths, directory / "proposal", observations_path=observation_path
                )
            else:
                build_fusion_proposal(
                    self.pool, decision_path, paths, ids, directory / "proposal", observations_path=observation_path
                )
        except CampaignFusionError as error:
            return None, str(error)
        return (directory / "proposal" / "candidate.diff").read_bytes(), "disjoint composition"

    def mechanical(self, number: int, base: str, patch: bytes) -> dict:
        identity = f"round-{number}-composition"
        previous = self.progress["attempts"].get(identity)
        if previous is not None:
            return previous
        self.progress["attempts"][identity] = {"identity": identity, "state": "in-flight"}
        self.save()
        child = self.workspace(identity, base)
        try:
            child.sb.write(".factory/fusion.patch.b64", base64.b64encode(patch).decode())
            stages._sh(child, "base64 -d < .factory/fusion.patch.b64 | git apply --index")
            head = stages.commit(child, stage="fusion", msg=f"fusion {identity}")
            self.scope(child, base, head, "build" if number == 1 else "fix")
            row = {
                "identity": identity,
                "role": "composition",
                "head": head,
                "base": base,
                "state": "answered",
                "cost_usd": 0.0,
                "verification": self.verify(child, identity),
            }
            self.retain(child, identity, head)
            self.progress["attempts"][identity] = row
            self.save()
            return row
        finally:
            child.sb.close()
            shutil.rmtree(child.sb.workdir)

    def search(self) -> dict:
        base, failures = self.initial, ""
        for number in range(1, self.spec.max_rounds + 1):
            self.fence()
            with ThreadPoolExecutor(max_workers=self.spec.max_parallel) as executor:
                futures = [executor.submit(self.attempt, number, role, base, failures, "") for role in ROLES]
                rows = [future.result() for future in futures]
            if any(row.get("usage_unknown") for row in rows):
                raise StageError("policy", "fusion provider usage is unknown; reconcile before continuing")
            patch, reason = self.compose(number, base, rows)
            if patch is not None:
                rows.append(self.mechanical(number, base, patch))
            passing = [row for row in rows if row.get("verification", {}).get("passed")]
            if not passing or not any(row["role"] == "composition" for row in passing):
                context = []
                for row in rows:
                    context.append(json.dumps(row, sort_keys=True))
                    if row.get("head"):
                        diff = _git(self.pool, "diff", "--no-ext-diff", "--no-textconv", "--binary", base, row["head"])
                        if len(diff) > 128 * 1024:
                            raise StageError("policy", "fusion synthesis context exceeds patch limit")
                        context.append(diff.decode("utf-8"))
                rows.append(self.attempt(number, "synthesis", base, failures, "\n".join(context), patch))
            passing = [row for row in rows if row.get("verification", {}).get("passed")]
            round_receipt = {
                "number": number,
                "base": base,
                "composition": reason,
                "attempts": [row["identity"] for row in rows],
            }
            self.progress["rounds"] = [row for row in self.progress["rounds"] if row["number"] != number] + [
                round_receipt
            ]
            self.save()
            if passing:
                # A failing fusion cannot displace an independently passing fallback.
                winner = min(
                    passing,
                    key=lambda row: (
                        0 if row["role"] in {"composition", "synthesis"} else 1,
                        row["cost_usd"],
                        row["identity"],
                    ),
                )
                individuals = [row for row in rows if row["role"] in {"repair-a", "repair-b"}]
                winner["fusion_only"] = (
                    winner["role"] in {"composition", "synthesis"}
                    and len(individuals) == 2
                    and all(row.get("verification", {}).get("passed") is False for row in individuals)
                )
                return winner
            attempted = [row for row in rows if row.get("verification")]
            if not attempted:
                raise StageError("policy", "fusion produced no verifiable candidate")
            continuation = max(attempted, key=lambda row: (row["verification"]["tests"]["passed"], row["identity"]))
            base = continuation["head"]
            failures = json.dumps([row["verification"] for row in attempted], sort_keys=True)
        raise StageError("policy", "fusion exhausted its bounded rounds without passing product checks")

    def adopt(self, winner: dict) -> StageResult:
        self.fence()
        before = stages._assert_workspace_head(self.ctx, "fusion adoption")
        expected_tree = winner["verification"]["tree"]
        if before == self.initial:
            patch = _git(
                self.pool,
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--binary",
                "--full-index",
                self.initial,
                winner["head"],
            )
            self.progress.update(
                {"phase": "adopting", "winner": winner, "patch_digest": json_digest(base64.b64encode(patch).decode())}
            )
            self.save()
            self.ctx.sb.write(".factory/fusion.patch.b64", base64.b64encode(patch).decode())
            stages._sh(self.ctx, "base64 -d < .factory/fusion.patch.b64 | git apply --index")
            stages.commit(self.ctx, stage="fusion", msg="adopt verified replica fusion")
        actual_tree = stages._sh(self.ctx, "git rev-parse HEAD^{tree}").strip()
        if actual_tree != expected_tree:
            raise StageError("policy", "adopted fusion bytes differ from verified candidate")
        if self.progress["phase"] != "complete":
            self.progress["final_verification"] = self.verify(self.ctx, "adopted")
            if not self.progress["final_verification"]["passed"]:
                raise StageError("policy", "adopted fusion failed final product verification")
            self.progress["phase"] = "complete"
            self.save()
        report = {
            **self.progress,
            "authority": "search-only",
            "runtime": "isolated-checkouts",
            "total_cost_usd": sum(row.get("cost_usd", 0.0) for row in self.progress["attempts"].values()),
        }
        individual_ids = [
            identity
            for identity in self.progress["rounds"][-1]["attempts"]
            if self.progress["attempts"][identity]["role"] in {"repair-a", "repair-b"}
        ]
        individuals = [self.progress["attempts"][identity] for identity in individual_ids]
        verdicts = [row.get("verification", {}).get("passed") for row in individuals]
        report["comparison"] = {
            "scope": "fixed retained pool; not a matched-budget model-performance estimate",
            "single_candidate": {"candidate_id": individual_ids[0], "passed": verdicts[0]},
            "best_of_candidates": {
                "candidate_ids": individual_ids,
                "passed": True
                if any(value is True for value in verdicts)
                else (False if all(value is False for value in verdicts) else None),
            },
            "selected_candidate": {
                "candidate_id": winner["identity"],
                "passed": True,
                "fusion_only": winner["fusion_only"],
            },
        }
        self.ctx.write_artifact(
            f"{self.ctx.art}/{REPORT}", json.dumps({**report, "report_digest": json_digest(report)}, indent=2) + "\n"
        )
        return StageResult(
            stage="build_and_test",
            artifacts=[f"{self.ctx.art}/{REPORT}"],
            numbers={
                "fusion_rounds": float(len(self.progress["rounds"])),
                "fusion_only": float(winner["fusion_only"]),
                "fusion_verifications": float(self.progress["verifications"]),
                **stages._test_numbers(stages.TestResult.model_validate(self.progress["final_verification"]["tests"])),
            },
        )


def execute_fusion_stage(ctx: stages.Ctx) -> StageResult:
    """Reserve the whole campaign before fan-out; replay completed work, refuse in-doubt work."""
    if not isinstance(ctx.sb, LocalSandbox) or ctx.cfg.sandbox not in {"local", "srt", "docker"}:
        raise StageError("policy", "fusion requires a qualified host checkpoint sandbox (local replay, srt or docker)")
    if ctx.agent.kind not in {"scripted", "claude"} or (ctx.cfg.sandbox == "local" and ctx.agent.kind != "scripted"):
        raise StageError("policy", "fusion supports scripted replay or confined native Claude execution")
    stages._ensure_clean(ctx, "fusion start", allow_artifacts=True)
    experiment = FusionExperiment(ctx)
    experiment.fence()
    ledger = stages.CallLedger(ctx.state)
    budget_file = "replica-fusion-budget.json"
    if not ctx.state.has_control(budget_file):
        stages.seed_budget(ctx, refresh=True)
        if experiment.spec.max_cost_usd > ctx.cfg.max_budget_usd - ctx.spent_usd:
            raise StageError("policy", "fusion reservation exceeds remaining job budget")
        attempt = ledger.reserve(stage=CAMPAIGN_STAGE, iteration=1, reserved_usd=experiment.spec.max_cost_usd)
        ctx.state.write_control(budget_file, json.dumps(dataclasses.asdict(attempt)) + "\n")
    else:
        attempt = CallAttempt(**json.loads(ctx.state.read_control(budget_file)))
    try:
        winner = experiment.progress.get("winner") or experiment.search()
        costs = sum(row.get("cost_usd", 0.0) for row in experiment.progress["attempts"].values())
        if (
            any(row.get("usage_unknown") for row in experiment.progress["attempts"].values())
            or costs > experiment.spec.max_cost_usd
        ):
            raise StageError("policy", "fusion usage is unknown or exceeds its reservation")
        if ledger.record(attempt.call_id).settled_usd is None:
            ledger.settle(attempt, cost_usd=costs, receipt={"fusion_pin": experiment.pin})
        stages.seed_budget(ctx, refresh=True)
        return experiment.adopt(winner)
    except BaseException as error:
        if ledger.record(attempt.call_id).settled_usd is None:
            rows = experiment.progress["attempts"].values()
            if any(row["state"] == "in-flight" or row.get("usage_unknown") for row in rows):
                ledger.mark_unknown(attempt, error)
            else:
                ledger.settle(
                    attempt,
                    cost_usd=sum(row.get("cost_usd", 0.0) for row in rows),
                    receipt={"fusion_pin": experiment.pin, "outcome": "failed-with-known-usage"},
                )
        raise
