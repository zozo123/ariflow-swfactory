"""Factory control-plane service after liquid-development fan-in.

There is one authority chain:

    Airflow schedules lifecycle -> Factory Cell fences identity/epoch -> backend owns external
    mutations/admission/evidence -> Rust renders the backend contract.

The service is deliberately storage/transport focused. It does not schedule work outside Airflow and
it does not infer provider behavior from names.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

from swfactory import blueprint
from swfactory.admission import Priority
from swfactory.cell_runtime import identity_for_job
from swfactory.cells import (
    SCHEMA_VERSION,
    TERMINAL_STATES,
    CellBusy,
    CellStore,
    DuplicateOperation,
    StaleEpoch,
)
from swfactory.control import AirflowClient, ControlError, GitHubClient, IsloClient, MetricsSource
from swfactory.control_kernel import ControlKernel
from swfactory.deployment_profile import assert_supported_state_root
from swfactory.doctor import _check_managed_workers
from swfactory.durable_admission import (
    MAX_DISPATCH_ATTEMPTS,
    WORK_ORDER_SCHEMA,
    DispatchIntent,
    DispatchLeaseLost,
    Member,
    MemberSpec,
    WorkOrderConflict,
    request_digest,
)
from swfactory.idempotency import MutationOutcome, OperationRef, RetryBudget
from swfactory.inspection import inspect_run, list_runs
from swfactory.lifecycle_evidence import TraceContext
from swfactory.product_surface import build_preview, capability_document
from swfactory.restore_contract import GATE_PENDING as RESTORE_PENDING
from swfactory.restore_contract import RestoreGate
from swfactory.security_contract import MutationEnvelope, policy_digest_for_mapping
from swfactory.store_schema import StoreSchemaError, assert_compatible
from swfactory.trust_evidence import TrustedEvidence
from swfactory.webhook import _NoRedirect, _safe_airflow_base

MAX_RESPONSE = 16 * 1024 * 1024
# A Factory Cell that is live at its recorded epoch: the states an earlier dispatch attempt can
# legitimately have left behind and this one may adopt.
LIVE_CELL_STATES = frozenset({"dispatching", "queued", "running"})
LINE_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}\Z")
SEG = r"[^/?#]+"
READ_ROUTES = re.compile(
    rf"/(?:monitor/health|dags|dags/{SEG}|dags/{SEG}/dagRuns"
    rf"|dags/{SEG}/dagRuns/{SEG}|dags/{SEG}/dagRuns/{SEG}/hitlDetails"
    rf"|dags/{SEG}/dagRuns/{SEG}/taskInstances"
    rf"|dags/{SEG}/dagRuns/{SEG}/taskInstances/{SEG}/{SEG}/hitlDetails"
    rf"|dags/{SEG}/dagRuns/{SEG}/taskInstances/{SEG}/xcomEntries/return_value"
    rf"|dags/{SEG}/dagRuns/{SEG}/taskInstances/{SEG}/logs/[0-9]+)\Z"
)


class Refused(ValueError):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status


def text(body: dict[str, Any], key: str, *, max_len: int = 512) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value.strip() or len(value) > max_len:
        raise ValueError(f"{key} must be a nonempty string of at most {max_len} characters")
    return value.strip()


def _cell_actor(actor: str, work_id: str) -> str:
    """The activation actor names the work order, so a Cell's history proves whose activation it is."""
    return f"backend:{actor}:{work_id}"


def _work_id(request: str, desired_epochs: dict[str, int]) -> str:
    """Work identity = request identity x epoch identity.

    Keeping the desired Cell epochs in the *work* id and out of ``request_digest`` is what lets an
    operator re-run the same request after a Cell reached a terminal state: that is a new work order,
    not a duplicate of the finished one.
    """
    payload = json.dumps(
        {"request": request, "epochs": desired_epochs},
        sort_keys=True,
        separators=(",", ":"),
    )
    return "submit_" + hashlib.sha256(payload.encode()).hexdigest()[:32]


def _priority(value: Any) -> Priority:
    if value is None:
        return Priority.NORMAL
    if not isinstance(value, str):
        raise ValueError("priority must be hotfix, manual, normal or background")
    try:
        return Priority[value.strip().upper()]
    except KeyError as error:
        raise ValueError("priority must be hotfix, manual, normal or background") from error


class Factory:
    def __init__(
        self,
        *,
        token: str,
        airflow_url: str,
        repo: str = "",
        owner: str = "",
        root: Path = Path("."),
        state_root: Path = Path(".factory"),
    ):
        if len(token) < 32 or any(c.isspace() for c in token):
            raise ValueError("SWF_BACKEND_TOKEN must contain at least 32 non-whitespace characters")
        self.token = token
        self.airflow_url = _safe_airflow_base(airflow_url)
        self.repo = repo
        self.owner = owner
        self.root = root.resolve()
        self.state_root = state_root.resolve()
        self.state_root.mkdir(parents=True, exist_ok=True)
        # Both refusals happen before a single store is opened for writing: a state root this
        # binary must not own (newer schema, partial restore), and a state root whose filesystem
        # cannot fence a second writer. Discovering either after the first mutation is too late.
        assert_supported_state_root(self.state_root)
        assert_compatible(self.state_root)
        self.restore_gate = RestoreGate(self.state_root)
        self.cell_store = CellStore(self.state_root / "cells.sqlite3")
        self.control = ControlKernel(self.state_root / "control", restore_gate=self.restore_gate)
        self.evidence = TrustedEvidence(self.state_root / "evidence")
        self.opener = urllib.request.build_opener(_NoRedirect)
        self.credentials = AirflowClient(
            self.airflow_url,
            token=os.getenv("AIRFLOW_TOKEN") or None,
            username=os.getenv("AIRFLOW_USER"),
            password=os.getenv("AIRFLOW_PASSWORD"),
            opener=self.opener.open,
        )
        self.auth_lock = threading.Lock()

    def close(self) -> None:
        self.cell_store.close()
        self.control.close()

    def _line(self, name: str) -> blueprint.Blueprint:
        if not LINE_NAME.fullmatch(name):
            raise ValueError("line must be an installed blueprint name")
        return blueprint.load(name)

    def _credential(self, *, refresh: bool = False) -> str | None:
        with self.auth_lock:
            if refresh and not os.getenv("AIRFLOW_TOKEN"):
                self.credentials._token = None
            return self.credentials.token()

    def airflow(self, method: str, path: str, body: dict | None) -> tuple[int, Any]:
        url = self.airflow_url + "/api/v2" + path
        for attempt in range(2):
            token = self._credential(refresh=attempt > 0)
            headers = {"Accept": "application/json"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            data = None if body is None else json.dumps(body, allow_nan=False).encode()
            if data is not None:
                headers["Content-Type"] = "application/json"
            request = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                response = self.opener.open(request, timeout=15)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                status = response.code
                raw = response.read(MAX_RESPONSE + 1)
            if status == 401 and attempt == 0 and not os.getenv("AIRFLOW_TOKEN"):
                continue
            if len(raw) > MAX_RESPONSE:
                raise Refused(502, "Airflow response exceeds backend limit")
            try:
                payload = json.loads(raw) if raw else None
            except ValueError:
                raise Refused(502, "Airflow returned invalid JSON") from None
            return status, payload
        raise Refused(401, "Airflow authentication failed")

    def _checked_airflow(self, method: str, path: str, body: dict | None = None) -> Any:
        status, payload = self.airflow(method, path, body)
        if status >= 300:
            suffix = "; mutation outcome may be unknown" if method != "GET" and status >= 500 else ""
            raise Refused(status, f"Airflow rejected {method} (HTTP {status}){suffix}")
        return payload

    def _policy_digest(self, line_name: str, job: dict[str, Any]) -> str:
        return policy_digest_for_mapping(
            {
                "line": line_name,
                "repo": str(job["repo"]),
                "issue": str(job["issue"]),
                "target_dir": str(job.get("dir", "")),
                "base_branch": str(job.get("base_branch", "main")),
                "sandbox": str(job.get("sandbox", "configured")),
            }
        )

    def _desired_epoch(self, job: dict[str, Any]) -> int:
        cell_id = identity_for_job(job).stable_id()
        try:
            cell = self.cell_store.get(cell_id)
        except KeyError:
            return 1
        epoch = int(cell["epoch"])
        return epoch + 1 if cell["state"] in TERMINAL_STATES else epoch

    def _journal_airflow_unpause(self, authority: dict[str, Any], line_name: str, path: str) -> None:
        ref = OperationRef.build(authority["cell_id"], authority["epoch"], "airflow_unpause", line_name)

        def apply() -> dict[str, Any]:
            payload = self._checked_airflow("PATCH", path, {"is_paused": False})
            return payload if isinstance(payload, dict) else {"is_paused": False}

        def reconcile() -> MutationOutcome:
            status, payload = self.airflow("GET", path, None)
            if status == 200 and isinstance(payload, dict):
                if payload.get("is_paused") is False:
                    return MutationOutcome("committed", payload, {"dag": line_name}, "DAG is unpaused")
                return MutationOutcome("definitely_absent", None, {"dag": line_name}, "DAG remains paused")
            return MutationOutcome("ambiguous", None, {"dag": line_name, "status": status}, "DAG state unavailable")

        self.control.mutate(ref, apply, replay_safe=True, reconcile=reconcile)

    def _work_order(
        self,
        line: blueprint.Blueprint,
        jobs: list[dict[str, Any]],
        conf: dict[str, Any],
        actor: str,
        priority: Priority,
    ) -> dict[str, Any]:
        """Build the versioned, immutable payload one admitted command is re-delivered from.

        It carries everything a restarted backend needs to finish the work without re-reading the
        blueprint from disk: source identity, the selected jobs, the resolved policy and blueprint
        identity, and the deterministic Airflow run id. ``request_digest`` covers the request alone,
        so one request stays recognisable across attempts; the desired Cell epochs are retry/epoch
        identity and are deliberately kept out of it.
        """
        request = {
            "schema_version": WORK_ORDER_SCHEMA,
            "line": line.name,
            "actor": actor,
            "priority": priority.name.lower(),
            "issues": list(conf["issues"]),
            "targets": list(conf.get("targets", [])),
            "blueprint": {
                "name": line.name,
                "version": int(line.version),
                "identity": policy_digest_for_mapping(
                    {
                        "name": line.name,
                        "version": int(line.version),
                        "stages": list(line.order),
                        "targets": [{"repo": t.repo, "dir": t.dir, "base_branch": t.base_branch} for t in line.targets],
                    }
                ),
            },
            "jobs": [
                {
                    "job_idx": int(job["job_idx"]),
                    "repo": str(job["repo"]),
                    "issue": str(job["issue"]),
                    "dir": str(job.get("dir", "")),
                    "base_branch": str(job.get("base_branch", "main")),
                    "policy_digest": self._policy_digest(line.name, job),
                }
                for job in jobs
            ],
        }
        digest = request_digest(request)
        desired = {str(int(job["job_idx"])): self._desired_epoch(job) for job in jobs}
        return {
            **request,
            "request_digest": digest,
            "desired_epochs": desired,
            "conf": conf,
            "generation": os.getenv("SWF_GENERATION") or "stable",
            "dag_run_id": "swf__" + _work_id(digest, desired).removeprefix("submit_"),
        }

    def submit(self, body: dict[str, Any]) -> dict[str, Any]:
        # Admission readiness belongs to the shared use case, not one HTTP route. Every caller --
        # canonical work-orders, the Airflow compatibility mount, and future internal callers --
        # must cross this guard before a reservation, Cell activation, or remote mutation exists.
        if not self.capabilities()["mutation_ready"]:
            raise Refused(503, "backend is draining or not mutation-ready")
        line = self._line(text(body, "line"))
        issues = body.get("issues")
        if not isinstance(issues, list) or not issues or len(issues) > 1000:
            raise ValueError("issues must contain between 1 and 1000 references")
        if any(not isinstance(i, str) or not i.strip() or len(i) > 128 for i in issues):
            raise ValueError("issue references must be nonempty strings of at most 128 characters")
        issues = [i.strip() for i in issues]
        if len(set(issues)) != len(issues):
            raise ValueError("duplicate issue references are not allowed")
        targets = body.get("targets", [])
        if not isinstance(targets, list) or any(not isinstance(t, str) for t in targets):
            raise ValueError("targets must be an array of repository names")
        actor = str(body.get("actor") or "operator").strip()
        if not actor or len(actor) > 128:
            raise ValueError("actor must be a nonempty string of at most 128 characters")

        conf: dict[str, Any] = {"issues": issues, **({"targets": targets} if targets else {})}
        jobs = list(line.jobs(conf))
        priority = _priority(body.get("priority"))
        order = self._work_order(line, jobs, conf, actor, priority)
        submission_id = _work_id(order["request_digest"], order["desired_epochs"])
        # One capacity unit per Factory Cell the order will activate, declared before anything is
        # activated, so every affected repository is counted and no sibling can be released early.
        members = [MemberSpec(int(job["job_idx"]), str(job["repo"]), identity_for_job(job).stable_id()) for job in jobs]
        try:
            decision = self.control.submit(
                work_id=submission_id,
                actor=actor,
                blueprint=line.name,
                order=order,
                members=members,
                priority=priority,
            )
        except WorkOrderConflict as error:
            raise Refused(409, str(error)) from error
        if decision.reason == "duplicate_terminal":
            # The Cells are not terminal (their epochs are part of the work id), so this order was
            # closed by a cancellation or a compensated activation. Saying so beats silently
            # answering with a run that will never exist.
            raise Refused(409, f"work order already finished as {decision.state}")
        if decision.state in {"queued", "rejected"}:
            # A queued order is durable down to its payload and its place in the queue, so a restart
            # before capacity frees up loses nothing. Pump the outbox on the way out: a backend that
            # just came back may be holding commands nobody has asked about since.
            self.resume_dispatch()
            return {
                "state": decision.state,
                "submission_id": submission_id,
                "reason": decision.reason,
                "position": decision.position,
                "limiting": decision.limiting,
                "issues": issues,
                "jobs": len(jobs),
                "blueprint": {"name": line.name, "resolved": True},
            }
        if decision.intent is not None:
            # This submission's own delivery, claimed inside the admission transaction, so no
            # concurrent request can be halfway through it while this one reports the result.
            self._dispatch_intent(decision.intent)
        else:
            self._deliver(submission_id)
        self.resume_dispatch()
        return self._work_document(submission_id)

    def resume_dispatch(self, *, limit: int = 32, rounds: int = 8) -> list[dict[str, Any]]:
        """Re-deliver admitted commands whose Airflow run does not exist yet.

        This is redelivery of one already-admitted command, never a second scheduler: it decides
        nothing about when a stage runs, it only finishes handing Airflow the run it was already
        promised. It is pumped by the request paths that create or release capacity, so a restart in
        the middle of a dispatch is repaired by the next submission, lifecycle transition or an
        explicit operator resume. A failure is recorded on the dispatch intent rather than raised:
        the caller is usually a lifecycle transition that must not fail because a *different* work
        order's Airflow call did.
        """
        self._reconcile_held_units()
        resumed: list[dict[str, Any]] = []
        seen: set[str] = set()
        for _ in range(max(1, rounds)):
            batch = [work_id for work_id in self.control.pending_dispatch(limit=limit) if work_id not in seen]
            if not batch:
                break
            for work_id in batch:
                seen.add(work_id)
                intent = self.control.claim_dispatch(work_id)
                if intent is None:
                    continue
                try:
                    run_id = self._dispatch_intent(intent)
                except Exception as error:  # noqa: BLE001 - recorded on the durable dispatch intent
                    resumed.append({"work_id": work_id, "dispatched": False, "detail": str(error)[:512]})
                else:
                    resumed.append({"work_id": work_id, "dispatched": True, "run_id": run_id})
        return resumed

    def _reconcile_held_units(self, *, limit: int = 200) -> list[str]:
        """Release units held against Factory Cells that are already finished.

        A unit is normally released by the Cell's own terminal transition.  That report can be
        missing for good reasons -- a compensation ended the Cell directly, a process died between
        cancelling a Cell and closing its reservation, an operator cleaned it up outside the
        backend -- and when it is, the reservation holds a unit that nothing will ever ask for
        back, which blocks the queue behind it forever.

        This is reconciliation, not scheduling: it reads durable admission state, compares it with
        Cell truth, and decides nothing about when any stage runs.  It never raises, because its
        caller is usually a lifecycle transition that must not fail over somebody else's bookkeeping.
        """
        repaired: list[str] = []
        try:
            memberships = self.control.admission.held_memberships(limit=limit)
        except Exception:  # noqa: BLE001 - reconciliation must never break its caller
            return repaired
        for work_id, cell_id, epoch in memberships:
            try:
                cell = self.cell_store.get(cell_id)
            except KeyError:
                # The Cell is gone; the activation it stood for certainly is not running.
                outcome = "cancelled"
            else:
                if int(cell["epoch"]) > epoch:
                    # A later epoch exists, so this one ended -- Cells only re-arm from a terminal
                    # state -- even though how it ended was never reported here.
                    outcome = "cancelled"
                elif int(cell["epoch"]) == epoch and str(cell["state"]) in TERMINAL_STATES:
                    outcome = str(cell["state"])
                else:
                    continue
            try:
                self.control.release_for_terminal_cell(work_id, cell_id=cell_id, epoch=epoch, state=outcome)
            except Exception:  # noqa: BLE001 - one unrepairable row must not stop the rest
                continue
            repaired.append(work_id)
        return repaired

    def _deliver(self, work_id: str) -> str | None:
        """Deliver this one admitted command now, letting its failure reach the submitter."""
        intent = self.control.claim_dispatch(work_id)
        if intent is None:
            return None
        return self._dispatch_intent(intent)

    def _dispatch_intent(self, intent: DispatchIntent) -> str:
        """Hand one admitted command to Airflow and bind its Cells, resuming from durable state.

        Every step is replay-safe: an activation is adopted only when the Cell's own history proves
        this work order made it, the Airflow POST goes through the operation journal (which observes
        an ambiguous outcome before repeating it), and the reservation is marked delivered only once
        the run exists and every member Cell is bound to it. A crash at any boundary therefore
        resumes into the same deterministic dispatch instead of starting a different one.
        """
        order = intent.order.payload
        line_name = str(order["line"])
        actor = str(order["actor"])
        jobs = {int(job["job_idx"]): job for job in order["jobs"]}
        try:
            bindings = self._bind_members(intent, jobs, line_name, actor)
        except Exception as error:
            # Nothing has been sent yet, so a proven-local failure is allowed to undo its own
            # activations instead of leaving Cells no work order owns.
            self._recover_dispatch(intent, error, compensate=True)
            raise
        authority = min(bindings, key=lambda row: row["cell_id"])
        conf = dict(order["conf"])
        conf["_factory_cells"] = bindings
        conf["_factory_submission_id"] = intent.work_id
        conf["_factory_actor"] = actor
        path = "/dags/" + urllib.parse.quote(line_name, safe="")
        dag_run_id = str(order["dag_run_id"])
        dispatch_ref = OperationRef.build(authority["cell_id"], authority["epoch"], "airflow_dispatch", intent.work_id)

        def dispatch() -> dict[str, Any]:
            result = self._checked_airflow(
                "POST",
                path + "/dagRuns",
                {"dag_run_id": dag_run_id, "logical_date": None, "conf": conf},
            )
            if not isinstance(result, dict):
                raise Refused(502, "Airflow returned an invalid DAG run document")
            return result

        def reconcile() -> MutationOutcome:
            status, payload = self.airflow("GET", path + "/dagRuns/" + urllib.parse.quote(dag_run_id, safe=""), None)
            if status == 200 and isinstance(payload, dict):
                return MutationOutcome(
                    "committed",
                    payload,
                    {"dag_run_id": dag_run_id, "status": status},
                    "deterministic Airflow run exists",
                )
            if status == 404:
                return MutationOutcome(
                    "definitely_absent",
                    None,
                    {"dag_run_id": dag_run_id, "status": status},
                    "deterministic Airflow run is absent",
                )
            return MutationOutcome(
                "ambiguous",
                None,
                {"dag_run_id": dag_run_id, "status": status},
                "Airflow outcome cannot yet be proven",
            )

        try:
            self._journal_airflow_unpause(authority, line_name, path)
            result = self.control.mutate(
                dispatch_ref,
                dispatch,
                replay_safe=True,
                reconcile=reconcile,
                # The journal keeps its own attempt budget for this operation kind. Left at its
                # default it is smaller than the outbox's, so the last outbox attempts would be
                # refused before they ever reached Airflow and "attempts exhausted" would mean two
                # different numbers. One budget, one meaning.
                budget=RetryBudget(MAX_DISPATCH_ATTEMPTS, 1.0, 30.0),
            )
        except Exception as error:
            # The remote outcome may be unknown here, so nothing is compensated on a guess: the
            # observation below is what decides, and only once the delivery budget is spent.
            self._recover_dispatch(intent, error, compensate=False, observe=reconcile)
            raise
        run_id = str(result.get("dag_run_id") or result.get("run_id") or dag_run_id)
        self._bind_run(bindings, line_name, run_id, actor)
        self.control.admission.record_dispatch(intent.work_id, token=intent.lease_token, dag_run_id=run_id)
        return run_id

    def _bind_members(
        self,
        intent: DispatchIntent,
        jobs: dict[int, dict[str, Any]],
        line_name: str,
        actor: str,
    ) -> list[dict[str, Any]]:
        bindings: list[dict[str, Any]] = []
        generation = str(intent.order.payload.get("generation") or "stable")
        for member in intent.members:
            job = jobs.get(member.job_idx)
            if job is None:
                raise Refused(500, "work order member has no job in its immutable payload")
            policy_digest = str(job["policy_digest"])
            cell = self._member_cell(intent, member, job, actor)
            try:
                cell = self.cell_store.patch(
                    cell["cell_id"],
                    int(cell["epoch"]),
                    f"policy:{intent.work_id}:{member.job_idx}",
                    policy_digest=policy_digest,
                    factory_generation=generation,
                )
            except DuplicateOperation:
                cell = self.cell_store.get(cell["cell_id"])
            if cell.get("policy_digest") != policy_digest:
                raise Refused(409, "active Factory Cell policy differs from retried submission")
            bindings.append(
                {
                    "job_idx": member.job_idx,
                    "cell_id": cell["cell_id"],
                    "epoch": int(cell["epoch"]),
                    "policy_digest": policy_digest,
                    "factory_generation": cell.get("factory_generation") or generation,
                }
            )
        return bindings

    def _member_cell(
        self,
        intent: DispatchIntent,
        member: Member,
        job: dict[str, Any],
        actor: str,
    ) -> dict[str, Any]:
        """Resolve this member's Cell, adopting only an activation this work order can prove is its own."""
        owner = _cell_actor(actor, intent.work_id)
        try:
            cell = self.cell_store.get(member.cell_id)
        except KeyError:
            cell = None
        if member.cell_epoch is not None and cell is not None:
            if int(cell["epoch"]) == member.cell_epoch and cell["state"] in LIVE_CELL_STATES:
                return cell
            if int(cell["epoch"]) > member.cell_epoch or cell["state"] in TERMINAL_STATES:
                raise Refused(409, "Factory Cell moved past the epoch this work order was admitted for")
        # An attempt of this same order may have activated the Cell and died before recording the
        # epoch. Adopt it only when the Cell's own history names this work order as the actor that
        # activated that epoch: a hopeful pre-write would let a failed activation claim a Cell some
        # other harness legitimately owns.
        unrecorded = member.cell_epoch is None and cell is not None and cell["state"] in LIVE_CELL_STATES
        if unrecorded and self._activated_by(cell, owner):
            self.control.admission.record_member_epoch(
                intent.work_id, member.job_idx, int(cell["epoch"]), token=intent.lease_token
            )
            return cell
        try:
            cell = self.cell_store.activate(identity_for_job(job), actor=owner)
        except CellBusy as error:
            raise Refused(409, str(error)) from error
        self.control.admission.record_member_epoch(
            intent.work_id, member.job_idx, int(cell["epoch"]), token=intent.lease_token
        )
        return cell

    def _activated_by(self, cell: dict[str, Any], owner: str) -> bool:
        epoch = int(cell["epoch"])
        for event in reversed(self.cell_store.history(cell["cell_id"])):
            if event["kind"] == "activated" and int(event["epoch"]) == epoch:
                return bool((event.get("payload") or {}).get("actor") == owner)
        return False

    def _bind_run(
        self,
        bindings: list[dict[str, Any]],
        line_name: str,
        run_id: str,
        actor: str,
    ) -> None:
        for binding in bindings:
            current = self.cell_store.get(binding["cell_id"])
            bound_now = False
            if current["state"] == "dispatching":
                try:
                    self.cell_store.patch(
                        binding["cell_id"],
                        binding["epoch"],
                        f"airflow-bind:{run_id}:{binding['job_idx']}",
                        state="queued",
                        airflow_dag_id=line_name,
                        airflow_run_id=run_id,
                        map_index=binding["job_idx"],
                    )
                    bound_now = True
                except DuplicateOperation:
                    pass
            elif current.get("airflow_run_id") not in {None, run_id}:
                raise Refused(409, "Factory Cell is bound to a different Airflow run")
            if bound_now:
                trace = TraceContext.for_cell(binding["cell_id"], binding["epoch"], "dispatch", run_id)
                envelope = MutationEnvelope(
                    cell_id=binding["cell_id"],
                    epoch=binding["epoch"],
                    operation_key=f"airflow-bind:{run_id}:{binding['job_idx']}",
                    policy_digest=binding["policy_digest"],
                    trace_id=trace.trace_id,
                    actor=actor,
                )
                self.evidence.mutation(
                    envelope,
                    kind="airflow_dispatch",
                    payload={
                        "dag_id": line_name,
                        "run_id": run_id,
                        "map_index": binding["job_idx"],
                    },
                )

    def _recover_dispatch(
        self,
        intent: DispatchIntent,
        error: BaseException,
        *,
        compensate: bool,
        observe: Callable[[], MutationOutcome] | None = None,
    ) -> None:
        detail = str(error)[:2000]
        admission = self.control.admission
        observation = None if compensate else {"remote": "unknown", "detail": detail[:512]}
        try:
            if admission.attempts_exhausted(intent.work_id):
                # Retiring an order cancels Factory Cells, so only the attempt that still owns the
                # intent may do it. A superseded attempt that compensated here would cancel work a
                # newer attempt is in the middle of dispatching.
                admission.assert_lease(intent.work_id, intent.lease_token)
                self._retire_dispatch(intent, detail, compensate=compensate, observe=observe)
                return
            admission.release_dispatch(intent.work_id, token=intent.lease_token, error=detail, observation=observation)
        except DispatchLeaseLost:
            # The order was cancelled while this attempt was in flight. Keep the unproven remote
            # outcome on the record instead of pretending the withdrawal was clean.
            admission.note_outcome(intent.work_id, error=detail, observation=observation)

    def _retire_dispatch(
        self,
        intent: DispatchIntent,
        detail: str,
        *,
        compensate: bool,
        observe: Callable[[], MutationOutcome] | None,
    ) -> None:
        """Close a reservation whose delivery budget is spent, on evidence rather than on a timer.

        Attempts are bounded, so this path is reached eventually by anything that keeps failing.
        What it must never do is leave the order ``admitted``: an admitted reservation that nothing
        can claim any more holds a unit forever, keeps the queue behind it blocked and reports
        nothing -- exactly the strand #2058 is about, one layer up.
        """
        if compensate:
            # Nothing was ever sent, so the failure is proven local.
            self._compensate(intent, detail)
            return
        outcome = self._observe_dispatch(observe)
        if outcome.status == "definitely_absent":
            # Proven: no Airflow run exists for this deterministic id, so the order never ran.
            # Undo its own activations and fail it, which releases the unit and lets the queue move.
            self._compensate(intent, f"undeliverable after {intent.attempt} attempts: {detail}")
            return
        # The run may exist. Capacity stays held rather than released on a guess, but the
        # reservation stops advertising itself as deliverable and is counted as stuck.
        self.control.admission.mark_undeliverable(
            intent.work_id,
            error=f"undeliverable after {intent.attempt} attempts: {detail}",
            observation={
                "remote": "unknown" if outcome.status == "ambiguous" else outcome.status,
                "detail": (outcome.detail or detail)[:512],
                "evidence": outcome.evidence,
            },
        )

    def _observe_dispatch(self, observe: Callable[[], MutationOutcome] | None) -> MutationOutcome:
        """One last look at the remote before retiring an intent; an unusable answer is ambiguous."""
        if observe is None:
            return MutationOutcome("ambiguous", None, None, "no reconciler for this dispatch")
        try:
            return observe()
        except Exception as error:  # noqa: BLE001 - an observation that fails proves nothing
            return MutationOutcome("ambiguous", None, None, str(error)[:512])

    def _cancel_activations(self, work_id: str, tag: str) -> list[str]:
        """Close the Factory Cells this work order activated, at exactly the epochs it recorded.

        Only Cells this work order recorded an epoch for are touched, and only at that exact epoch,
        so this can never cancel a sibling submission's live work.
        """
        closed: list[str] = []
        for member in self.control.admission.members(work_id):
            if member.cell_epoch is None:
                continue
            try:
                cell = self.cell_store.get(member.cell_id)
            except KeyError:
                continue
            if int(cell["epoch"]) != member.cell_epoch or cell["state"] not in LIVE_CELL_STATES:
                continue
            if cell.get("airflow_run_id"):
                # An Airflow run is already bound to this Cell, so the compute is real and the Cell
                # lifecycle owns its ending. Cancelling it from the dispatch side would contradict a
                # run that is still executing -- the one divergence worse than a stuck reservation.
                continue
            try:
                self.cell_store.patch(
                    member.cell_id,
                    member.cell_epoch,
                    f"{tag}:{member.job_idx}",
                    state="cancelled",
                )
            except (DuplicateOperation, StaleEpoch):
                continue
            closed.append(member.cell_id)
        return closed

    def _compensate(self, intent: DispatchIntent, detail: str) -> None:
        """Undo this order's own activations so a failed batch leaves no invisible Cell."""
        self._cancel_activations(intent.work_id, f"compensate:{intent.work_id}:{intent.attempt}")
        self.control.cancel_reservation(intent.work_id, reason=f"activation_failed: {detail}", state="failed")

    def _work_document(self, work_id: str) -> dict[str, Any]:
        """The submission answer, rebuilt from durable state rather than from in-flight locals."""
        order = self.control.admission.work_order(work_id).payload
        members = self.control.admission.members(work_id)
        dispatch = self.control.admission.dispatch_row(work_id) or {}
        state = self.control.admission.state_of(work_id)
        run_id = str(dispatch.get("dag_run_id") or order["dag_run_id"])
        line_name = str(order["line"])
        path = "/dags/" + urllib.parse.quote(line_name, safe="")
        return {
            "state": "submitted" if state == "bound" else str(state),
            "submission_id": work_id,
            "dag_id": line_name,
            "run_id": run_id,
            "issues": list(order["issues"]),
            "jobs": len(order["jobs"]),
            "cells": [member.cell_id for member in members],
            "blueprint": {"name": line_name, "resolved": True},
            "url": self.airflow_url + path + "/runs/" + urllib.parse.quote(run_id, safe=""),
        }

    def compatibility(self, method: str, target: str, body: dict | None) -> tuple[int, Any]:
        parsed = urllib.parse.urlsplit(target)
        path = parsed.path
        segments = [urllib.parse.unquote(p) for p in path.split("/")[1:]]
        if any(p in {".", ".."} or "/" in p or "\\" in p for p in segments):
            raise Refused(400, "invalid path segment")
        if method == "GET" and READ_ROUTES.fullmatch(path):
            return self.airflow(method, target, None)
        if parsed.query:
            raise Refused(400, "mutation queries are not supported")
        if len(segments) < 2 or segments[0] != "dags":
            raise Refused(404, "unknown control route")
        self._line(segments[1])
        if method == "POST" and len(segments) == 3 and segments[2] == "dagRuns":
            conf = (body or {}).get("conf", {})
            if not isinstance(conf, dict) or set(conf) - {"issues", "targets"}:
                raise ValueError("only issues and installed targets can be submitted")
            submission = self.submit({"line": segments[1], **conf})
            if submission.get("state") != "submitted":
                raise Refused(
                    429,
                    f"factory admission {submission.get('state')}: {submission.get('reason')}",
                )
            return 201, {"dag_run_id": submission["run_id"]}
        if method == "PATCH" and len(segments) == 2 and body == {"is_paused": False}:
            return self.airflow(method, path, body)
        if method == "PATCH" and len(segments) == 4 and segments[2] == "dagRuns" and body == {"state": "failed"}:
            dag_id, run_id = segments[1], segments[3]
            managed = [
                cell
                for cell in self.cell_store.list(limit=1000)
                if cell.get("airflow_dag_id") == dag_id
                and cell.get("airflow_run_id") == run_id
                and (cell.get("state") not in TERMINAL_STATES or cell.get("state") == "cancelled")
            ]
            if not managed:
                return self.airflow(method, path, body)
            for cell in managed:
                self._transition(
                    {
                        "cell_id": cell["cell_id"],
                        "epoch": int(cell["epoch"]),
                        "state": "cancelled",
                        "operation_key": f"operator:airflow-stop:{run_id}:{cell['cell_id']}",
                    }
                )
            return 200, {"state": "failed", "managed_cells": len(managed)}
        if (
            method == "PATCH"
            and len(segments) == 8
            and segments[2] == "dagRuns"
            and segments[4] == "taskInstances"
            and segments[7] == "hitlDetails"
        ):
            if body not in (
                {"chosen_options": ["Approve"], "params_input": {}},
                {"chosen_options": ["Reject"], "params_input": {}},
            ):
                raise ValueError("only explicit Approve or Reject answers are supported")
            tasks = AirflowClient(self.airflow_url, token=self._credential(), opener=self.opener.open)
            states = tasks.task_states(segments[1], segments[3])
            index = int(segments[6])
            if not any(
                t.task_id == segments[5] and t.map_index == index and t.state in {"awaiting_input", "deferred"}
                for t in states
            ):
                raise Refused(409, "gate is not waiting for operator input")
            return self.airflow(method, path, body)
        raise Refused(403, "operation is outside the factory control surface")

    def _gh(self, args: list[str]) -> Any:
        if not self.repo:
            raise Refused(503, "SWF_REPO is not configured on the backend")
        result = subprocess.run(
            ["gh", *args, "--repo", self.repo],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if result.returncode:
            raise Refused(502, "GitHub operation failed; check backend credentials and repository")
        return json.loads(result.stdout)

    def _cell(self, cell_id: str) -> dict[str, Any]:
        try:
            return self.cell_store.get(cell_id)
        except KeyError as error:
            raise Refused(404, f"no Factory Cell {cell_id}") from error

    def capabilities(self) -> dict[str, Any]:
        draining = os.getenv("SWF_DRAIN", "").lower() in {"1", "true", "yes"}
        # Two fields stopped being constants with #2074. A state root this binary must not own, or
        # one restored from a backup and not yet validated, is not authoritative storage: admitting
        # work into it would reserve capacity and activate Cells against a journal whose
        # relationship to the outside world is still unknown. Reporting it here refuses the whole
        # mutation surface at once rather than one route at a time.
        detail = ""
        schema_compatible = True
        try:
            assert_compatible(self.state_root)
        except StoreSchemaError as error:
            schema_compatible = False
            detail = str(error)
        storage_authoritative = self.restore_gate.state != RESTORE_PENDING
        if not storage_authoritative and not detail:
            detail = (
                "restored factory state has not been validated; run `swfactory backup status` and "
                "then `swfactory backup resume` before mutations resume"
            )
        return capability_document(
            read_ready=True,
            storage_authoritative=storage_authoritative,
            schema_compatible=schema_compatible,
            draining=draining,
            detail=detail,
            serving_generation=os.getenv("SWF_GENERATION") or "stable",
            draining_generation=(os.getenv("SWF_GENERATION") or "stable") if draining else None,
        ).to_dict()

    def fleet(self) -> dict[str, Any]:
        cells = self.cell_store.list(limit=1000)
        control = self.control.snapshot(limit=1000)
        counts: dict[str, int] = {}
        generations: dict[str, int] = {}
        stale = 0
        orphaned = 0
        cleanup_debt = 0
        now = time.time()
        for cell in cells:
            state = str(cell.get("state") or "unknown")
            counts[state] = counts.get(state, 0) + 1
            generation = str(cell.get("factory_generation") or "unknown")
            generations[generation] = generations.get(generation, 0) + 1
            if state == "dispatching" and not cell.get("airflow_run_id") and now - float(cell["updated_at"]) > 300:
                stale += 1
                orphaned += 1
            if cell.get("compute") and not cell.get("cleanup") and state in TERMINAL_STATES:
                cleanup_debt += 1
        pressure = control["queue"]["pressure"]
        unresolved = control["operations"]
        bottlenecks: list[str] = []
        if pressure.get("queued"):
            bottlenecks.append(f"queue:{pressure['queued']}")
        if unresolved:
            bottlenecks.append(f"repair_debt:{len(unresolved)}")
        if cleanup_debt:
            bottlenecks.append(f"cleanup_debt:{cleanup_debt}")
        return {
            "cells_active": sum(counts.get(s, 0) for s in ("dispatching", "queued", "running")),
            "cells_queued": counts.get("queued", 0),
            "cells_failed": counts.get("failed", 0),
            "cells_stale": stale,
            "cells_orphaned": orphaned,
            "queue_depth": int(pressure.get("queued", 0)),
            "unresolved_operations": len(unresolved),
            "cleanup_debt": cleanup_debt,
            "generation_counts": dict(sorted(generations.items())),
            "bottlenecks": bottlenecks,
        }

    def _journal_airflow_cancel(self, cell: dict[str, Any]) -> dict[str, Any]:
        """Stop one managed Airflow run through the same durable operation journal as dispatch.

        The Cell is cancelled *before* this method is called. Capacity is released only after this
        operation is committed or observation proves the run is already terminal, so an ambiguous
        stop can never admit replacement work while the old run may still be alive.
        """
        dag_id = str(cell.get("airflow_dag_id") or "")
        run_id = str(cell.get("airflow_run_id") or "")
        if not dag_id or not run_id:
            return {"state": "no_remote_run"}
        path = "/dags/" + urllib.parse.quote(dag_id, safe="") + "/dagRuns/" + urllib.parse.quote(run_id, safe="")
        ref = OperationRef.build(str(cell["cell_id"]), int(cell["epoch"]), "airflow_cancel", dag_id, run_id)

        def apply() -> dict[str, Any]:
            payload = self._checked_airflow("PATCH", path, {"state": "failed"})
            return payload if isinstance(payload, dict) else {"state": "failed"}

        def reconcile() -> MutationOutcome:
            status, payload = self.airflow("GET", path, None)
            if status == 404:
                return MutationOutcome(
                    "committed",
                    {"state": "absent", "dag_run_id": run_id},
                    {"status": status, "dag_run_id": run_id},
                    "managed Airflow run is absent",
                )
            if status == 200 and isinstance(payload, dict):
                state = str(payload.get("state") or "")
                if state in {"failed", "success"}:
                    return MutationOutcome(
                        "committed",
                        payload,
                        {"status": status, "state": state, "dag_run_id": run_id},
                        "managed Airflow run is terminal",
                    )
                return MutationOutcome(
                    "definitely_absent",
                    None,
                    {"status": status, "state": state, "dag_run_id": run_id},
                    "managed Airflow run is still live",
                )
            return MutationOutcome(
                "ambiguous",
                None,
                {"status": status, "dag_run_id": run_id},
                "managed Airflow cancellation outcome is unknown",
            )

        result = self.control.mutate(ref, apply, replay_safe=True, reconcile=reconcile)
        return result if isinstance(result, dict) else {"state": "failed"}

    def _transition(self, body: dict[str, Any]) -> dict[str, Any]:
        cell_id = text(body, "cell_id")
        epoch = body.get("epoch")
        if type(epoch) is not int or epoch < 1:
            raise ValueError("epoch must be a positive integer")
        requested = text(body, "state", max_len=64)
        if requested not in {"running", "success", "failed", "cancelled", "rejected", "cleaned"}:
            raise ValueError("invalid lifecycle state")
        operation_key = text(
            {"operation_key": body.get("operation_key") or f"operator:{requested}"},
            "operation_key",
            max_len=256,
        )
        current = self._cell(cell_id)
        if int(current["epoch"]) != epoch:
            raise Refused(409, f"stale Factory Cell epoch {epoch}; current epoch is {current['epoch']}")

        if requested == "cleaned":
            cleanup = {
                "schema_version": 1,
                "status": "converged",
                "operation_key": operation_key,
                "observed_at": time.time(),
            }
            try:
                updated = self.cell_store.patch(
                    cell_id,
                    epoch,
                    operation_key,
                    cleanup=cleanup,
                )
            except DuplicateOperation:
                updated = self._cell(cell_id)
            released = (
                self.control.release_cell(cell_id, epoch=epoch, state=str(updated["state"]))
                if updated["state"] in TERMINAL_STATES
                else []
            )
            next_state = updated["state"]
        else:
            old_state = str(current["state"])
            if old_state in {"failed", "cancelled", "rejected", "cleaned"} and requested != old_state:
                raise Refused(409, f"terminal Factory Cell cannot transition {old_state} -> {requested}")
            if old_state == "success" and requested not in {"success", "failed"}:
                raise Refused(409, f"Factory Cell cannot transition success -> {requested}")
            if requested == "running" and old_state not in {"dispatching", "queued", "running"}:
                raise Refused(409, f"Factory Cell cannot transition {old_state} -> running")
            try:
                updated = self.cell_store.patch(
                    cell_id,
                    epoch,
                    operation_key,
                    state=requested,
                )
            except DuplicateOperation:
                updated = self._cell(cell_id)
            if (
                requested == "cancelled"
                and not operation_key.startswith("airflow:")
                and updated.get("airflow_dag_id")
                and updated.get("airflow_run_id")
            ):
                self._journal_airflow_cancel(updated)
            released = (
                self.control.release_cell(cell_id, epoch=epoch, state=requested) if requested in TERMINAL_STATES else []
            )
            next_state = requested

        # Draining only moves an admission's state; the work it released is still owed a command.
        # Resuming here is what turns "B is admitted" into "B actually runs" -- and it happens on
        # the backend, so Airflow workers never gain a second scheduler of their own.
        resumed = self.resume_dispatch() if released else []
        self.evidence.append(
            cell_id=cell_id,
            epoch=epoch,
            kind="lifecycle_transition" if requested != "cleaned" else "cleanup",
            payload={
                "from": current.get("state"),
                "requested": requested,
                "to": next_state,
                "released_work": released,
                "resumed_dispatch": resumed,
            },
            policy_digest=updated.get("policy_digest"),
            trace=TraceContext.for_cell(cell_id, epoch, "lifecycle", operation_key),
        )
        return {"cell": updated, "released_work": released, "resumed_dispatch": resumed}

    def operation(self, path: str, body: dict[str, Any]) -> Any:
        if path == "/doctor":
            caps = self.capabilities()
            # Every row carries `ok` as well as `status`. The console deserializes into
            # `swf_domain::doctor::Check`, whose `ok: bool` has no default and no alias — so a row
            # with only `status` fails to parse, the whole response is discarded, and `swf doctor`
            # prints one fabricated failure blaming the operator's token. `doctor` is the command
            # someone runs when nothing else works; it must not be the thing that lies to them.
            # `status` is kept alongside for the Python CLI, which reads it.
            checks = [
                {
                    "name": "factory backend",
                    "ok": True,
                    "status": "ok",
                    "detail": "Python API v1",
                    "required": True,
                },
                {
                    "name": "factory cells",
                    "ok": True,
                    "status": "ok",
                    "detail": f"durable CellStore schema v{SCHEMA_VERSION}",
                    "required": True,
                },
                {
                    "name": "mutation readiness",
                    "ok": bool(caps["mutation_ready"]),
                    "status": "ok" if caps["mutation_ready"] else "warn",
                    # A capability document rendered as text: `detail` is a string on both sides,
                    # and an object here failed to parse even once `ok` was present.
                    "detail": ", ".join(f"{k}={v}" for k, v in sorted(caps.items())),
                    "required": True,
                },
            ]
            # The row the console path was missing (#2050 added it to the Python doctor only): a
            # managed cell fails closed in its FIRST stage without SWF_BACKEND_URL/SWF_BACKEND_TOKEN,
            # and from `swf doctor` that looked like a healthy backend. One honesty caveat, written
            # into `detail`: this reads THIS process's environment. The workers carry their own copy
            # (Compose passes the pair to the airflow service separately), so a green row here means
            # the backend host is configured, not that every worker is -- the compose guard in
            # tests/test_doctor.py is what pins the worker side.
            workers = _check_managed_workers(os.environ)
            checks.append(
                {
                    "name": "managed worker callback",
                    "ok": workers.ok,
                    "status": workers.status,
                    "detail": workers.detail + " (as seen from the backend host's environment)",
                    "fix": workers.fix,
                    "required": workers.required,
                }
            )
            try:
                health = self._checked_airflow("GET", "/monitor/health")
                for name in ("metadatabase", "scheduler"):
                    healthy = (health.get(name) or {}).get("status") == "healthy"
                    checks.append(
                        {
                            "name": name,
                            "ok": healthy,
                            "status": "ok" if healthy else "fail",
                            "detail": "Airflow health",
                            "required": True,
                            "fix": "" if healthy else "restore the Airflow service",
                        }
                    )
                self._checked_airflow("GET", "/dags?limit=1")
                checks.append({"name": "airflow auth", "ok": True, "status": "ok", "required": True})
            except (Refused, ControlError, OSError):
                checks.append(
                    {
                        "name": "airflow",
                        "ok": False,
                        "status": "fail",
                        "required": True,
                        "detail": "Airflow is unavailable or authentication failed",
                        "fix": "check AIRFLOW_URL and credentials on the backend",
                    }
                )
            for tool, configured in (("gh", bool(self.repo)), ("islo", bool(self.owner))):
                present = bool(shutil.which(tool))
                checks.append(
                    {
                        "name": tool,
                        "ok": present,
                        "status": "ok" if present else "warn",
                        "required": False,
                        "detail": (
                            f"backend tool installed={present}, configured={configured}; credentials not probed"
                        ),
                        "fix": "" if present else f"install {tool} on the backend if needed",
                    }
                )
            return checks
        if path == "/compatibility":
            return self.capabilities()
        if path == "/fleet":
            return self.fleet()
        if path == "/queue":
            return self.control.admission.snapshot(limit=self._limit(body))
        if path == "/queue/resume":
            # The explicit operator handle on the same redelivery the request paths pump, for the
            # case where a backend restarted and nothing has submitted or transitioned since.
            return {"resumed": self.resume_dispatch(limit=self._limit(body))}
        if path == "/queue/cancel":
            work_id = text(body, "work_id")
            state = self.control.admission.state_of(work_id)
            if state is None:
                raise Refused(404, f"no admission work {work_id}")
            if state == "bound":
                # The Airflow run already exists; cancelling it here would release capacity while
                # the compute keeps going. Its Factory Cells own that decision.
                raise Refused(409, "a dispatched work order is cancelled through its Factory Cells")
            # An order can already have activated its Cells and still be cancellable: a delivery
            # that failed after activation leaves it admitted with live Cells behind it. Closing
            # the reservation without closing those Cells leaves a live Cell no work order owns --
            # and because the Cell's epoch is part of the deterministic work id, the identity then
            # answers 409 to every resubmission forever. That is #2058 with the arrows reversed.
            reason = text(body, "reason", max_len=512)
            cancelled_cells = self._cancel_activations(work_id, f"queue-cancel:{work_id}")
            released = self.control.cancel_reservation(work_id, reason=reason)
            return {
                "work_id": work_id,
                "was": state,
                "state": self.control.admission.state_of(work_id),
                "cancelled_cells": cancelled_cells,
                "released_work": released,
                "resumed_dispatch": self.resume_dispatch(),
                "dispatch": self.control.admission.dispatch_row(work_id),
            }
        if path == "/queue/inspect":
            work_id = text(body, "work_id")
            snapshot = self.control.admission.snapshot(limit=1000)
            for section in ("active", "queued"):
                for row in snapshot[section]:
                    if row["work_id"] == work_id:
                        return row
            raise Refused(404, f"no admission work {work_id}")
        if path == "/operations":
            return self.control.operations.unresolved(limit=self._limit(body))
        if path == "/operations/inspect":
            try:
                return self.control.operations.get(text(body, "operation_key"))
            except KeyError as error:
                raise Refused(404, "no such operation") from error
        if path == "/work-orders":
            if not self.capabilities()["mutation_ready"]:
                raise Refused(503, "backend is draining or not mutation-ready")
            return self.submit(body)
        if path == "/blueprints/preview":
            line = self._line(text(body, "line"))
            issues = body.get("issues") or []
            targets = body.get("targets") or []
            conf = {"issues": issues, **({"targets": targets} if targets else {})}
            return build_preview(line=line.name, jobs=list(line.jobs(conf))).to_dict()
        if path == "/cells":
            return self.cell_store.list(limit=self._limit(body))
        if path == "/cells/inspect":
            return self._cell(text(body, "cell_id"))
        if path == "/cells/history":
            cell_id = text(body, "cell_id")
            self._cell(cell_id)
            return self.cell_store.history(cell_id)
        if path == "/cells/transition":
            return self._transition(body)
        if path == "/evidence/verify":
            cell_id = text(body, "cell_id")
            ok, tail = self.evidence.verify(cell_id)
            return {"cell_id": cell_id, "verified": ok, "tail_digest": tail}
        if path == "/evidence/checkpoint":
            return self.evidence.checkpoint(text(body, "cell_id"))
        if path == "/deliveries/prs":
            if not self.repo:
                return []
            return GitHubClient(self.repo).prs(
                label=text({"label": body.get("label", "factory")}, "label"),
                limit=self._limit(body),
            )
        if path == "/deliveries/issues":
            if not self.repo:
                return []
            return GitHubClient(self.repo).issues(
                label=text({"label": body.get("label", "factory")}, "label"),
                limit=self._limit(body),
            )
        if path == "/deliveries/head":
            rows = self._gh(
                [
                    "pr",
                    "list",
                    "--head",
                    text(body, "branch"),
                    "--state",
                    "all",
                    "--limit",
                    "1",
                    "--json",
                    "url,state,title,labels,headRefOid,baseRefName",
                ]
            )
            if not rows:
                return None
            row = rows[0]
            return {
                "url": row["url"],
                "state": row["state"],
                "title": row["title"],
                "labels": [label["name"] for label in row.get("labels", [])],
                "head_sha": row["headRefOid"],
                "base_ref": row["baseRefName"],
            }
        if path in {"/deliveries/checks", "/deliveries/url"}:
            from swfactory.control import summarize_checks

            number = body.get("number")
            if type(number) is not int or number <= 0:
                raise ValueError("number must be a positive integer")
            if path == "/deliveries/url":
                return self._gh(["pr", "view", str(number), "--json", "url"])["url"]
            row = self._gh(["pr", "view", str(number), "--json", "statusCheckRollup"])
            return summarize_checks(row.get("statusCheckRollup"))
        if path == "/workers":
            return IsloClient(self.owner).own_sandboxes()
        if path == "/workers/remove":
            return IsloClient(self.owner).remove(text(body, "name"))
        if path == "/metrics/runs":
            return MetricsSource(self.root).runs()
        if path == "/metrics/summary":
            return MetricsSource(self.root).summary()
        if path == "/state/runs":
            return list_runs(self.state_root, limit=self._limit(body))
        if path == "/state/inspect":
            return inspect_run(self.state_root, text(body, "run_id"))
        if path == "/lines":
            return [
                {
                    "name": bp.name,
                    "targets": [t.repo for t in bp.targets],
                    "route": list(bp.order),
                    "gates": [g.model_dump() for g in bp.gates],
                }
                for bp in (blueprint.load(str(p)) for p in blueprint.blueprint_paths())
            ]
        raise Refused(404, "unknown factory operation")

    @staticmethod
    def _limit(body: dict[str, Any]) -> int:
        limit = body.get("limit", 30)
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        return limit
