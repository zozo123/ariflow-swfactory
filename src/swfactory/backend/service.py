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
from pathlib import Path
from typing import Any

from swfactory import blueprint
from swfactory.admission import Priority
from swfactory.cell_runtime import identity_for_job
from swfactory.cells import SCHEMA_VERSION, TERMINAL_STATES, CellBusy, CellStore, DuplicateOperation
from swfactory.control import AirflowClient, ControlError, GitHubClient, IsloClient, MetricsSource
from swfactory.control_kernel import ControlKernel
from swfactory.idempotency import MutationOutcome, OperationRef
from swfactory.inspection import inspect_run, list_runs
from swfactory.lifecycle_evidence import TraceContext
from swfactory.product_surface import build_preview, capability_document
from swfactory.security_contract import MutationEnvelope, policy_digest_for_mapping
from swfactory.trust_evidence import TrustedEvidence
from swfactory.webhook import _NoRedirect, _safe_airflow_base

MAX_RESPONSE = 16 * 1024 * 1024
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
        self.cell_store = CellStore(self.state_root / "cells.sqlite3")
        self.control = ControlKernel(self.state_root / "control")
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
            suffix = (
                "; mutation outcome may be unknown" if method != "GET" and status >= 500 else ""
            )
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

    def _submission_id(self, line_name: str, jobs: list[dict[str, Any]], actor: str) -> str:
        payload = {
            "line": line_name,
            "actor": actor,
            "jobs": [
                {
                    "job_idx": int(job["job_idx"]),
                    "repo": str(job["repo"]),
                    "issue": str(job["issue"]),
                    "dir": str(job.get("dir", "")),
                    "base_branch": str(job.get("base_branch", "main")),
                    "desired_epoch": self._desired_epoch(job),
                    "policy_digest": self._policy_digest(line_name, job),
                }
                for job in jobs
            ],
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return "submit_" + digest[:32]

    def _activate_bindings(
        self,
        line_name: str,
        jobs: list[dict[str, Any]],
        submission_id: str,
        actor: str,
        *,
        recover_existing: bool,
    ) -> list[dict[str, Any]]:
        bindings: list[dict[str, Any]] = []
        generation = os.getenv("SWF_GENERATION") or "stable"
        for job in jobs:
            identity = identity_for_job(job)
            policy_digest = self._policy_digest(line_name, job)
            if recover_existing:
                try:
                    cell = self.cell_store.get(identity.stable_id())
                except KeyError as error:
                    raise Refused(
                        409, "active admission has no corresponding Factory Cell"
                    ) from error
                if cell["state"] in TERMINAL_STATES:
                    raise Refused(409, "active admission points at a terminal Factory Cell")
            else:
                try:
                    cell = self.cell_store.activate(identity, actor=f"backend:{actor}")
                except CellBusy as error:
                    raise Refused(409, str(error)) from error
            try:
                cell = self.cell_store.patch(
                    cell["cell_id"],
                    int(cell["epoch"]),
                    f"policy:{submission_id}:{job['job_idx']}",
                    policy_digest=policy_digest,
                    factory_generation=generation,
                )
            except DuplicateOperation:
                cell = self.cell_store.get(cell["cell_id"])
            if cell.get("policy_digest") != policy_digest:
                raise Refused(409, "active Factory Cell policy differs from retried submission")
            bindings.append(
                {
                    "job_idx": int(job["job_idx"]),
                    "cell_id": cell["cell_id"],
                    "epoch": int(cell["epoch"]),
                    "policy_digest": policy_digest,
                    "factory_generation": cell.get("factory_generation") or generation,
                }
            )
        return bindings

    def _journal_airflow_unpause(
        self, authority: dict[str, Any], line_name: str, path: str
    ) -> None:
        ref = OperationRef.build(
            authority["cell_id"], authority["epoch"], "airflow_unpause", line_name
        )

        def apply() -> dict[str, Any]:
            payload = self._checked_airflow("PATCH", path, {"is_paused": False})
            return payload if isinstance(payload, dict) else {"is_paused": False}

        def reconcile() -> MutationOutcome:
            status, payload = self.airflow("GET", path, None)
            if status == 200 and isinstance(payload, dict):
                if payload.get("is_paused") is False:
                    return MutationOutcome(
                        "committed", payload, {"dag": line_name}, "DAG is unpaused"
                    )
                return MutationOutcome(
                    "definitely_absent", None, {"dag": line_name}, "DAG remains paused"
                )
            return MutationOutcome(
                "ambiguous", None, {"dag": line_name, "status": status}, "DAG state unavailable"
            )

        self.control.mutate(ref, apply, replay_safe=True, reconcile=reconcile)

    def submit(self, body: dict[str, Any]) -> dict[str, Any]:
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
        submission_id = self._submission_id(line.name, jobs, actor)
        repos = sorted({str(job["repo"]) for job in jobs})
        repo_key = (
            repos[0]
            if len(repos) == 1
            else "multi:" + hashlib.sha256("\0".join(repos).encode()).hexdigest()[:16]
        )
        decision = self.control.submit(
            work_id=submission_id,
            repo=repo_key,
            actor=actor,
            blueprint=line.name,
            priority=_priority(body.get("priority")),
        )
        if decision.state != "active":
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

        recover_existing = decision.reason == "duplicate_active"
        try:
            bindings = self._activate_bindings(
                line.name,
                jobs,
                submission_id,
                actor,
                recover_existing=recover_existing,
            )
        except Exception:
            if not recover_existing:
                self.control.cancel_reservation(submission_id, reason="activation_failed")
            raise

        authority = min(bindings, key=lambda row: row["cell_id"])
        self.control.bind_cell(submission_id, authority["cell_id"], authority["epoch"])
        conf["_factory_cells"] = bindings
        conf["_factory_submission_id"] = submission_id
        conf["_factory_actor"] = actor

        path = "/dags/" + urllib.parse.quote(line.name, safe="")
        self._journal_airflow_unpause(authority, line.name, path)
        dag_run_id = "swf__" + submission_id.removeprefix("submit_")
        dispatch_ref = OperationRef.build(
            authority["cell_id"], authority["epoch"], "airflow_dispatch", submission_id
        )

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
            status, payload = self.airflow(
                "GET", path + "/dagRuns/" + urllib.parse.quote(dag_run_id, safe=""), None
            )
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

        result = self.control.mutate(
            dispatch_ref,
            dispatch,
            replay_safe=True,
            reconcile=reconcile,
        )
        run_id = result.get("dag_run_id") or result.get("run_id") or dag_run_id
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
                        airflow_dag_id=line.name,
                        airflow_run_id=run_id,
                        map_index=binding["job_idx"],
                    )
                    bound_now = True
                except DuplicateOperation:
                    pass
            elif current.get("airflow_run_id") not in {None, run_id}:
                raise Refused(409, "Factory Cell is bound to a different Airflow run")
            if bound_now:
                trace = TraceContext.for_cell(
                    binding["cell_id"], binding["epoch"], "dispatch", run_id
                )
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
                        "dag_id": line.name,
                        "run_id": run_id,
                        "map_index": binding["job_idx"],
                    },
                )

        return {
            "state": "submitted",
            "submission_id": submission_id,
            "dag_id": line.name,
            "run_id": run_id,
            "issues": issues,
            "jobs": len(jobs),
            "cells": [b["cell_id"] for b in bindings],
            "blueprint": {"name": line.name, "resolved": True},
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
        if (
            method == "PATCH"
            and len(segments) == 4
            and segments[2] == "dagRuns"
            and body == {"state": "failed"}
        ):
            return self.airflow(method, path, body)
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
            tasks = AirflowClient(
                self.airflow_url, token=self._credential(), opener=self.opener.open
            )
            states = tasks.task_states(segments[1], segments[3])
            index = int(segments[6])
            if not any(
                t.task_id == segments[5]
                and t.map_index == index
                and t.state in {"awaiting_input", "deferred"}
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
        return capability_document(
            read_ready=True,
            storage_authoritative=True,
            schema_compatible=True,
            draining=draining,
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
            if (
                state == "dispatching"
                and not cell.get("airflow_run_id")
                and now - float(cell["updated_at"]) > 300
            ):
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

    def _transition(self, body: dict[str, Any]) -> dict[str, Any]:
        cell_id = text(body, "cell_id")
        epoch = body.get("epoch")
        if type(epoch) is not int or epoch < 1:
            raise ValueError("epoch must be a positive integer")
        requested = text(body, "state", max_len=64)
        if requested not in {"running", "success", "failed", "cancelled", "rejected", "cleaned"}:
            raise ValueError("invalid lifecycle state")
        operation_key = text(
            {"operation_key": body.get("operation_key") or f"airflow:{requested}"},
            "operation_key",
            max_len=256,
        )
        current = self._cell(cell_id)
        if int(current["epoch"]) != epoch:
            raise Refused(
                409, f"stale Factory Cell epoch {epoch}; current epoch is {current['epoch']}"
            )

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
            if (
                old_state in {"failed", "cancelled", "rejected", "cleaned"}
                and requested != old_state
            ):
                raise Refused(
                    409, f"terminal Factory Cell cannot transition {old_state} -> {requested}"
                )
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
            released = (
                self.control.release_cell(cell_id, epoch=epoch, state=requested)
                if requested in TERMINAL_STATES
                else []
            )
            next_state = requested

        self.evidence.append(
            cell_id=cell_id,
            epoch=epoch,
            kind="lifecycle_transition" if requested != "cleaned" else "cleanup",
            payload={
                "from": current.get("state"),
                "requested": requested,
                "to": next_state,
                "released_work": released,
            },
            policy_digest=updated.get("policy_digest"),
            trace=TraceContext.for_cell(cell_id, epoch, "lifecycle", operation_key),
        )
        return {"cell": updated, "released_work": released}

    def operation(self, path: str, body: dict[str, Any]) -> Any:
        if path == "/doctor":
            caps = self.capabilities()
            checks = [
                {
                    "name": "factory backend",
                    "status": "ok",
                    "detail": "Python API v1",
                    "required": True,
                },
                {
                    "name": "factory cells",
                    "status": "ok",
                    "detail": f"durable CellStore schema v{SCHEMA_VERSION}",
                    "required": True,
                },
                {
                    "name": "mutation readiness",
                    "status": "ok" if caps["mutation_ready"] else "warn",
                    "detail": caps,
                    "required": True,
                },
            ]
            try:
                health = self._checked_airflow("GET", "/monitor/health")
                for name in ("metadatabase", "scheduler"):
                    healthy = (health.get(name) or {}).get("status") == "healthy"
                    checks.append(
                        {
                            "name": name,
                            "status": "ok" if healthy else "fail",
                            "detail": "Airflow health",
                            "required": True,
                            "fix": "" if healthy else "restore the Airflow service",
                        }
                    )
                self._checked_airflow("GET", "/dags?limit=1")
                checks.append({"name": "airflow auth", "status": "ok", "required": True})
            except (Refused, ControlError, OSError):
                checks.append(
                    {
                        "name": "airflow",
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
                        "status": "ok" if present else "warn",
                        "required": False,
                        "detail": (
                            f"backend tool installed={present}, configured={configured}; "
                            "credentials not probed"
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
