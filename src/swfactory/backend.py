"""Factory control API. Rust is the console; Python owns service access and execution.

The Airflow compatibility mount keeps its public response shapes, pagination and conflicts.
It is an allowlisted control surface, not an arbitrary HTTP proxy or remote shell.
"""

from __future__ import annotations

import dataclasses
import hmac
import json
import os
import re
import shutil
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from swfactory import blueprint
from swfactory.cell_runtime import identity_for_job
from swfactory.cells import CellBusy, CellStore, DuplicateOperation, SCHEMA_VERSION
from swfactory.control import AirflowClient, ControlError, GitHubClient, IsloClient, MetricsSource
from swfactory.inspection import inspect_run, list_runs
from swfactory.webhook import _NoRedirect, _safe_airflow_base

PREFIX = "/v1"
MAX_BODY = 64 * 1024
MAX_RESPONSE = 16 * 1024 * 1024
LINE_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}\Z")
# Dynamic segments remain encoded all the way to Airflow. No filesystem paths are accepted.
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


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def _text(body: dict, key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError(f"{key} must be a nonempty string of at most 512 characters")
    return value.strip()


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
        self.cell_store = CellStore(self.state_root / "cells.sqlite3")
        self.opener = urllib.request.build_opener(_NoRedirect)
        self.credentials = AirflowClient(
            self.airflow_url,
            token=os.getenv("AIRFLOW_TOKEN") or None,
            username=os.getenv("AIRFLOW_USER"),
            password=os.getenv("AIRFLOW_PASSWORD"),
            opener=self.opener.open,
        )
        self.auth_lock = threading.Lock()

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
        """Bounded transport; preserve status and never replay a possibly committed mutation."""
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
            # Only authentication rejection is safe to retry. Timeouts retain unknown outcome.
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
            outcome = (
                "; mutation outcome may be unknown" if method != "GET" and status >= 500 else ""
            )
            raise Refused(status, f"Airflow rejected {method} (HTTP {status}){outcome}")
        return payload

    def submit(self, body: dict) -> dict:
        line = self._line(_text(body, "line"))
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

        conf: dict[str, Any] = {"issues": issues, **({"targets": targets} if targets else {})}
        jobs = line.jobs(conf)  # Authoritative validation against the server's installed line.

        # Activation is a durable compare-and-swap before the external Airflow mutation. Two
        # concurrent submissions for the same issue×target cannot both become authoritative.
        bindings: list[dict[str, Any]] = []
        for job in jobs:
            try:
                cell = self.cell_store.activate(identity_for_job(job), actor="backend:submit")
            except CellBusy as error:
                raise Refused(409, str(error)) from error
            bindings.append(
                {
                    "job_idx": int(job["job_idx"]),
                    "cell_id": cell["cell_id"],
                    "epoch": int(cell["epoch"]),
                }
            )
        conf["_factory_cells"] = bindings

        path = "/dags/" + urllib.parse.quote(line.name, safe="")
        self._checked_airflow("PATCH", path, {"is_paused": False})
        result = self._checked_airflow(
            "POST", path + "/dagRuns", {"logical_date": None, "conf": conf}
        )
        run_id = (result or {}).get("dag_run_id") or (result or {}).get("run_id")
        if not run_id:
            raise Refused(
                502, "Airflow accepted submission without a run ID; inspect runs before retrying"
            )

        for binding in bindings:
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
            except DuplicateOperation:
                # The exact binding was already durably recorded; replay is safe.
                pass

        return {
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
            # Enforce readiness server-side too, even for a client bypassing the Rust prompts.
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

    def operation(self, path: str, body: dict) -> Any:
        if path == "/doctor":
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
        if path == "/work-orders":
            return self.submit(body)
        if path == "/cells":
            return self.cell_store.list(limit=self._limit(body))
        if path == "/cells/inspect":
            return self._cell(_text(body, "cell_id"))
        if path == "/cells/history":
            cell_id = _text(body, "cell_id")
            self._cell(cell_id)
            return self.cell_store.history(cell_id)
        if path == "/deliveries/prs":
            if not self.repo:
                return []
            return GitHubClient(self.repo).prs(
                label=_text({"label": body.get("label", "factory")}, "label"),
                limit=self._limit(body),
            )
        if path == "/deliveries/issues":
            if not self.repo:
                return []
            return GitHubClient(self.repo).issues(
                label=_text({"label": body.get("label", "factory")}, "label"),
                limit=self._limit(body),
            )
        if path == "/deliveries/head":
            rows = self._gh(
                [
                    "pr", "list", "--head", _text(body, "branch"), "--state", "all",
                    "--limit", "1", "--json", "url,state,title,labels,headRefOid,baseRefName",
                ]
            )  # fmt: skip
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
            return IsloClient(self.owner).remove(_text(body, "name"))
        if path == "/metrics/runs":
            return MetricsSource(self.root).runs()
        if path == "/metrics/summary":
            return MetricsSource(self.root).summary()
        if path == "/state/runs":
            return list_runs(self.state_root, limit=self._limit(body))
        if path == "/state/inspect":
            return inspect_run(self.state_root, _text(body, "run_id"))
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
    def _limit(body: dict) -> int:
        limit = body.get("limit", 30)
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        return limit


def make_server(factory: Factory, host: str = "127.0.0.1", port: int = 8082) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        server_version = "swfactory-backend/1"

        def log_message(self, *_args: Any) -> None:
            pass  # Never log credentials, work orders or approval bodies.

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(20)

        def reply(self, status: int, payload: Any) -> None:
            data = json.dumps(payload, default=_json_default, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def handle_api(self) -> None:
            try:
                credential = self.headers.get("Authorization", "")
                if not hmac.compare_digest(
                    credential.encode(), ("Bearer " + factory.token).encode()
                ):
                    raise Refused(401, "factory backend token required")
                if self.headers.get("Transfer-Encoding"):
                    raise Refused(400, "chunked requests are not supported")
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 <= length <= MAX_BODY:
                    raise Refused(413, "request exceeds backend limit")
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ValueError("incomplete request body")
                body = json.loads(raw) if raw else {}
                if not isinstance(body, dict):
                    raise ValueError("request must be a JSON object")
                mount = PREFIX + "/airflow/api/v2"
                if self.path.startswith(mount + "/"):
                    status, payload = factory.compatibility(
                        self.command, self.path[len(mount) :], body
                    )
                elif self.command == "GET" and self.path == PREFIX + "/health":
                    status, payload = 200, {
                        "service": "swfactory",
                        "api_version": 1,
                        "cell_schema_version": SCHEMA_VERSION,
                        "mutation_ready": True,
                    }
                elif self.command == "POST" and self.path.startswith(PREFIX + "/"):
                    status, payload = 200, factory.operation(self.path[len(PREFIX) :], body)
                else:
                    raise Refused(404, "unknown factory route")
            except Refused as error:
                status, payload = error.status, {"detail": str(error)}
            except PermissionError:
                status, payload = (
                    403,
                    {"detail": "worker ownership or factory-name check refused removal"},
                )
            except FileNotFoundError:
                status, payload = 404, {"detail": "backend resource or required tool not found"}
            except (ValueError, TypeError) as error:
                status, payload = 400, {"detail": str(error)[:300]}
            except (ControlError, OSError, subprocess.SubprocessError):
                status, payload = (
                    502,
                    {"detail": "backend service unavailable; mutation outcome may be unknown"},
                )
            self.reply(status, payload)

        do_GET = handle_api
        do_POST = handle_api
        do_PATCH = handle_api

    return ThreadingHTTPServer((host, port), Handler)


def serve(host: str = "127.0.0.1", port: int = 8082) -> None:
    factory = Factory(
        token=os.getenv("SWF_BACKEND_TOKEN", ""),
        airflow_url=os.getenv("AIRFLOW_URL", "http://localhost:8080"),
        repo=os.getenv("SWF_REPO", ""),
        owner=os.getenv("SWF_SANDBOX_OWNER", ""),
        root=Path(os.getenv("SWF_METRICS_ROOT", ".")),
        state_root=Path(os.getenv("SWF_STATE_ROOT", ".factory")),
    )
    try:
        with make_server(factory, host, port) as server:
            server.serve_forever()
    finally:
        factory.cell_store.close()
