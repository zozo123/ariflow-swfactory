"""HTTP transport for the stabilized factory backend.

Only minimal liveness/readiness probes are unauthenticated. Every control/read API containing
factory state requires the backend bearer token. Ordinary JSON requests stay small; only the two
managed SCM routes that intentionally carry patch/issue bodies may use the larger authenticated
limit. Mutating compatibility submissions are drain-fenced at the HTTP boundary as defense in
depth, while the shared Factory.submit use case remains the canonical readiness guard.
"""

from __future__ import annotations

import dataclasses
import hmac
import json
import os
import subprocess
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from swfactory.control import ControlError

from .core_service import operation as core_operation
from .scm_service import operation as scm_operation
from .service import Factory, Refused

PREFIX = "/v1"
MAX_BODY = 64 * 1024
MAX_SCM_BODY = 16 * 1024 * 1024
LARGE_SCM_ROUTES = {"/v1/scm/publish", "/v1/scm/open-issue"}


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def make_server(factory: Factory, host: str = "127.0.0.1", port: int = 8082) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        server_version = "swfactory-backend/2"

        def log_message(self, *_args: Any) -> None:
            pass

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(30)

        def reply(self, status: int, payload: Any) -> None:
            data = json.dumps(payload, default=_json_default, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _public_probe(self) -> bool:
            if self.command != "GET":
                return False
            if self.path == PREFIX + "/liveness":
                self.reply(200, {"service": "swfactory", "live": True, "api_version": 1})
                return True
            if self.path == PREFIX + "/readiness":
                document = factory.capabilities()
                status = 200 if document.get("read_ready") else 503
                self.reply(
                    status,
                    {
                        "service": "swfactory",
                        "read_ready": bool(document.get("read_ready")),
                        "mutation_ready": bool(document.get("mutation_ready")),
                        "serving_generation": document.get("serving_generation"),
                    },
                )
                return True
            return False

        def _authenticate(self) -> None:
            credential = self.headers.get("Authorization", "")
            if not hmac.compare_digest(credential.encode(), ("Bearer " + factory.token).encode()):
                raise Refused(401, "factory backend token required")

        def _body_limit(self) -> int:
            if self.command == "POST" and self.path in LARGE_SCM_ROUTES:
                return MAX_SCM_BODY
            return MAX_BODY

        def _read_body(self) -> dict[str, Any]:
            if self.headers.get("Transfer-Encoding"):
                raise Refused(400, "chunked requests are not supported")
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise Refused(400, "invalid Content-Length") from exc
            if not 0 <= length <= self._body_limit():
                raise Refused(413, "request exceeds backend limit")
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError("incomplete request body")
            body = json.loads(raw) if raw else {}
            if not isinstance(body, dict):
                raise ValueError("request must be a JSON object")
            return body

        def _health(self) -> tuple[int, dict[str, Any]]:
            document = factory.capabilities()
            return 200, {
                "service": "swfactory",
                "api_version": document["contracts"]["api"],
                "cell_schema_version": document["contracts"]["cell"],
                "mutation_ready": document["mutation_ready"],
            }

        def _compatibility(self, body: dict[str, Any]) -> tuple[int, Any]:
            mount = PREFIX + "/airflow/api/v2"
            path = self.path[len(mount) :]
            if (
                self.command == "POST"
                and path.startswith("/dags/")
                and path.endswith("/dagRuns")
                and not factory.capabilities().get("mutation_ready")
            ):
                raise Refused(503, "backend is draining or not mutation-ready")
            return factory.compatibility(self.command, path, body)

        def handle_api(self) -> None:
            try:
                if self._public_probe():
                    return
                self._authenticate()
                body = self._read_body()
                mount = PREFIX + "/airflow/api/v2"
                if self.command == "GET" and self.path == PREFIX + "/health":
                    status, payload = self._health()
                elif self.path.startswith(mount + "/"):
                    status, payload = self._compatibility(body)
                elif self.command == "POST" and self.path.startswith(PREFIX + "/scm/"):
                    status, payload = 200, scm_operation(factory, self.path[len(PREFIX) :], body)
                elif self.command == "POST" and self.path.startswith(PREFIX + "/core/"):
                    status, payload = 200, core_operation(factory, self.path[len(PREFIX) :], body)
                elif self.command == "POST" and self.path.startswith(PREFIX + "/"):
                    status, payload = 200, factory.operation(self.path[len(PREFIX) :], body)
                else:
                    raise Refused(404, "unknown factory route")
            except Refused as error:
                status, payload = error.status, {"detail": str(error)}
            except PermissionError:
                status, payload = 403, {"detail": "factory policy refused the operation"}
            except FileNotFoundError:
                status, payload = 404, {"detail": "backend resource or required tool not found"}
            except (ValueError, TypeError) as error:
                status, payload = 400, {"detail": str(error)[:500]}
            except (ControlError, OSError, subprocess.SubprocessError):
                status, payload = 502, {"detail": "backend service unavailable; mutation outcome may be unknown"}
            except Exception:
                status, payload = 500, {"detail": "internal backend error"}
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
        owner=os.getenv("SWF_SANDBOX_OWNER") or os.getenv("SWF_OWNER", ""),
        root=Path(os.getenv("SWF_METRICS_ROOT", ".")),
        state_root=Path(os.getenv("SWF_STATE_ROOT", ".factory")),
    )
    try:
        with make_server(factory, host, port) as server:
            server.serve_forever()
    finally:
        factory.close()
