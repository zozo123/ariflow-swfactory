"""Experimental, structurally Airflow-compatible SmolVM SandboxBackend.

Use a host-owned Unix socket; neither that socket nor the host journal enters the guest.
Imports stay independent of Airflow so DAG parsing and the normal factory install stay cheap.
Ordinary machines have no TTL. Ambiguous commands are fenced, never replayed in place.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import math
import re
import shlex
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from swfactory.models import StageError
from swfactory.state import RunState

_CONTROL_LIMIT = 1024 * 1024
_EVENT_LIMIT = 2 * 1024 * 1024
_FILE_LIMIT = 8 * 1024 * 1024
_NAME = re.compile(r"swf-smol-[a-f0-9]{32}\Z")
_HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\Z")


class SmolvmError(StageError):
    def __init__(self, message: str) -> None:
        super().__init__("sandbox", message, retryable=False)


@dataclass(frozen=True)
class SmolvmExecResult:
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    sandbox_terminated: bool = False


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class SmolvmSandboxBackend:
    """Create/exec/file/destroy contract consumed by Airflow and ToolsetSandbox.

    ``state`` enables durable recovery; callers must serialize operations with the run lock.
    Each factory run binds a unique ``machine_name`` before any provisioning request.
    Standalone callers without state must arrange orphan cleanup after a process crash.
    """

    name = "smolvm"

    def __init__(
        self,
        *,
        socket_path: str = "/run/smolvm/api.sock",
        image: str = "ghcr.io/zozo123/swfactory-sandbox:latest",
        cpus: int = 2,
        memory_mb: int = 2048,
        create_timeout: float = 240,
        state: RunState | None = None,
        machine_name: str | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        if not Path(socket_path).is_absolute() or "\x00" in socket_path:
            raise ValueError("SmolVM socket_path must be absolute")
        if not image or "\x00" in image:
            raise ValueError("SmolVM image must be nonempty")
        if type(cpus) is not int or not 1 <= cpus <= 255:
            raise ValueError("SmolVM cpus must be between 1 and 255")
        if type(memory_mb) is not int or not 128 <= memory_mb <= 2**32 - 1:
            raise ValueError("SmolVM memory_mb must be between 128 and 2**32-1")
        if not math.isfinite(create_timeout) or not 0 < create_timeout <= 240:
            raise ValueError("SmolVM create_timeout must be in (0, 240]")
        if machine_name is not None and not _NAME.fullmatch(machine_name):
            raise ValueError("invalid SmolVM machine identity")
        self.socket_path = socket_path
        self.image = image
        self.cpus = cpus
        self.memory_mb = memory_mb
        self.create_timeout = create_timeout
        self.state = state
        self.machine_name = machine_name
        self.env = dict(env or {})
        self._envs: dict[str, dict[str, str]] = {}
        self._records: dict[str, dict[str, Any]] = {}
        self._journal_lock = threading.RLock()

    @contextmanager
    def _response(self, method: str, path: str, body: Any, deadline: float) -> Iterator[Any]:
        conn = _UnixHTTPConnection(self.socket_path, self._remaining(deadline))
        data = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/octet-stream" if isinstance(body, bytes) else "application/json"}
        response = None
        timer = None
        try:
            conn.connect()
            sock = conn.sock

            def expire() -> None:
                with suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)

            # Socket inactivity timeouts alone do not bound a trickling response.
            timer = threading.Timer(self._remaining(deadline), expire)
            timer.daemon = True
            timer.start()
            conn.request(method, "/api/v1" + path, body=data, headers=headers)
            response = conn.getresponse()

            def read_line() -> bytes:
                sock.settimeout(self._remaining(deadline))
                line = response.readline(_EVENT_LIMIT + 1)
                if len(line) > _EVENT_LIMIT:
                    raise SmolvmError("SmolVM SSE line exceeds the transport limit")
                return line

            yield response, read_line
        finally:
            if timer is not None:
                timer.cancel()
            if response is not None:
                response.close()
            conn.close()

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("SmolVM operation deadline exceeded")
        return remaining

    def _json(self, method: str, path: str, body: Any = None, *, timeout: float = 30) -> tuple[int, dict]:
        with self._response(method, path, body, time.monotonic() + timeout) as (response, _):
            data = response.read(_CONTROL_LIMIT + 1)
            if len(data) > _CONTROL_LIMIT:
                raise SmolvmError("SmolVM control response exceeds limit")
            if response.status >= 400:
                # Do not echo server bodies, which can contain submitted environment values.
                return response.status, {}
            value = json.loads(data) if data else {}
            if not isinstance(value, dict):
                raise SmolvmError("invalid SmolVM control response")
            return response.status, value

    def _save(self, name: str, record: dict) -> None:
        with self._journal_lock:
            if self.state is not None:
                self.state.write_control(f"smolvm/{name}.json", json.dumps(record, sort_keys=True))
            self._records[name] = dict(record)

    def _finish(self, name: str) -> None:
        # Cancellation may have destroyed the VM while a command/upload response was in flight.
        # Never let that late response overwrite a terminal cleanup record with "ready".
        with self._journal_lock:
            record = self._load(name)
            if record["phase"] != "ready":
                raise SmolvmError("SmolVM was terminated during the operation")
            record["inflight"] = False
            self._save(name, record)

    def _load(self, name: str) -> dict:
        if not _NAME.fullmatch(name):
            raise SmolvmError("invalid SmolVM handle")
        if self.state is not None and self.state.has_control(f"smolvm/{name}.json"):
            record = json.loads(self.state.read_control(f"smolvm/{name}.json"))
        else:
            record = self._records.get(name)
        if not isinstance(record, dict) or record.get("name") != name or record.get("socket") != self.socket_path:
            raise SmolvmError("SmolVM handle has no matching host-owned record")
        return dict(record)

    def _expected(self, spec: Any) -> dict:
        hosts = sorted(set(spec.allow_egress_to or ())) if spec is not None else []
        if any(not isinstance(host, str) or not _HOST.fullmatch(host) or ".." in host for host in hosts):
            raise SmolvmError("SmolVM egress requires lowercase DNS hostnames, without ports or wildcards")
        blocked = spec is None or spec.block_network
        if hosts and not blocked:
            raise SmolvmError("egress allowlist requires block_network=True")
        expected: dict[str, Any] = {
            "image": self.image,
            "cpus": self.cpus,
            "memoryMb": self.memory_mb,
            "network": not blocked or bool(hosts),
            "mounts": [],
            "ports": [],
            "gpu": False,
            "cuda": False,
        }
        if blocked and hosts:
            expected.update(allowedHosts=hosts, allowedCidrs=[], networkBackend="virtio-net")
        return expected

    @staticmethod
    def _validate(info: dict, record: dict) -> None:
        if info.get("name") != record["name"]:
            raise SmolvmError("SmolVM returned another machine identity")
        # MachineInfo does not echo the full image/env configuration. Never adopt an unjournalled
        # machine, and compare every isolation field the API does report.
        for key, value in record["expected"].items():
            if key != "image" and info.get(key) != value:
                raise SmolvmError(f"SmolVM did not enforce {key}")

    def create(self, *, spec: Any = None) -> str:
        expected = self._expected(spec)
        env = dict(spec.env or {}) if spec is not None else dict(self.env)
        if any(
            not isinstance(k, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k)
            or not isinstance(v, str)
            or "\x00" in v
            for k, v in env.items()
        ):
            raise SmolvmError("invalid SmolVM environment")
        digest = hashlib.sha256(json.dumps([expected, env], sort_keys=True).encode()).hexdigest()
        name = self.machine_name or f"swf-smol-{uuid.uuid4().hex}"
        self._envs[name] = env
        exists = name in self._records or (self.state is not None and self.state.has_control(f"smolvm/{name}.json"))
        if exists:
            record = self._load(name)
            if record.get("digest") != digest:
                raise SmolvmError("SmolVM provisioning policy changed; start a new factory run")
            if record["phase"] == "ready":
                self._ready(name)
                return name
            if record["phase"] != "pending":
                raise SmolvmError("SmolVM attempt is terminated; start a new factory run")
            status, info = self._json("GET", f"/machines/{name}")
            if status != 200:
                raise SmolvmError("SmolVM create remains in doubt; reconcile the recorded name before a new run")
        else:
            record = {
                "name": name,
                "socket": self.socket_path,
                "expected": expected,
                "digest": digest,
                "phase": "pending",
                "inflight": False,
            }
            self._save(name, record)  # Before POST: worker death cannot erase the cleanup identity.
            status, info = self._json(
                "POST",
                "/machines/",
                {**expected, "name": name, "env": [{"name": k, "value": v} for k, v in env.items()]},
                timeout=self.create_timeout,
            )
            if status == 409:
                record["phase"] = "collision"
                self._save(name, record)
                raise SmolvmError("SmolVM name collision; refusing to adopt or delete the existing machine")
            if status not in (200, 201):
                raise SmolvmError(f"SmolVM create failed (HTTP {status}); reconcile the recorded name")
        self._validate(info, record)
        if info.get("state") != "running":
            status, info = self._json("POST", f"/machines/{name}/start", timeout=self.create_timeout)
            if status != 200:
                raise SmolvmError(f"SmolVM start failed (HTTP {status}); cleanup identity retained")
        self._validate(info, record)
        if info.get("state") != "running" or type(info.get("pid")) is not int:
            raise SmolvmError("SmolVM did not report a running incarnation")
        record.update(phase="ready", pid=info["pid"])
        self._save(name, record)
        return name

    def _ready(self, name: str) -> dict:
        record = self._load(name)
        if record["phase"] != "ready":
            raise SmolvmError("SmolVM attempt is not ready; start a new run after cleanup")
        if record.get("inflight"):
            self.destroy(name)
            raise SmolvmError("interrupted SmolVM operation was fenced; start a new factory run")
        status, info = self._json("GET", f"/machines/{name}")
        self._validate(info, record) if status == 200 else None
        if status != 200 or info.get("state") != "running" or info.get("pid") != record["pid"]:
            raise SmolvmError("SmolVM incarnation is unavailable or changed; refusing to restart it")
        return record

    def destroy(self, sandbox: str) -> None:
        record = self._load(sandbox)
        if record["phase"] == "deleted":
            return
        if record["phase"] == "collision":
            raise SmolvmError("refusing cleanup of a colliding SmolVM identity")
        status, info = self._json("GET", f"/machines/{sandbox}")
        if status == 200:
            self._validate(info, record)
        elif status != 404:
            raise SmolvmError("cannot observe SmolVM cleanup target; cleanup debt retained")
        elif record["phase"] == "pending":
            raise SmolvmError(
                "SmolVM creation is still in doubt; absence does not prove a delayed create cannot finish"
            )
        record["phase"] = "terminated"
        self._save(sandbox, record)  # Fence even if DELETE fails; keep cleanup debt durable.
        status, _ = self._json("DELETE", f"/machines/{sandbox}")
        if status not in (200, 204, 404):
            raise SmolvmError(f"SmolVM cleanup failed (HTTP {status}); retry cleanup of the recorded handle")
        status, _ = self._json("GET", f"/machines/{sandbox}")
        if status != 404:
            raise SmolvmError("SmolVM deletion is not confirmed; cleanup debt retained")
        record.update(phase="deleted", inflight=False)
        self._save(sandbox, record)

    def run_command(self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int) -> SmolvmExecResult:
        if not isinstance(command, str) or "\x00" in command:
            raise ValueError("command must be text without NUL")
        if not math.isfinite(timeout) or timeout <= 0 or type(max_output_bytes) is not int or max_output_bytes <= 0:
            raise ValueError("timeout and max_output_bytes must be positive")
        record = self._ready(sandbox)
        record["inflight"] = True
        self._save(sandbox, record)
        output = {"stdout": bytearray(), "stderr": bytearray()}
        truncated = {"stdout": False, "stderr": False}
        exit_code = None
        deadline = time.monotonic() + timeout
        try:
            body = {
                "command": ["/bin/sh", "-c", command],
                "timeoutSecs": max(1, math.ceil(timeout)),
                "env": [{"name": k, "value": v} for k, v in self._envs.get(sandbox, self.env).items()],
            }
            with self._response("POST", f"/machines/{sandbox}/exec/stream", body, deadline) as (response, read_line):
                if response.status != 200 or "text/event-stream" not in response.getheader("Content-Type", ""):
                    raise SmolvmError("SmolVM exec did not return an event stream")
                event, data, size = "", [], 0
                while True:
                    line = read_line()
                    if not line:
                        raise SmolvmError("SmolVM stream ended without a terminal exit event")
                    size += len(line)
                    if size > _EVENT_LIMIT:
                        raise SmolvmError("SmolVM SSE event exceeds the transport limit")
                    line = line.rstrip(b"\r\n")
                    if line:
                        if line.startswith(b"event:"):
                            event = line[6:].strip().decode("ascii")
                        elif line.startswith(b"data:"):
                            data.append(line[5:].removeprefix(b" "))
                        continue
                    payload = b"\n".join(data)
                    if event in output:
                        buf = output[event]
                        buf.extend(payload)
                        if len(buf) > max_output_bytes:
                            del buf[: len(buf) - max_output_bytes]
                            truncated[event] = True
                    elif event == "exit":
                        exit_code = json.loads(payload)["exitCode"]
                        if type(exit_code) is not int:
                            raise SmolvmError("invalid SmolVM exit code")
                        break
                    elif event == "error":
                        raise SmolvmError("SmolVM reported a streaming execution error")
                    elif event or data:
                        raise SmolvmError("unknown SmolVM stream event")
                    event, data, size = "", [], 0
            # SmolVM reserves 124 for timeout but cannot distinguish a guest's own exit 124.
            # Conservatively terminate this attempt for either case.
            if exit_code == 124:
                self.destroy(sandbox)
            else:
                self._finish(sandbox)
        except BaseException as error:
            try:
                self.destroy(sandbox)
            except Exception:
                raise SmolvmError(
                    "SmolVM execution failed and cleanup is unconfirmed; durable cleanup debt retained"
                ) from None
            if not isinstance(error, Exception):
                raise
            if isinstance(error, TimeoutError) or time.monotonic() >= deadline:
                return SmolvmExecResult(124, timed_out=True, sandbox_terminated=True)
            raise SmolvmError("SmolVM execution failed; attempt terminated, start a new factory run") from None
        return SmolvmExecResult(
            exit_code,
            output["stdout"].decode("utf-8", "replace"),
            output["stderr"].decode("utf-8", "replace"),
            timed_out=exit_code == 124,
            stdout_truncated=truncated["stdout"],
            stderr_truncated=truncated["stderr"],
            sandbox_terminated=exit_code == 124,
        )

    @staticmethod
    def _path(path: str) -> str:
        if not path.startswith("/") or "\x00" in path or ".." in path.split("/"):
            raise ValueError("file path must be absolute without traversal")
        return path

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        if type(max_bytes) is not int or not 0 <= max_bytes <= _FILE_LIMIT:
            raise ValueError(f"max_bytes must be between 0 and {_FILE_LIMIT}")
        path = shlex.quote(self._path(path))
        # Native GET buffers the full file on the server. Bound the read INSIDE the guest,
        # using exec's filesystem, including for procfs, FIFOs and concurrently growing files.
        script = f"test -f {path} || exit 66; head -c {max_bytes + 1} -- {path} | base64"
        result = self.run_command(
            sandbox,
            "bash -o pipefail -c " + shlex.quote(script),
            timeout=120,
            max_output_bytes=(max_bytes + 1) * 2 + 4096,
        )
        if result.exit_code == 66:
            raise FileNotFoundError("file is absent or not a regular file")
        if result.exit_code or result.stdout_truncated or result.stderr_truncated:
            raise SmolvmError("SmolVM file read failed or was truncated")
        raw = base64.b64decode("".join(result.stdout.split()), validate=True)
        if len(raw) > max_bytes:
            raise SmolvmError("SmolVM file exceeds max_bytes")
        return raw

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        path = self._path(path)
        if not isinstance(content, bytes) or len(content) > _FILE_LIMIT:
            raise ValueError(f"file content must be bytes, at most {_FILE_LIMIT} bytes")
        record = self._ready(sandbox)
        record["inflight"] = True
        self._save(sandbox, record)
        try:
            status, _ = self._json(
                "PUT", f"/machines/{sandbox}/files/{quote(path.lstrip('/'), safe='/')}", content, timeout=120
            )
            if status != 200:
                raise SmolvmError(f"SmolVM upload failed (HTTP {status})")
        except BaseException:
            self.destroy(sandbox)
            raise
        self._finish(sandbox)

    def list_directory(self, sandbox: str, path: str) -> list[tuple[str, bool]]:
        path = shlex.quote(self._path(path))
        script = f"find {path} -mindepth 1 -maxdepth 1 -printf '%f\\0%y\\0' | base64"
        result = self.run_command(
            sandbox, "bash -o pipefail -c " + shlex.quote(script), timeout=120, max_output_bytes=_CONTROL_LIMIT
        )
        if result.exit_code or result.stdout_truncated or result.stderr_truncated:
            raise SmolvmError("SmolVM directory listing failed or was truncated")
        fields = base64.b64decode("".join(result.stdout.split()), validate=True).decode().split("\0")
        return [(fields[i], fields[i + 1] == "d") for i in range(0, len(fields) - 1, 2)]
