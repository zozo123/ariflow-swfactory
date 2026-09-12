"""Pinned HTTP/SSE contract, over in-memory byte streams and real Unix sockets when permitted."""

from __future__ import annotations

import base64
import http.client
import io
import json
import queue
import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler
from types import SimpleNamespace

import pytest

from swfactory.config import Config
from swfactory.sandbox import load_toolset_backend, make_sandbox
from swfactory.sandbox_contract import toolset_document
from swfactory.smolvm_backend import SmolvmError, SmolvmSandboxBackend
from swfactory.state import RunState

NAME = "swf-smol-" + "a" * 32


def sse(event, data):
    return (f"event: {event}\n" + "".join(f"data: {line}\n" for line in data.split("\n")) + "\n").encode()


@pytest.fixture(params=["memory", "unix"])
def daemon(tmp_path, monkeypatch, request):
    fake = SimpleNamespace(machines={}, calls=[], uploads={}, stream=None, create_hook=None, delete_status=200)

    fake.health = {"status": "ok", "version": "test"}
    fake.health_status = 200
    fake.ready_status = 200

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def handle_request(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            data = json.loads(body) if body and self.headers.get("Content-Type") == "application/json" else body
            fake.calls.append((self.command, self.path, data))
            path = self.path.removeprefix("/api/v1/machines").strip("/")
            name = path.split("/")[0]
            status, result = 200, {}
            if self.path == "/health":
                status, result = fake.health_status, fake.health
            elif self.path == "/readyz":
                status = fake.ready_status
            elif self.command == "POST" and not path:
                if fake.create_hook:
                    fake.create_hook(data)
                if data["name"] in fake.machines:
                    status = 409
                else:
                    assert all(set(e) == {"name", "value"} for e in data.get("env", []))
                    fake.machines[data["name"]] = {**data, "state": "created"}
                    result = fake.machines[data["name"]]
            elif name not in fake.machines:
                status = 404
            elif self.command == "GET":
                result = fake.machines[name]
            elif path.endswith("/start"):
                fake.machines[name].update(state="running", pid=123)
                result = fake.machines[name]
            elif path.endswith("/exec/stream"):
                assert set(data) == {"command", "timeoutSecs", "env"}
                assert data["command"][:2] == ["/bin/sh", "-c"]
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                try:
                    if fake.stream:
                        fake.stream(self, data)
                    else:
                        self.wfile.write(sse("stdout", "ok\n") + sse("exit", '{"exitCode":0}'))
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
            elif self.command == "PUT":
                fake.uploads[path] = body
            elif self.command == "DELETE":
                status = fake.delete_status
                if status == 200:
                    del fake.machines[name]
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            encoded = json.dumps(result).encode()
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        do_GET = do_POST = do_PUT = do_DELETE = handle_request

    if request.param == "memory":
        # Exercise the stdlib HTTP request/response parser even in runners that forbid sockets.
        # Only the byte transport is substituted; the backend and server handlers are unchanged.
        errors = []

        class Reader(io.RawIOBase):
            def __init__(self, peer):
                self.peer, self.pending, self.ended = peer, b"", False

            def readable(self):
                return True

            def readinto(self, target):
                if self.ended:
                    return 0
                if not self.pending:
                    try:
                        self.pending = self.peer.chunks.get(timeout=self.peer.timeout)
                    except queue.Empty:
                        raise TimeoutError() from None
                    if self.pending is None:
                        self.ended = True
                        return 0
                size = min(len(target), len(self.pending))
                target[:size] = self.pending[:size]
                self.pending = self.pending[size:]
                return size

        class Peer:
            def __init__(self):
                self.request = bytearray()
                self.chunks = queue.Queue()
                self.timeout = 30

            def sendall(self, data):
                self.request.extend(data)

            def settimeout(self, timeout):
                self.timeout = timeout

            def makefile(self, *_):
                return io.BufferedReader(Reader(self))

            def close(self):
                pass  # The response's file still owns the read side.

            def shutdown(self, *_):
                self.chunks.put(None)

        class Connection(http.client.HTTPConnection):
            def __init__(self, _path, timeout):
                super().__init__("localhost", timeout=timeout)

            def connect(self):
                self.sock = Peer()

            def getresponse(self):
                peer = self.sock

                class Writer:
                    def write(self, data):
                        peer.chunks.put(bytes(data))

                    def flush(self):
                        pass

                def serve():
                    handler = Handler.__new__(Handler)
                    handler.rfile = io.BytesIO(peer.request)
                    handler.wfile = Writer()
                    handler.client_address = ("local", 0)
                    try:
                        handler.handle()
                    except Exception as error:
                        errors.append(error)
                    finally:
                        peer.chunks.put(None)

                threading.Thread(target=serve, daemon=True).start()
                return super().getresponse()

        monkeypatch.setattr("swfactory.smolvm_backend._UnixHTTPConnection", Connection)
        fake.socket = "/run/test-smolvm/api.sock"
        yield fake
        assert not errors
        return

    class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
        daemon_threads = True

    # Linux limits sockaddr_un to 108 bytes; pytest's full tmp path can exceed it.
    import tempfile

    with tempfile.TemporaryDirectory(prefix="swf-smol-") as directory:
        fake.socket = directory + "/api.sock"
        try:
            server = Server(fake.socket, Handler)
        except PermissionError:
            pytest.skip("runner forbids Unix sockets; in-memory HTTP transport cases still run")
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        yield fake
        server.shutdown()
        server.server_close()
        thread.join()


def backend(daemon, tmp_path, **kwargs):
    return SmolvmSandboxBackend(socket_path=daemon.socket, state=RunState(tmp_path), machine_name=NAME, **kwargs)


def spec(**kwargs):
    return SimpleNamespace(**{"env": {}, "block_network": True, "allow_egress_to": (), **kwargs})


def test_provision_journal_precedes_effect_and_contains_no_credentials(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    daemon.create_hook = lambda data: be._load(data["name"])["phase"] == "pending" or pytest.fail("missing intent")
    assert be.create(spec=spec(env={"MODEL_KEY": "secret-model-value"})) == NAME
    sent = daemon.calls[0][2]
    assert sent["network"] is False and sent["mounts"] == [] and sent["ports"] == []
    assert sent["env"] == [{"name": "MODEL_KEY", "value": "secret-model-value"}]
    assert "secret-model-value" not in be.state.read_control(f"smolvm/{NAME}.json")
    assert be.run_command(NAME, "printf ok", timeout=2, max_output_bytes=100).stdout == "ok\n"
    assert daemon.calls[-1][2]["env"] == sent["env"]
    be.destroy(NAME)
    be.destroy(NAME)
    assert not daemon.machines


def test_network_allowlist_and_configuration_roundtrip(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec(allow_egress_to=["pypi.org"]))
    assert daemon.machines[NAME]["allowedHosts"] == ["pypi.org"]
    assert daemon.machines[NAME]["allowedCidrs"] == []
    daemon.machines[NAME]["allowedHosts"] = ["evil.example"]
    with pytest.raises(SmolvmError, match="did not enforce"):
        be.run_command(NAME, "echo never", timeout=1, max_output_bytes=10)
    assert not any(path.endswith("/exec/stream") for _, path, _ in daemon.calls)


@pytest.mark.parametrize("hosts", [["*.example.org"], ["example.org:443"], ["https://example.org"], ["a..b"]])
def test_invalid_network_policy_refused_before_provisioning(daemon, tmp_path, hosts):
    with pytest.raises(SmolvmError, match="egress"):
        backend(daemon, tmp_path).create(spec=spec(allow_egress_to=hosts))
    assert not daemon.calls


def test_worker_restart_reconnects_without_create(daemon, tmp_path):
    backend(daemon, tmp_path).create(spec=spec())
    restarted = backend(daemon, tmp_path)
    assert restarted.create(spec=spec()) == NAME
    assert restarted.run_command(NAME, "true", timeout=1, max_output_bytes=10).exit_code == 0
    assert sum(path == "/api/v1/machines/" for _, path, _ in daemon.calls) == 1


def test_interrupted_exec_is_deleted_never_replayed(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())
    record = be._load(NAME)
    record["inflight"] = True
    be._save(NAME, record)
    with pytest.raises(SmolvmError, match="interrupted"):
        backend(daemon, tmp_path).run_command(NAME, "never replay", timeout=1, max_output_bytes=10)
    assert NAME not in daemon.machines
    assert not any(path.endswith("/exec/stream") for _, path, _ in daemon.calls)


def test_missing_or_restarted_machine_is_not_auto_started(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())
    daemon.machines[NAME]["pid"] = 456
    with pytest.raises(SmolvmError, match="incarnation"):
        be.run_command(NAME, "true", timeout=1, max_output_bytes=10)
    del daemon.machines[NAME]
    with pytest.raises(SmolvmError, match="incarnation"):
        be.run_command(NAME, "true", timeout=1, max_output_bytes=10)
    assert sum(path.endswith("/start") for _, path, _ in daemon.calls) == 1


def test_ambiguous_create_reconciles_without_second_post(daemon, tmp_path, monkeypatch):
    be = backend(daemon, tmp_path)
    original = be._json

    def lose_response(method, path, *args, **kwargs):
        result = original(method, path, *args, **kwargs)
        if path == "/machines/":
            raise TimeoutError("lost response")
        return result

    monkeypatch.setattr(be, "_json", lose_response)
    with pytest.raises(TimeoutError):
        be.create(spec=spec())
    assert backend(daemon, tmp_path).create(spec=spec()) == NAME
    assert sum(path == "/api/v1/machines/" for _, path, _ in daemon.calls) == 1


def test_pending_absence_cannot_be_replayed_or_claimed_clean(daemon, tmp_path, monkeypatch):
    be = backend(daemon, tmp_path)
    monkeypatch.setattr(be, "_json", lambda *_a, **_k: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(TimeoutError):
        be.create(spec=spec())
    restarted = backend(daemon, tmp_path)
    with pytest.raises(SmolvmError, match="in doubt"):
        restarted.create(spec=spec())
    with pytest.raises(SmolvmError, match="in doubt"):
        restarted.destroy(NAME)


def test_name_collision_is_not_adopted_or_deleted(daemon, tmp_path):
    daemon.machines[NAME] = {"name": NAME}
    be = backend(daemon, tmp_path)
    with pytest.raises(SmolvmError, match="collision"):
        be.create(spec=spec())
    with pytest.raises(SmolvmError, match="colliding"):
        be.destroy(NAME)
    assert NAME in daemon.machines


def test_failed_cleanup_stays_fenced_and_can_be_retried(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())
    daemon.delete_status = 500
    with pytest.raises(SmolvmError, match="cleanup failed"):
        be.destroy(NAME)
    assert be._load(NAME)["phase"] == "terminated"
    with pytest.raises(SmolvmError, match="not ready"):
        be.run_command(NAME, "true", timeout=1, max_output_bytes=10)
    daemon.delete_status = 200
    backend(daemon, tmp_path).destroy(NAME)
    assert not daemon.machines


@pytest.mark.parametrize("payload", [b"", sse("error", "provider failed"), sse("exit", '{"exitCode":"0"}')])
def test_incomplete_error_or_malformed_stream_never_succeeds(daemon, tmp_path, payload):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())
    daemon.stream = lambda handler, _data: handler.wfile.write(payload)
    with pytest.raises(SmolvmError, match="execution failed"):
        be.run_command(NAME, "true", timeout=1, max_output_bytes=10)
    assert not daemon.machines


def test_output_is_bounded_per_stream_and_exit_is_preserved(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())
    daemon.stream = lambda h, _: h.wfile.write(
        sse("stdout", "a" * 100_000) + sse("stderr", "failure\n") + sse("exit", '{"exitCode":7}')
    )
    result = be.run_command(NAME, "test", timeout=2, max_output_bytes=10)
    assert result.exit_code == 7 and result.stdout == "a" * 10 and result.stdout_truncated
    assert result.stderr == "failure\n" and not result.stderr_truncated


def test_trickling_stream_cannot_extend_deadline(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())

    def trickle(h, _):
        for _ in range(100):
            h.wfile.write(b": keepalive\n\n")
            h.wfile.flush()
            time.sleep(0.01)

    daemon.stream = trickle
    started = time.monotonic()
    result = be.run_command(NAME, "sleep forever", timeout=0.15, max_output_bytes=10)
    assert result.timed_out and result.sandbox_terminated
    assert time.monotonic() - started < 1.5
    assert not daemon.machines


def test_exit_124_conservatively_terminates_attempt(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())
    daemon.stream = lambda h, _: h.wfile.write(sse("exit", '{"exitCode":124}'))
    result = be.run_command(NAME, "exit 124", timeout=1, max_output_bytes=10)
    assert result.timed_out and result.sandbox_terminated
    assert not daemon.machines


def test_late_success_cannot_resurrect_cancelled_attempt(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())

    def cancel_then_reply(handler, _):
        be.destroy(NAME)
        handler.wfile.write(sse("exit", '{"exitCode":0}'))

    daemon.stream = cancel_then_reply
    with pytest.raises(SmolvmError, match="execution failed"):
        be.run_command(NAME, "work", timeout=2, max_output_bytes=10)
    assert be._load(NAME)["phase"] == "deleted"


def test_bounded_file_read_and_binary_upload(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.create(spec=spec())
    content = b"\x00\xff\n"
    be.write_file(NAME, "/tmp/space name", content)
    assert daemon.uploads[NAME + "/files/tmp/space%20name"] == content
    daemon.stream = lambda h, _: h.wfile.write(
        sse("stdout", base64.b64encode(content).decode()) + sse("exit", '{"exitCode":0}')
    )
    assert be.read_file(NAME, "/tmp/space name", max_bytes=3) == content
    with pytest.raises(SmolvmError, match="exceeds max_bytes"):
        be.read_file(NAME, "/tmp/space name", max_bytes=2)
    command = daemon.calls[-1][2]["command"][2]
    assert "head -c 3" in command and "pipefail" in command
    with pytest.raises(ValueError, match="traversal"):
        be.write_file(NAME, "/tmp/../secret", b"no")


def test_one_backend_keeps_sandbox_credentials_separate(daemon):
    be = SmolvmSandboxBackend(socket_path=daemon.socket)
    first = be.create(spec=spec(env={"KEY": "one"}))
    second = be.create(spec=spec(env={"KEY": "two"}))
    be.run_command(first, "true", timeout=1, max_output_bytes=10)
    assert daemon.calls[-1][2]["env"] == [{"name": "KEY", "value": "one"}]
    be.run_command(second, "true", timeout=1, max_output_bytes=10)
    assert daemon.calls[-1][2]["env"] == [{"name": "KEY", "value": "two"}]


def test_factory_wires_durable_backend_without_airflow_import(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "only-model-credential")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "must-not-enter")
    cfg = Config(
        issue="42",
        sandbox="toolset",
        agent="claude",
        toolset_backend="smolvm",
        toolset_smolvm_socket="/run/private/api.sock",
    )
    sb = make_sandbox(cfg, "42", run_dir=tmp_path)
    assert isinstance(sb.backend, SmolvmSandboxBackend)
    assert sb.backend.state.root == RunState(tmp_path).root
    assert sb.backend.env == {"ANTHROPIC_API_KEY": "only-model-credential"}
    assert sb.backend.socket_path == "/run/private/api.sock"
    assert sb.backend.machine_name == make_sandbox(cfg, "42", run_dir=tmp_path).backend.machine_name
    assert sb.backend.machine_name != make_sandbox(cfg, "43", run_dir=tmp_path).backend.machine_name
    assert isinstance(load_toolset_backend("smolvm"), SmolvmSandboxBackend)
    caps = toolset_document("smolvm").capabilities
    assert not caps.ttl and not caps.fork and not caps.snapshot


def test_cleanup_refuses_unjournalled_or_different_endpoint_handle(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    with pytest.raises(SmolvmError, match="host-owned record"):
        be.destroy(NAME)
    be.create(spec=spec())
    other = SmolvmSandboxBackend(socket_path="/some/other.sock", state=RunState(tmp_path))
    with pytest.raises(SmolvmError, match="host-owned record"):
        other.destroy(NAME)


def test_released_airflow_toolset_consumes_backend_result(daemon, tmp_path, monkeypatch):
    import asyncio

    module = pytest.importorskip("airflow.providers.common.ai.toolsets.sandbox")
    be = backend(daemon, tmp_path)
    be.create(spec=spec())

    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    # No event loop/socketpair required: this coroutine has only the to_thread await.
    monkeypatch.setattr(asyncio, "to_thread", inline)
    toolset = module.SandboxToolset(be)
    coroutine = toolset._run_command(NAME, {"command": "printf ok"})
    with pytest.raises(StopIteration) as result:
        coroutine.send(None)
    assert "ok" in result.value.value and "[stdout]" in result.value.value


def test_readiness_is_read_only_and_uses_root_endpoints(daemon, tmp_path):
    be = backend(daemon, tmp_path)
    be.check_ready()
    assert [(method, path) for method, path, _ in daemon.calls] == [("GET", "/health"), ("GET", "/readyz")]
    assert daemon.machines == {} and be._records == {}


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("health_status", 503, "health returned HTTP 503"),
        ("health", {"status": "broken"}, "not healthy"),
        ("health", [], "not healthy"),
        ("ready_status", 503, "readiness returned HTTP 503"),
    ],
)
def test_readiness_rejects_unhealthy_daemon(daemon, tmp_path, field, value, message):
    setattr(daemon, field, value)
    with pytest.raises(SmolvmError, match=message):
        backend(daemon, tmp_path).check_ready()
    assert all(method == "GET" for method, _, _ in daemon.calls)


def test_readiness_shares_one_deadline(monkeypatch):
    from contextlib import contextmanager

    deadlines = []
    clock = [100.0]
    monkeypatch.setattr("swfactory.smolvm_backend.time.monotonic", lambda: clock[0])

    @contextmanager
    def response(method, path, body, deadline, *, root):
        deadlines.append(deadline)
        assert method == "GET" and root and body is None
        if path == "/health":
            clock[0] += 4
            yield SimpleNamespace(status=200, read=lambda _: b'{"status":"ok"}'), None
        else:
            clock[0] += 2
            yield SimpleNamespace(status=200), None

    be = SmolvmSandboxBackend()
    monkeypatch.setattr(be, "_response", response)
    with pytest.raises(TimeoutError, match="deadline"):
        be.check_ready(timeout=5)
    assert deadlines == [105, 105]
