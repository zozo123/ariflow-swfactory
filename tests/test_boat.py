"""boat.dev work cells over a fake client and a canned transport: no network, no VM, no key leak.

Two hermetic seams, the same split the factory already uses elsewhere: ``FakeBoatClient`` stands
in for the whole ``BoatClient`` protocol for the cell lifecycle (the ``tests/test_sandbox_argv.py``
``FakeBackend`` style), and a canned ``urllib`` opener pins the transport itself (the
``tests/test_linear_source.py`` style) -- the Bearer header, the base64 write, the 600 s command
cap, and the discipline that the key never enters an error or a response this module accepts.
"""

from __future__ import annotations

import base64
import io
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from swfactory import boat as boat_mod
from swfactory import doctor
from swfactory.blueprint import load
from swfactory.boat import (
    BOAT_DEFAULT_TYPE,
    BOAT_STATE_FILE,
    MISSING_KEY,
    BoatError,
    BoatSandbox,
    boat_client_from_env,
)
from swfactory.config import Config
from swfactory.sandbox import make_sandbox
from swfactory.state import RunState

ROOT = Path(__file__).resolve().parents[1]
# A synthetic key: only ever a value in memory, never something a real account answers to.
KEY = "boat_synthetic_key_do_not_print"
REPO = "jop8281/zozo123-genworld"
WORKDIR = "/workspace/zozo123-genworld/code"


def _cfg(**kw: object) -> Config:
    return Config(  # type: ignore[arg-type]
        **{
            "issue": "YOS-103",
            "sandbox": "boat",
            "agent": "scripted",
            "repo": REPO,
            "target_dir": "code",
            "base_branch": "stabilize/main",
            "run_id": "boatrun1",
            **kw,
        }
    )


class FakeBoatClient:
    """The whole ``BoatClient`` surface with an in-memory VM; records every call it receives."""

    def __init__(self, *, fail_on: str | None = None, rc: int = 0, timed_out: bool = False) -> None:
        self.created: list[dict] = []
        self.waited_ready: list[str] = []
        self.commands: list[tuple[str, str, str | None, int | None]] = []
        self.files: dict[str, bytes] = {}
        self.stopped: list[str] = []
        self.waited_stopped: list[str] = []
        self.stop_errors: list[str] = []  # sandbox ids whose stop raises once
        self.next_id = 0
        self.fail_on = fail_on  # any command carrying this text fails (rc 1)
        self.rc = rc
        self.timed_out = timed_out
        self.cloned = False  # an empty VM: nothing is a work tree until the clone ran

    def create(self, *, ttl_s: int, machine_type: str = BOAT_DEFAULT_TYPE) -> str:
        self.created.append({"machine_type": machine_type, "ttl_s": ttl_s})
        self.next_id += 1
        return f"boat-{self.next_id}"

    def wait_ready(self, sandbox_id: str) -> None:
        self.waited_ready.append(sandbox_id)

    def exec(self, sandbox_id: str, command: str, *, cwd: str | None = None, timeout_s: int | None = None):
        self.commands.append((sandbox_id, command, cwd, timeout_s))
        if self.fail_on and self.fail_on in command:
            return boat_mod.BoatExecResult(exit_code=1, stdout="", stderr=f"boom: {self.fail_on}")
        if command == "git rev-parse --is-inside-work-tree":
            return boat_mod.BoatExecResult(exit_code=0 if self.cloned else 1)
        if command.startswith("git clone"):
            self.cloned = True
            return boat_mod.BoatExecResult(exit_code=0)
        if command.startswith("test -d "):
            return boat_mod.BoatExecResult(exit_code=0 if self.cloned else 1)
        if command.startswith("test -e "):
            # ``exists`` probes with the absolute path; only written files are there.
            return boat_mod.BoatExecResult(exit_code=0 if command.removeprefix("test -e ") in self.files else 1)
        return boat_mod.BoatExecResult(exit_code=self.rc, stdout="out", stderr="", timed_out=self.timed_out)

    def read_file(self, sandbox_id: str, path: str) -> str:
        if path not in self.files:
            raise BoatError("boat.dev read file failed with HTTP 404", status=404)
        return self.files[path].decode("utf-8")

    def write_file(self, sandbox_id: str, path: str, content: bytes) -> None:
        self.files[path] = content

    def stop(self, sandbox_id: str) -> None:
        if sandbox_id in self.stop_errors:
            self.stop_errors.remove(sandbox_id)
            raise BoatError("boat.dev stop failed with HTTP 503", status=503)
        self.stopped.append(sandbox_id)

    def wait_stopped(self, sandbox_id: str) -> None:
        self.waited_stopped.append(sandbox_id)


def _cell(tmp_path: Path | None = None, client: FakeBoatClient | None = None) -> tuple[FakeBoatClient, BoatSandbox]:
    client = client or FakeBoatClient()
    sandbox = BoatSandbox(
        "swf-yos-103-zozo123-genworld-boatrun1",
        client=client,
        repo=REPO,
        base_branch="stabilize/main",
        target_dir="code",
        ttl_s=10_800,
        state=RunState(tmp_path) if tmp_path is not None else None,
    )
    return client, sandbox


def _commands(client: FakeBoatClient) -> list[str]:
    return [command for _, command, _, _ in client.commands]


# ---------------------------------------------------------------- the cell lifecycle


def test_ensure_creates_a_small_vm_clones_the_target_and_persists_the_id(tmp_path: Path) -> None:
    client, sandbox = _cell(tmp_path)
    sandbox.ensure()
    assert client.created == [{"machine_type": "small", "ttl_s": 10_800}]
    assert client.waited_ready == ["boat-1"]
    commands = _commands(client)
    assert "mkdir -p /workspace/zozo123-genworld" in commands
    assert f"git clone --depth 1 --branch stabilize/main https://github.com/{REPO}.git zozo123-genworld" in commands
    assert "test -d /workspace/zozo123-genworld/code" in commands
    assert sandbox.workdir == WORKDIR
    assert json.loads(RunState(tmp_path).read_control(BOAT_STATE_FILE)) == {"sandbox_id": "boat-1"}


def test_a_retried_task_reconnects_to_the_same_vm_instead_of_leaking_a_second(tmp_path: Path) -> None:
    client, sandbox = _cell(tmp_path)
    sandbox.ensure()
    _, restored = _cell(tmp_path, client=client)  # the next task rebuilds the cell from run state
    restored.ensure()
    assert client.created == [{"machine_type": "small", "ttl_s": 10_800}]  # one create
    assert "true" in _commands(client)  # the reconnect probe


def test_a_provisioning_failure_takes_the_vm_down_and_leaves_no_handle(tmp_path: Path) -> None:
    client, sandbox = _cell(tmp_path, client=FakeBoatClient(fail_on="git clone"))
    with pytest.raises(BoatError, match="boat checkout"):
        sandbox.ensure()
    # The VM always goes down: stop AND wait_stopped ran even though provisioning failed.
    assert client.stopped == ["boat-1"] and client.waited_stopped == ["boat-1"]
    # No handle outlives a sandbox that is already stopped.
    assert not RunState(tmp_path).has_control(BOAT_STATE_FILE)


def test_a_failed_teardown_retains_the_handle_for_the_retry_that_follows(tmp_path: Path) -> None:
    client, sandbox = _cell(tmp_path)
    sandbox.ensure()
    client.stop_errors.append("boat-1")
    sandbox.close()  # stop fails once: the handle must survive for the retry
    assert client.stopped == [] and sandbox.sandbox_id == "boat-1"
    assert json.loads(RunState(tmp_path).read_control(BOAT_STATE_FILE)) == {"sandbox_id": "boat-1"}
    sandbox.close()  # the retry converges and clears the handle
    assert client.stopped == ["boat-1"] and client.waited_stopped == ["boat-1"]
    assert sandbox.sandbox_id is None and not RunState(tmp_path).has_control(BOAT_STATE_FILE)
    sandbox.close()  # a second close of a closed cell is a no-op, never a second stop
    assert client.stopped == ["boat-1"]


def test_run_execs_in_the_confined_workdir_and_folds_a_timeout_into_exit_124(tmp_path: Path) -> None:
    client, sandbox = _cell(tmp_path)
    sandbox.ensure()
    client.timed_out = True  # boat kills the command at its cap; provisioning is already done
    res = sandbox.run("bun run check")
    assert res.timed_out and res.exit_code == 124  # coreutils `timeout` semantics, like every kind
    with pytest.raises(boat_mod.StageError, match="escapes"):
        sandbox.run("true", cwd="../../etc")  # the protocol confines cwd to the workdir


def test_run_carries_the_confined_cwd_to_the_client(tmp_path: Path) -> None:
    client, sandbox = _cell(tmp_path)
    sandbox.ensure()
    res = sandbox.run("uv run pytest", cwd="tests")
    assert res.exit_code == 0 and res.stdout == "out"
    assert ("boat-1", "uv run pytest", f"{WORKDIR}/tests", 1800) in client.commands
    assert ("boat-1", "bun run check", WORKDIR, 1800) not in client.commands


def test_paths_that_escape_the_repo_root_are_refused(tmp_path: Path) -> None:
    _, sandbox = _cell(tmp_path)
    sandbox.ensure()
    with pytest.raises(boat_mod.StageError, match="escapes"):
        sandbox.write("../../../etc/swf-escape", "no")
    with pytest.raises(boat_mod.StageError, match="escapes"):
        sandbox.read("../../outside.txt")


def test_write_creates_parents_and_reads_round_trip(tmp_path: Path) -> None:
    client, sandbox = _cell(tmp_path)
    sandbox.ensure()
    sandbox.write("docs/factory/YOS-103/spec.md", "hello")
    assert client.files[f"{WORKDIR}/docs/factory/YOS-103/spec.md"] == b"hello"
    assert sandbox.read("docs/factory/YOS-103/spec.md") == "hello"
    assert sandbox.exists("docs/factory/YOS-103/spec.md") is True
    with pytest.raises(FileNotFoundError):
        sandbox.read("missing.md")
    assert sandbox.exists("missing.md") is False


def test_make_sandbox_wires_the_boat_kind_from_the_process_environment(monkeypatch, tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    def fake_from_env(env=None):
        seen["env_arg"] = env
        return FakeBoatClient()

    monkeypatch.setattr(boat_mod, "boat_client_from_env", fake_from_env)
    cfg = _cfg()
    sandbox = make_sandbox(cfg, "YOS-103", repo=cfg.repo, run_dir=tmp_path)  # runtime's own wiring
    assert isinstance(sandbox, BoatSandbox)
    assert seen["env_arg"] is None  # the real client reads the process environment itself
    assert sandbox.name == "swf-yos-103-zozo123-genworld-boatrun1"
    assert sandbox.workdir == WORKDIR and sandbox.base_branch == "stabilize/main"


def test_config_refuses_the_claude_agent_on_boat_and_keeps_the_scripted_one() -> None:
    with pytest.raises(ValueError, match="no credential path into a boat sandbox"):
        _cfg(agent="claude")
    assert _cfg(agent="scripted").sandbox == "boat"


# ---------------------------------------------------------------- the worldgen blueprint


def test_the_worldgen_pilot_line_pins_target_branch_and_sandbox() -> None:
    bp = load("worldgen")
    assert bp.sandbox.kind == "boat"
    assert [(t.repo, t.dir, t.base_branch) for t in bp.targets] == [(REPO, "code", "stabilize/main")]
    assert bp.sandbox.ttl_s > bp.gate_timeout_h * 3600  # a run never outlives its cell
    job = bp.jobs({"issues": ["YOS-103"]})[0]
    cfg = bp.config(job, run_id="boatrun1")
    assert cfg.sandbox == "boat" and cfg.base_branch == "stabilize/main" and cfg.target_dir == "code"


# ---------------------------------------------------------------- doctor: presence only


def _boat_checks(env: dict[str, str]) -> dict[str, doctor.Check]:
    def refuse(argv) -> str:
        raise AssertionError(f"the boat check shells out to {list(argv)}")

    return {check.name: check for check in doctor.sandbox_checks(_cfg(), refuse, env=env)}


def test_boat_health_is_the_env_key_presence_never_its_value() -> None:
    checks = _boat_checks({"BOAT_API_KEY": KEY})
    (check,) = checks.values()
    assert check.ok and check.name == "boat api key" and "BOAT_API_KEY is set" in check.detail
    assert KEY not in check.detail  # a doctor report gets pasted into issues
    assert "boat api key" in doctor.PROVIDER_CHECKS


def test_a_missing_boat_key_blocks_the_preflight_before_provisioning() -> None:
    checks = _boat_checks({})
    assert list(checks) == ["boat api key"]
    check = checks["boat api key"]
    assert not check.ok and check.required and "BOAT_API_KEY is not set" in check.detail
    assert "boat.dev/dashboard?tab=api-keys" in check.fix
    blocking = doctor.preflight(_cfg(), env={})
    assert [c.name for c in blocking] == ["boat api key"]
    assert "export BOAT_API_KEY" in doctor.preflight_report(blocking)


def test_the_islo_chain_does_not_run_when_the_sandbox_is_boat(tmp_path: Path) -> None:
    def refuse(argv) -> str:
        raise AssertionError(f"no islo/docker/srt/toolset command may run for a boat cell: {list(argv)}")

    # The WorldGen side owns `code/factory.toml`; until it lands, the row honestly reports it.
    (tmp_path / "code").mkdir()
    (tmp_path / "code" / "factory.toml").write_text('[commands]\ntest = "bun run check"\n', encoding="utf-8")
    checks = {c.name: c for c in doctor.run_doctor(_cfg(), refuse, root=tmp_path, env={"BOAT_API_KEY": KEY})}
    assert checks["boat api key"].ok
    for name in ("islo cli", "islo auth", "integration github", "gateway profile", "islo environment"):
        assert name not in checks, f"{name} must not run for a boat cell"
    assert "claude cli" not in checks  # the scripted agent needs no host claude
    assert doctor.exit_code(list(checks.values())) == 0  # the key alone preflights the line


# ---------------------------------------------------------------- the transport (canned opener)


BASE_PATH = "/api/v1"  # the canned opener routes on the path relative to the base URL


def transport(monkeypatch, *, payloads: dict | None = None, error: Exception | None = None):
    """A canned opener keyed by ``(method, path)``.

    A route's value is one response (every call) or a list of responses (one per call, in
    order, so a poll loop can be walked through changing states). Unknown routes answer 404.
    """
    calls: list[tuple[urllib.request.Request, float]] = []

    class Opener:
        def open(self, request, timeout):
            calls.append((request, timeout))
            if error is not None:
                raise error
            parsed = urllib.parse.urlsplit(request.full_url)
            route = (request.get_method(), parsed.path.removeprefix(BASE_PATH) or "/")
            if route not in (payloads or {}):
                raise urllib.error.HTTPError(request.full_url, 404, "nope", {}, io.BytesIO(b"{}"))
            canned = payloads[route]
            if isinstance(canned, urllib.error.HTTPError):
                raise canned
            if isinstance(canned, list):
                if not canned:
                    raise urllib.error.HTTPError(request.full_url, 500, "exhausted", {}, io.BytesIO(b"{}"))
                canned = canned.pop(0)
            return io.BytesIO(json.dumps(canned).encode())

    monkeypatch.setattr(urllib.request, "build_opener", lambda *handlers: Opener())
    return calls


def _client(monkeypatch, payloads: dict | None = None, error: Exception | None = None):
    monkeypatch.setenv("BOAT_API_KEY", KEY)
    monkeypatch.setenv("BOAT_BASE_URL", "https://boat.test/api/v1")
    calls = transport(monkeypatch, payloads=payloads, error=error)
    return boat_client_from_env(), calls


def test_from_env_reads_the_key_and_base_url_from_the_environment_only(monkeypatch) -> None:
    calls = transport(monkeypatch)
    monkeypatch.delenv("BOAT_API_KEY", raising=False)
    monkeypatch.delenv("BOAT_BASE_URL", raising=False)
    with pytest.raises(BoatError) as error:
        boat_client_from_env()
    assert MISSING_KEY in str(error.value) and "boat.dev/dashboard" in str(error.value)
    assert calls == []  # refused before any network


def test_create_carries_the_bearer_key_small_type_and_noenv(monkeypatch) -> None:
    payloads = {
        ("POST", "/sandboxes"): {"ok": True, "type": "sandbox.created", "sandbox": {"id": "sb-1", "state": "ready"}},
    }
    client, calls = _client(monkeypatch, payloads)
    assert client.create(ttl_s=10_800) == "sb-1"
    (request, timeout) = calls[0]
    assert request.full_url == "https://boat.test/api/v1/sandboxes"
    assert request.get_header("Authorization") == f"Bearer {KEY}"
    assert json.loads(request.data) == {"noEnv": True, "type": "small", "ttlSeconds": 10_800}


def test_an_unknown_machine_type_and_a_bad_ttl_are_refused_before_the_network(monkeypatch) -> None:
    calls = transport(monkeypatch)
    client = boat_mod.HttpBoatClient(KEY, "https://boat.test/api/v1")
    with pytest.raises(BoatError, match="no machine type"):
        client.create(ttl_s=10_800, machine_type="gargantuan")
    with pytest.raises(BoatError, match="positive integer"):
        client.create(ttl_s=0)
    assert calls == []


def test_wait_ready_polls_until_ready_and_fails_a_terminal_state(monkeypatch) -> None:
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setenv("BOAT_API_KEY", KEY)
    transport(
        monkeypatch,
        payloads={
            ("GET", "/sandboxes/sb-1"): [
                {"sandbox": {"id": "sb-1", "state": "provisioning"}},
                {"sandbox": {"id": "sb-1", "state": "provisioning"}},
                {"sandbox": {"id": "sb-1", "state": "ready"}},
            ]
        },
    )
    client = boat_mod.HttpBoatClient(KEY, "https://boat.test/api/v1", poll_s=2.0, sleep=fake_sleep)
    client.wait_ready("sb-1")
    assert slept == [2.0, 2.0]

    transport(
        monkeypatch,
        payloads={("GET", "/sandboxes/sb-1"): {"sandbox": {"id": "sb-1", "state": "error", "error": "disk"}}},
    )
    client = boat_mod.HttpBoatClient(KEY, "https://boat.test/api/v1", sleep=fake_sleep)
    with pytest.raises(BoatError, match="entered state error: disk"):
        client.wait_ready("sb-1")


def test_exec_caps_the_requested_timeout_at_boats_own_bound(monkeypatch) -> None:
    payloads = {
        ("POST", "/sandboxes/sb-1/commands"): {
            "ok": True,
            "type": "command.finished",
            "exitCode": 0,
            "stdout": "out",
            "stderr": "",
            "timedOut": False,
        },
    }
    client, calls = _client(monkeypatch, payloads)
    result = client.exec("sb-1", "bun run check", cwd="/workspace/repo", timeout_s=1800)
    assert result.exit_code == 0 and result.stdout == "out"
    body = json.loads(calls[0][0].data)
    assert body == {"command": "bun run check", "cwd": "/workspace/repo", "timeoutSeconds": 600}
    assert urllib.parse.urlsplit(calls[0][0].full_url).path == f"{BASE_PATH}/sandboxes/sb-1/commands"


def test_write_file_travels_base64_and_read_file_comes_back_utf8(monkeypatch) -> None:
    payloads = {
        ("PUT", "/sandboxes/sb-1/files"): {"ok": True, "type": "file.written", "path": "/w/a.txt", "size": 5},
        ("GET", "/sandboxes/sb-1/files"): {
            "ok": True,
            "type": "file.read",
            "path": "/w/a.txt",
            "content": "hello",
            "size": 5,
            "encoding": "utf8",
        },
    }
    client, calls = _client(monkeypatch, payloads)
    client.write_file("sb-1", "/w/a.txt", b"hello")
    assert json.loads(calls[0][0].data) == {
        "path": "/w/a.txt",
        "content": base64.b64encode(b"hello").decode("ascii"),
        "encoding": "base64",
    }
    assert client.read_file("sb-1", "/w/a.txt") == "hello"
    query = urllib.parse.urlsplit(calls[1][0].full_url).query
    assert urllib.parse.parse_qs(query) == {"path": ["/w/a.txt"], "encoding": ["utf8"]}


def test_stop_treats_gone_and_archived_as_done(monkeypatch) -> None:
    payloads = {
        ("POST", "/sandboxes/sb-1/stop"): {"ok": True},
        ("GET", "/sandboxes/sb-1"): {"sandbox": {"id": "sb-1", "state": "archived"}},
    }
    client, _ = _client(monkeypatch, payloads)
    client.stop("sb-1")  # a plain stop is fine
    gone, _ = _client(monkeypatch, payloads={("GET", "/sandboxes/sb-1"): {"sandbox": {"id": "sb-1", "state": "ready"}}})
    gone.stop("sb-1")  # 404: boat.dev already removed it -- a no-op, not an error
    rejected = urllib.error.HTTPError(
        "https://boat.test/api/v1/sandboxes/sb-1/stop", 400, "bad request", {}, io.BytesIO(b"{}")
    )
    busy = dict(payloads)
    busy[("POST", "/sandboxes/sb-1/stop")] = rejected
    busy[("GET", "/sandboxes/sb-1")] = {"sandbox": {"id": "sb-1", "state": "running"}}
    running, _ = _client(monkeypatch, payloads=busy)
    with pytest.raises(BoatError) as error:  # 400 while still running is a real failure
        running.stop("sb-1")
    assert error.value.status == 400
    settled = dict(payloads)
    settled[("POST", "/sandboxes/sb-1/stop")] = rejected  # same 400, but the VM is archived
    archived, _ = _client(monkeypatch, payloads=settled)
    archived.stop("sb-1")  # 400 while already archived: the teardown already happened


def test_wait_stopped_polls_until_archived(monkeypatch) -> None:
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setenv("BOAT_API_KEY", KEY)
    transport(
        monkeypatch,
        payloads={
            ("GET", "/sandboxes/sb-1"): [
                {"sandbox": {"id": "sb-1", "state": "archiving"}},
                {"sandbox": {"id": "sb-1", "state": "archived"}},
            ]
        },
    )
    client = boat_mod.HttpBoatClient(KEY, "https://boat.test/api/v1", sleep=fake_sleep)
    client.wait_stopped("sb-1")
    assert slept == [2.0]


def test_the_key_never_enters_an_error_or_an_accepted_response(monkeypatch) -> None:
    # An HTTP error whose own reason text carries the key: the one-line BoatError must not.
    error = urllib.error.HTTPError("https://boat.test/api/v1/sandboxes", 500, f"boom {KEY}", {}, io.BytesIO(b"{}"))
    client, _ = _client(monkeypatch, payloads={("POST", "/sandboxes"): {}}, error=error)
    with pytest.raises(BoatError) as caught:
        client.create(ttl_s=600)
    assert str(caught.value) == "[sandbox] boat.dev create failed with HTTP 500"
    assert KEY not in str(caught.value) and caught.value.status == 500

    # A response body that echoes the key is refused, not parsed (the linear_source discipline).
    leaky = {("POST", "/sandboxes"): {"sandbox": {"id": KEY, "state": "ready"}}}
    client, _ = _client(monkeypatch, payloads=leaky)
    with pytest.raises(BoatError, match="response contains the API key"):
        client.create(ttl_s=600)


def test_the_bearer_key_never_travels_over_plaintext_beyond_loopback(monkeypatch) -> None:
    calls = transport(monkeypatch)
    for url in ("http://boat.test/api/v1", "ftp://boat.test/api/v1", "https://", "boat.test/api/v1"):
        with pytest.raises(BoatError, match="https URL"):
            boat_mod.HttpBoatClient(KEY, url)
    assert boat_mod.HttpBoatClient(KEY, "http://127.0.0.1:8787/api/v1").base_url == "http://127.0.0.1:8787/api/v1"
    assert boat_mod.HttpBoatClient(KEY, "http://localhost:8787").base_url == "http://localhost:8787"
    assert calls == []


def test_the_default_base_url_applies_without_an_override(monkeypatch) -> None:
    monkeypatch.setenv("BOAT_API_KEY", KEY)
    monkeypatch.delenv("BOAT_BASE_URL", raising=False)
    client = boat_client_from_env()
    assert client.base_url == boat_mod.BOAT_DEFAULT_BASE_URL
    assert KEY not in repr(client)
