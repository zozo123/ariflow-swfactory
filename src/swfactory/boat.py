"""boat.dev work cells: a small hosted VM per (issue, target), created credential-free.

The WorldGen side of the house runs its product sandboxes on boat.dev, so this is where the
WorldGen pilot line's work cells run (``sandbox.kind = "boat"``, ``blueprints/worldgen.toml``).
The provider surface is the one the WorldGen reference already exercises (``code/src/boat/
client.ts`` and ``code/src/sandboxes/boat.ts`` in ``jop8281/zozo123-genworld``), checked against
that repo's vendored ``@boatdev/sdk`` rather than guessed::

    POST  /sandboxes                create (``type`` small, ``ttlSeconds``, ``noEnv``) -> id
    GET   /sandboxes/<id>           state (ready/idle/running ... archived/cancelled)
    POST  /sandboxes/<id>/commands   exec (``cwd`` and ``timeoutSeconds`` travel per command)
    GET   /sandboxes/<id>/files      read one file (utf8)
    PUT   /sandboxes/<id>/files      write one file (base64)
    POST  /sandboxes/<id>/stop       archive the sandbox

``BOAT_API_KEY`` (and optional ``BOAT_BASE_URL``) come from the process environment only --
never a file, never an ``SWF_*`` knob -- and the key never enters an error message, a doctor
report, or a response body this module accepts. The VM is created with ``noEnv``, so nothing
from the account reaches it: the cell clones the public target itself (the same ``git clone
--depth 1 --branch`` the toolset cell runs, where islo passes ``--source`` and lets islo clone)
and every stage/test command runs over exec. A failure after create takes the VM down before
the error propagates, and ``close()`` (the teardown task) always stops and waits for the
archive, so a cell never outlives its run.

Honest limits, written down rather than discovered:

* boat accepts 1-600 s per command; a longer ``timeout_s`` is capped at 600 and a command killed
  at the cap reports ``timed_out`` (exit 124) rather than running unbounded.
* exec has no per-command environment and ``create(noEnv)`` passes nothing, so the model
  credential has no path into a boat VM without becoming ambient for target verification too.
  ``Config`` therefore refuses ``agent=claude`` with ``sandbox=boat``; the scripted agent needs
  nothing.
* create sends no egress policy, so a boat cell has no swfactory-enforced allowlist: neither the
  srt/toolset default (``SRT_DEFAULT_DOMAINS``) nor the islo ``swfactory`` gateway profile applies,
  and the VM reaches whatever boat.dev lets it reach (its own ``git clone`` of the target included).
* the sandbox id is persisted in host-owned run state the moment provisioning succeeds, so a
  retried or restarted task reconnects to the same VM instead of leaking a second one.
"""

from __future__ import annotations

import base64
import functools
import json
import os
import posixpath
import shlex
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Protocol, runtime_checkable

from swfactory.backend_http import is_loopback_host, no_redirect_open
from swfactory.models import TIMEOUT_EXIT_CODE, ExecResult, RunResult, StageError
from swfactory.paths import validate_git_ref, validate_repo
from swfactory.sandbox import (
    _CONTROL_TIMEOUT_S,
    _checkout_dir,
    _confine,
    fold_exec,
    load_handle,
    provision_checkout,
    save_handle,
)
from swfactory.state import RunState

BOAT_API_KEY_ENV = "BOAT_API_KEY"
BOAT_BASE_URL_ENV = "BOAT_BASE_URL"
BOAT_DEFAULT_BASE_URL = "https://boat.dev/api/v1"
MISSING_KEY = "BOAT_API_KEY is not set: create a key at https://boat.dev/dashboard?tab=api-keys and export it"
# boat.dev machine types: small is 2 vCPU / 4 GB and consumes machine time at half rate.
BOAT_TYPES = ("small", "default", "large")
BOAT_DEFAULT_TYPE = "small"
BOAT_MAX_COMMAND_S = 600  # boat accepts 1-600 s per command
BOAT_WORKDIR_ROOT = "/workspace"  # where the target repo is cloned inside the VM
BOAT_STATE_FILE = "boat-sandbox.json"  # host-owned handle, so teardown survives a crash
BOAT_READY_STATES = frozenset({"ready", "idle", "running"})
BOAT_STOPPED_STATES = frozenset({"archived", "cancelled"})
# States from which a sandbox can never become ready (archiving included: it is going down).
BOAT_TERMINAL_STATES = frozenset({"error", "archiving", "archived", "cancelled"})
BOAT_POLL_S = 2.0
BOAT_READY_TIMEOUT_S = 300.0
BOAT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class BoatError(StageError):
    """A boat.dev call failed: one line, never the API key, and its HTTP status when there was one."""

    def __init__(self, message: str, *, retryable: bool = False, status: int | None = None) -> None:
        super().__init__("sandbox", message, retryable=retryable)
        self.status = status


BoatExecResult = ExecResult


@runtime_checkable
class BoatClient(Protocol):
    """The narrow boat.dev surface a work cell needs; tests fake exactly this and nothing else."""

    def create(self, *, ttl_s: int, machine_type: str = BOAT_DEFAULT_TYPE) -> str:
        """Create a VM and return its sandbox id."""
        ...

    def wait_ready(self, sandbox_id: str) -> None:
        """Resolve once the VM is ready, idle or running."""
        ...

    def exec(
        self, sandbox_id: str, command: str, *, cwd: str | None = None, timeout_s: int | None = None
    ) -> BoatExecResult:
        """Run ``command`` to completion; ``cwd`` and ``timeout_s`` travel with it."""
        ...

    def read_file(self, sandbox_id: str, path: str) -> str:
        """Read one UTF-8 file inside the VM."""
        ...

    def write_file(self, sandbox_id: str, path: str, content: bytes) -> None:
        """Write one file inside the VM (base64 over the transport)."""
        ...

    def stop(self, sandbox_id: str) -> None:
        """Ask boat.dev to archive the VM; an already removed sandbox is a no-op."""
        ...

    def wait_stopped(self, sandbox_id: str) -> None:
        """Resolve once boat.dev reports the VM archived, so teardown is verified, not assumed."""
        ...


def boat_client_from_env(env: Mapping[str, str] | None = None) -> BoatClient:
    """The transport client from ``BOAT_API_KEY`` (and optional ``BOAT_BASE_URL``) in the process
    environment only -- never a file, never an ``SWF_*`` knob, so the key cannot leak into a
    config dump. Raises the one-line ``MISSING_KEY`` message without one."""
    source = os.environ if env is None else env
    key = (source.get(BOAT_API_KEY_ENV) or "").strip()
    if not key:
        raise BoatError(MISSING_KEY)
    base_url = (source.get(BOAT_BASE_URL_ENV) or "").strip()
    return HttpBoatClient(key, base_url or BOAT_DEFAULT_BASE_URL)


def _sandbox_path(sandbox_id: str) -> str:
    return f"/sandboxes/{urllib.parse.quote(sandbox_id, safe='')}"


def _state_of(sandbox: Mapping[str, object]) -> str:
    state = sandbox.get("state")
    if not isinstance(state, str) or not state.strip():
        raise BoatError("boat.dev returned a sandbox without a state")
    return state.strip()


def _expect(payload: dict, type_: str, op: str) -> None:
    if payload.get("type") != type_:
        raise BoatError(f"boat.dev {op} returned {payload.get('type')!r}, expected {type_!r}")


class HttpBoatClient:
    """The boat.dev REST transport. Every failure is one ``BoatError`` line; the key never enters one."""

    def __init__(
        self,
        api_key: str,
        base_url: str = BOAT_DEFAULT_BASE_URL,
        *,
        poll_s: float = BOAT_POLL_S,
        ready_timeout_s: float = BOAT_READY_TIMEOUT_S,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key or any(ch.isspace() for ch in api_key):
            raise BoatError(f"{BOAT_API_KEY_ENV} must be non-empty and free of whitespace")
        base_url = (base_url or BOAT_DEFAULT_BASE_URL).strip().rstrip("/")
        parsed = urllib.parse.urlsplit(base_url)
        # The bearer key rides on every request, so plaintext is allowed only where it never
        # leaves the host -- the rule webhook._safe_base holds the backend and Airflow tokens to.
        if not parsed.hostname or not (
            parsed.scheme == "https" or (parsed.scheme == "http" and is_loopback_host(parsed.hostname))
        ):
            raise BoatError(f"{BOAT_BASE_URL_ENV} must be an https URL (http only to loopback)")
        self._key = api_key
        self.base_url = base_url
        self.poll_s = poll_s
        self.ready_timeout_s = ready_timeout_s
        self._sleep = sleep

    def _request(
        self,
        op: str,
        method: str,
        path: str,
        *,
        body: Mapping[str, object] | None = None,
        query: Mapping[str, str] | None = None,
        timeout_s: float = _CONTROL_TIMEOUT_S,
    ) -> dict:
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                "Authorization": f"Bearer {self._key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method=method,
        )
        try:
            with no_redirect_open(request, timeout=timeout_s) as response:
                raw = response.read(BOAT_MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as error:
            error.close()
            raise BoatError(
                f"boat.dev {op} failed with HTTP {error.code}", retryable=error.code >= 500, status=error.code
            ) from None
        except (OSError, urllib.error.URLError):
            raise BoatError(f"boat.dev {op} failed; check connectivity to {self.base_url}", retryable=True) from None
        if len(raw) > BOAT_MAX_RESPONSE_BYTES:
            raise BoatError(f"boat.dev {op} response exceeds the transport limit")
        if self._key.encode() in raw:
            raise BoatError(f"boat.dev {op} response contains the API key")
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeError):
            raise BoatError(f"boat.dev {op} returned invalid JSON") from None
        if not isinstance(payload, dict):
            raise BoatError(f"boat.dev {op} returned a non-object response")
        return payload

    @staticmethod
    def _sandbox(payload: dict, op: str) -> dict:
        sandbox = payload.get("sandbox")
        if not isinstance(sandbox, dict) or not isinstance(sandbox.get("id"), str) or not sandbox["id"].strip():
            raise BoatError(f"boat.dev {op} returned no sandbox")
        return sandbox

    def _state(self, sandbox_id: str) -> str:
        try:
            sandbox = self._sandbox(self._request("get", "GET", _sandbox_path(sandbox_id)), "get")
        except BoatError as error:
            if error.status == 404:
                return "cancelled"  # boat.dev already removed it: gone is stopped
            raise
        return _state_of(sandbox)

    def _poll(self, sandbox_id: str, observe: Callable[[], str], done: frozenset[str], what: str) -> None:
        """Re-``observe`` the state every ``poll_s`` until it is in ``done``; ``observe`` raises on a
        dead end, and a state still short of ``done`` after ``ready_timeout_s`` is retryable."""
        waited = 0.0
        while (state := observe()) not in done:
            if waited >= self.ready_timeout_s:
                raise BoatError(
                    f"boat sandbox {sandbox_id} not {what} after {self.ready_timeout_s:g} s (state {state})",
                    retryable=True,
                )
            self._sleep(self.poll_s)
            waited += self.poll_s

    def create(self, *, ttl_s: int, machine_type: str = BOAT_DEFAULT_TYPE) -> str:
        if machine_type not in BOAT_TYPES:
            raise BoatError(f"boat has no machine type {machine_type!r}; have {list(BOAT_TYPES)}")
        if type(ttl_s) is not int or ttl_s <= 0:
            raise BoatError("boat ttl_s must be a positive integer")
        body = {"noEnv": True, "type": machine_type, "ttlSeconds": ttl_s}
        return self._sandbox(self._request("create", "POST", "/sandboxes", body=body), "create")["id"]

    def wait_ready(self, sandbox_id: str) -> None:
        def observe() -> str:
            sandbox = self._sandbox(self._request("get", "GET", _sandbox_path(sandbox_id)), "get")
            state = _state_of(sandbox)
            if state in BOAT_TERMINAL_STATES:
                why = sandbox.get("error")
                detail = f": {why}" if isinstance(why, str) and why.strip() else ""
                raise BoatError(f"boat sandbox {sandbox_id} entered state {state}{detail}")
            return state

        self._poll(sandbox_id, observe, BOAT_READY_STATES, "ready")

    def exec(
        self, sandbox_id: str, command: str, *, cwd: str | None = None, timeout_s: int | None = None
    ) -> BoatExecResult:
        body: dict[str, object] = {"command": command}
        if cwd:
            body["cwd"] = cwd
        if timeout_s is not None:
            if type(timeout_s) is not int or timeout_s <= 0:
                raise BoatError("boat exec timeout_s must be a positive integer")
            # The provider's own bound: values outside 1-600 are rejected, so a longer request
            # is capped rather than refused, and a kill at the cap reports timed_out.
            body["timeoutSeconds"] = min(timeout_s, BOAT_MAX_COMMAND_S)
        payload = self._request("exec", "POST", f"{_sandbox_path(sandbox_id)}/commands", body=body)
        _expect(payload, "command.finished", "exec")
        return BoatExecResult(
            exit_code=payload.get("exitCode") if isinstance(payload.get("exitCode"), int) else None,
            stdout=payload.get("stdout") if isinstance(payload.get("stdout"), str) else "",
            stderr=payload.get("stderr") if isinstance(payload.get("stderr"), str) else "",
            timed_out=payload.get("timedOut") is True,
            stdout_truncated=payload.get("stdoutTruncated") is True,
            stderr_truncated=payload.get("stderrTruncated") is True,
        )

    def read_file(self, sandbox_id: str, path: str) -> str:
        query = {"path": path, "encoding": "utf8"}
        payload = self._request("read file", "GET", f"{_sandbox_path(sandbox_id)}/files", query=query)
        _expect(payload, "file.read", "read")
        content = payload.get("content")
        if not isinstance(content, str):
            raise BoatError("boat.dev read returned no content")
        return content

    def write_file(self, sandbox_id: str, path: str, content: bytes) -> None:
        body = {"path": path, "content": base64.b64encode(content).decode("ascii"), "encoding": "base64"}
        payload = self._request("write file", "PUT", f"{_sandbox_path(sandbox_id)}/files", body=body)
        _expect(payload, "file.written", "write")

    def stop(self, sandbox_id: str) -> None:
        try:
            self._request("stop", "POST", f"{_sandbox_path(sandbox_id)}/stop")
        except BoatError as error:
            # A sandbox boat.dev already removed answers 404; a stop of an archived one 400.
            # Both mean the teardown already happened, which is all this call promises.
            if error.status == 404:
                return
            if error.status == 400 and self._state(sandbox_id) in BOAT_STOPPED_STATES:
                return
            raise

    def wait_stopped(self, sandbox_id: str) -> None:
        def observe() -> str:
            state = self._state(sandbox_id)
            if state == "error":
                raise BoatError(
                    f"boat sandbox {sandbox_id} entered state error while stopping, so it may still be billed"
                )
            return state

        self._poll(sandbox_id, observe, BOAT_STOPPED_STATES, "stopped")


class BoatSandbox:
    """A work cell on a boat.dev VM: create and clone once, exec per command, stop on close.

    The id is persisted in host-owned run state (``boat-sandbox.json``) the moment provisioning
    succeeds, so a retried or restarted task reconnects to the same VM -- a probe command proves
    it is alive -- instead of leaking a second one. Provisioning is the toolset cell's own
    sequence over exec (``mkdir`` the workspace, ``git clone --depth 1 --branch`` the public
    target, ``test -d`` the target dir). A failure after create stops the VM before the error
    propagates; a stop that also fails retains the handle so teardown can still close it.
    """

    def __init__(
        self,
        name: str,
        *,
        client: BoatClient,
        repo: str,
        base_branch: str,
        target_dir: str,
        ttl_s: int,
        state: RunState | None = None,
    ) -> None:
        self.name = name
        self.client = client
        self.repo = validate_repo(repo)
        self.base_branch = validate_git_ref(base_branch, field="base_branch")
        self.repo_root = f"{BOAT_WORKDIR_ROOT}/{self.repo.rsplit('/', 1)[-1]}"
        self.workdir = _checkout_dir(self.repo_root, target_dir)
        if type(ttl_s) is not int or ttl_s <= 0:
            raise BoatError("boat sandbox ttl_s must be a positive integer")
        self.ttl_s = ttl_s
        self.state = state
        # The public clone URL, exactly like make_scm's seed_url: read-only, and no token, ever.
        self.source = f"https://github.com/{self.repo}.git"
        self.sandbox_id: str | None = None

    def ensure(self) -> None:
        """Create the VM and clone the target once; reconnect to a persisted id (idempotent)."""
        self._restore_id()
        if self.sandbox_id is not None:
            probe = self._exec("true", cwd=self.workdir, timeout_s=_CONTROL_TIMEOUT_S)
            if not probe.ok:
                raise BoatError(
                    f"existing boat sandbox is unavailable (rc={probe.exit_code}); "
                    "recover it or start a new factory run",
                    retryable=True,
                )
            return
        sandbox_id = self.client.create(ttl_s=self.ttl_s, machine_type=BOAT_DEFAULT_TYPE)
        # Not persisted yet: anything that fails from here takes the VM down below, so no handle
        # outlives a sandbox that is already stopped.
        try:
            self.client.wait_ready(sandbox_id)
            self._provision(sandbox_id)
        except BaseException:
            try:
                self.client.stop(sandbox_id)
                self.client.wait_stopped(sandbox_id)
            except Exception:
                self.sandbox_id = sandbox_id
                self._persist_id()  # retain the handle so teardown can still close it
            raise
        self.sandbox_id = sandbox_id
        self._persist_id()

    def _provision(self, sandbox_id: str) -> None:
        """``mkdir`` the workspace, clone the public target, prove the target dir exists.

        ``sandbox.provision_checkout``, the same sequence the toolset cell runs inside its backend
        (islo gets it by passing ``--source`` and letting islo clone). Nothing here carries a
        credential: the clone URL is the public one, and the VM was created with ``noEnv``.
        """
        failed = provision_checkout(
            functools.partial(self._exec, sandbox_id=sandbox_id),
            repo_root=self.repo_root,
            workdir=self.workdir,
            source=self.source,
            branch=self.base_branch,
        )
        if failed is None:
            return
        step, res = failed
        if step == "target":
            raise BoatError(f"boat target directory is unavailable: {self.workdir}")
        what = "workspace mkdir" if step == "mkdir" else f"checkout of {self.repo}@{self.base_branch}"
        raise BoatError(f"boat {what} failed (rc={res.exit_code}): {res.stderr[-800:]}", retryable=step == "clone")

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        """Run ``cmd`` in the cell; the exit code propagates (a kill at boat's cap is 124)."""
        run_cwd = _confine(self.workdir, self.workdir, cwd) if cwd else self.workdir
        return self._exec(cmd, cwd=run_cwd, timeout_s=timeout_s)

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        """Same path: the boat cell is credential-free by construction, so there is no separate
        agent authentication to scope (``Config`` refuses ``agent=claude`` on boat)."""
        return self.run(cmd, timeout_s=timeout_s)

    def _exec(self, cmd: str, *, cwd: str, timeout_s: int, sandbox_id: str | None = None) -> RunResult:
        started = time.monotonic()
        res = self.client.exec(sandbox_id or self._id(), cmd, cwd=cwd, timeout_s=timeout_s)
        return fold_exec(res, tag="boat", started=started, timeout_exit=TIMEOUT_EXIT_CODE)

    def read(self, path: str) -> str:
        """Read one UTF-8 file relative to ``workdir``; missing -> ``FileNotFoundError``."""
        abs_path = _confine(self.repo_root, self.workdir, path)
        if not self.exists(path):
            raise FileNotFoundError(abs_path)
        return self.client.read_file(self._id(), abs_path)

    def write(self, path: str, content: str) -> None:
        """Write one file (base64 over the transport), creating parent directories by exec."""
        abs_path = _confine(self.repo_root, self.workdir, path)
        parent = posixpath.dirname(abs_path)
        if parent:
            made = self._exec(f"mkdir -p {shlex.quote(parent)}", cwd="/", timeout_s=_CONTROL_TIMEOUT_S)
            if not made.ok:
                raise BoatError(f"boat mkdir for {abs_path} failed (rc={made.exit_code}): {made.stderr[-800:]}")
        self.client.write_file(self._id(), abs_path, content.encode("utf-8"))

    def exists(self, path: str) -> bool:
        """True if ``path`` (relative to ``workdir``) exists inside the VM."""
        abs_path = _confine(self.repo_root, self.workdir, path)
        return self._exec(f"test -e {shlex.quote(abs_path)}", cwd="/", timeout_s=_CONTROL_TIMEOUT_S).ok

    def close(self) -> None:
        """Stop the VM and wait for the archive; a failed stop retains the handle for a retry."""
        self._restore_id()
        if self.sandbox_id is None:
            return
        sandbox_id = self.sandbox_id
        try:
            self.client.stop(sandbox_id)
            self.client.wait_stopped(sandbox_id)
        except Exception as error:
            # The handle stays: a later teardown (or a retried task's ensure) can retry the stop
            # instead of silently leaking a billable VM. The message is key-free by construction.
            print(f"sandbox: boat sandbox {sandbox_id} could not be stopped: {error}")
            return
        self.sandbox_id = None
        if self.state is not None:
            self.state.clear_control(BOAT_STATE_FILE)

    def _id(self) -> str:
        if self.sandbox_id is None:
            self.ensure()
        assert self.sandbox_id is not None
        return self.sandbox_id

    def _restore_id(self) -> None:
        if self.sandbox_id is not None:
            return
        try:
            record = load_handle(self.state, BOAT_STATE_FILE)
        except ValueError as e:
            raise BoatError(f"invalid boat sandbox state: {e}") from e
        if record is not None:
            self.sandbox_id = record["sandbox_id"]

    def _persist_id(self) -> None:
        if self.sandbox_id is not None:
            save_handle(self.state, BOAT_STATE_FILE, {"sandbox_id": self.sandbox_id})
