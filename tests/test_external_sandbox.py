"""Executable harness process supervision, authority confinement and confirmed teardown."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from swfactory import sandbox as sandbox_mod
from swfactory.models import RunResult, StageError
from swfactory.sandbox import DockerSandbox, LocalSandbox, SrtSandbox
from swfactory.sandbox_governance import SandboxIdentity
from swfactory.state import RunState


def _run(sb, argv, **overrides):
    options = dict(
        request='{"hello":"world"}', call_id="stage:1:build:0", writes=(), protected=(), timeout_s=5, credential_env=()
    )
    options.update(overrides)
    return sb.run_external(argv, **options)


def _worker(root: Path, source: str) -> Path:
    path = root / "worker.py"
    path.write_text(source)
    return path


def _docker(tmp_path: Path, **overrides) -> DockerSandbox:
    root = tmp_path / "checkout"
    root.mkdir(exist_ok=True)
    options = dict(
        image="pinned-test-image",
        identity=SandboxIdentity("docker", "cell", 1, "run"),
        state=RunState(tmp_path / "run"),
        network="none",
    )
    options.update(overrides)
    return DockerSandbox(root, **options)


class Fleet:
    """Deliberately returns foreign rows too: callers must authorize each observation."""

    def __init__(self):
        self.rows = []
        self.removed = []
        self.events = []
        self.fail_remove = False
        self.fail_observe = False

    def containers(self, labels=None):
        self.events.append(("observe", dict(labels or {})))
        if self.fail_observe:
            raise RuntimeError("daemon unavailable")
        return list(self.rows)

    def remove(self, resource):
        self.events.append(("remove", resource))
        if self.fail_remove:
            raise RuntimeError("removal unknown")
        self.removed.append(resource)
        self.rows = [row for row in self.rows if row["id"] != resource]


def _row(sb, *, name="stale-external", resource="old", call_id="old:call", labels=None):
    return dict(
        id=resource,
        name=name,
        running=True,
        labels=labels
        or {
            **sb.identity.labels,
            "swfactory.external": "true",
            "swfactory.external-call": hashlib.sha256(call_id.encode()).hexdigest()[:16],
        },
    )


def _mock_launch(monkeypatch, fleet, *, result=None, register=True, assertions=None):
    captured = {}
    monkeypatch.setattr(sandbox_mod, "DockerContainers", lambda: fleet)

    def launch(argv, **kwargs):
        fleet.events.append(("launch",))
        captured.update(argv=list(argv), **kwargs)
        if assertions:
            assertions(captured)
        if register:
            labels = dict(value.split("=", 1) for flag, value in zip(argv, argv[1:], strict=False) if flag == "--label")
            fleet.rows.append(dict(id="new", name=argv[argv.index("--name") + 1], running=True, labels=labels))
        return result or RunResult(0, "{}\n", "", 0.01, False)

    monkeypatch.setattr(sandbox_mod, "_run_external_subprocess", launch)
    return captured


def test_local_request_and_only_explicit_provider_auth(tmp_path, monkeypatch):
    sb = LocalSandbox(tmp_path)
    for name in (
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "SWF_BACKEND_TOKEN",
        "AIRFLOW_TOKEN",
        "GH_TOKEN",
        "CUSTOM_SECRET",
        "DOCKER_HOST",
    ):
        monkeypatch.setenv(name, "sentinel-" + name)
    worker = _worker(
        sb.root,
        """import json,os,pathlib,sys
p=pathlib.Path(sys.argv[-1])
print(json.dumps({'request':json.loads(p.read_text()),'mode':p.stat().st_mode & 511,
 'home':os.environ['HOME'],'env':{k:v for k,v in os.environ.items() if 'sentinel-' in v}}))
""",
    )
    result = _run(sb, [sys.executable, str(worker)], credential_env=("OPENAI_API_KEY",))
    data = json.loads(result.stdout)
    assert result.ok and data["request"] == {"hello": "world"} and data["mode"] == 0o444
    assert data["env"] == {"OPENAI_API_KEY": "sentinel-OPENAI_API_KEY"}
    assert not Path(data["home"]).exists()  # no credential/cache state persists between attempts
    assert not Path(data["home"]).is_relative_to(sb.root)


@pytest.mark.parametrize(
    "stream,limit",
    [("stdout", sandbox_mod.EXTERNAL_STDOUT_MAX_BYTES), ("stderr", sandbox_mod.EXTERNAL_STDERR_MAX_BYTES)],
)
def test_local_output_is_bounded_and_never_returns_truncated_json(tmp_path, stream, limit):
    worker = _worker(tmp_path, f"import os\nos.write({1 if stream == 'stdout' else 2}, b'x'*{limit + 1})\n")
    with pytest.raises(StageError, match=f"external {stream} exceeds"):
        _run(LocalSandbox(tmp_path), [sys.executable, str(worker)])


def test_local_invalid_utf8_is_rejected(tmp_path):
    worker = _worker(tmp_path, "import os\nos.write(1,b'\\xff')\n")
    with pytest.raises(StageError, match="not valid UTF-8"):
        _run(LocalSandbox(tmp_path), [sys.executable, str(worker)])


def _running(pid: int) -> bool:
    row = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False)
    return row.returncode == 0 and bool(row.stdout.strip()) and not row.stdout.strip().startswith("Z")


def test_local_timeout_kills_child_which_ignores_term(tmp_path):
    pid_path = tmp_path / "child.pid"
    child = (
        "import os,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "open('child.pid','w').write(str(os.getpid()));time.sleep(60)"
    )
    worker = _worker(
        tmp_path, f"import subprocess,sys,time\nsubprocess.Popen([sys.executable,'-c',{child!r}])\ntime.sleep(60)\n"
    )
    pid = None
    try:
        result = _run(LocalSandbox(tmp_path), [sys.executable, str(worker)], timeout_s=1)
        pid = int(pid_path.read_text())
        assert result.timed_out and result.exit_code == 124 and result.duration_s < 4
        until = time.monotonic() + 2
        while _running(pid) and time.monotonic() < until:
            time.sleep(0.02)
        assert not _running(pid)
    finally:
        if pid and _running(pid):
            os.kill(pid, signal.SIGKILL)


def test_confined_subclass_cannot_inherit_weak_local_boundary(tmp_path):
    with pytest.raises(StageError, match="not qualified"):
        _run(SrtSandbox(tmp_path, allowed_domains=()), ["true"])


@pytest.mark.parametrize("credential", ["SWF_BACKEND_TOKEN", "AIRFLOW_TOKEN", "GH_TOKEN", "DOCKER_HOST", "HOME"])
def test_service_credentials_cannot_be_declared_as_provider_auth(tmp_path, credential):
    with pytest.raises(StageError, match="credential name is not permitted"):
        _run(LocalSandbox(tmp_path), ["true"], credential_env=(credential,))


def test_docker_minimal_mounts_request_and_separate_provider_env(tmp_path, monkeypatch):
    sb = _docker(tmp_path, credentials="host", pass_env=("ANTHROPIC_API_KEY",))
    (sb.root / "src/private").mkdir(parents=True)
    (sb.root / "src/private/authority.py").write_text("protected")
    (sb.root / ".git").mkdir()
    fleet = Fleet()
    monkeypatch.setenv("OPENAI_API_KEY", "provider-secret")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "native-secret")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "service-secret")

    def check(captured):
        argv = captured["argv"]
        mounts = [value for flag, value in zip(argv, argv[1:], strict=False) if flag == "-v"]
        assert f"{sb.root}:{sb.root}:ro" in mounts
        assert f"{sb.root}/src:{sb.root}/src:rw" in mounts
        assert f"{sb.root}/src/private:{sb.root}/src/private:ro" in mounts
        assert f"{sb.root}/.git:{sb.root}/.git:ro" in mounts
        request_mount = next(value for value in mounts if value.endswith(":/tmp/swf-external/request.json:ro"))
        request_path = Path(request_mount.split(":", 1)[0])
        assert json.loads(request_path.read_text()) == {"hello": "world"}
        assert request_path.stat().st_mode & 0o777 == 0o444
        assert "--read-only" in argv and "--cap-drop=ALL" in argv
        assert "--rm" not in argv
        assert all("swfactory-sandbox-cache" not in value for value in mounts)
        assert all(str(Path.home()) + "/.claude" not in value for value in mounts)
        passed = [value for flag, value in zip(argv, argv[1:], strict=False) if flag == "-e"]
        assert "OPENAI_API_KEY" in passed and "ANTHROPIC_API_KEY" not in passed
        assert all("secret" not in value and "SWF_" not in value for value in passed)
        assert captured["env"]["OPENAI_API_KEY"] == "provider-secret"
        assert "ANTHROPIC_API_KEY" not in captured["env"] and "SWF_BACKEND_TOKEN" not in captured["env"]

    _mock_launch(monkeypatch, fleet, assertions=check)
    assert _run(
        sb,
        ["/trusted/adapter"],
        writes=("src",),
        protected=("src/private/authority.py",),
        credential_env=("OPENAI_API_KEY",),
    ).ok
    assert fleet.removed == ["new"] and not fleet.rows
    assert [receipt["status"] for receipt in sb.receipts] == ["converged"]
    assert [entry["event"] for entry in sb.state.read_jsonl("external-processes.jsonl")] == ["intent", "cleaned"]


@pytest.mark.parametrize(
    "writes,protected,reason",
    [
        (("../outside",), (), "stay inside its root"),
        (("/absolute",), (), "stay inside its root"),
        (("missing",), (), "existing directory"),
        (("src",), ("src/private/missing.py",), "missing inside"),
        (("src",), ("src/**",), "overlaps protected"),
        (("src",), ("*.lock",), "safely enforce"),
        ((".git",), (), "overlaps protected"),
    ],
)
def test_docker_refuses_unenforceable_write_policy_before_launch(tmp_path, monkeypatch, writes, protected, reason):
    sb = _docker(tmp_path)
    (sb.root / "src/private").mkdir(parents=True)
    (sb.root / ".git").mkdir()
    called = []
    monkeypatch.setattr(sandbox_mod, "_run_external_subprocess", lambda *args, **kwargs: called.append(True))
    with pytest.raises(StageError, match=reason):
        _run(sb, ["/trusted/adapter"], writes=writes, protected=protected)
    assert not called


@pytest.mark.parametrize("alias", ["symlink", "hardlink"])
def test_docker_rejects_existing_writable_aliases(tmp_path, alias):
    sb = _docker(tmp_path)
    (sb.root / "src").mkdir()
    target = sb.root / "authority.py"
    target.write_text("protected")
    if alias == "symlink":
        (sb.root / "src/alias").symlink_to(target)
    else:
        os.link(target, sb.root / "src/alias")
    with pytest.raises(StageError, match="symlink or hardlink"):
        _run(sb, ["/trusted/adapter"], writes=("src",))


def test_docker_timeout_removes_exact_container_and_keeps_foreign_runs(tmp_path, monkeypatch):
    sb = _docker(tmp_path)
    fleet = Fleet()
    foreign = _row(
        sb, resource="foreign", labels={**sb.identity.labels, "swfactory.epoch": "2", "swfactory.external": "true"}
    )
    legacy = _row(sb, resource="legacy", labels=dict(sb.identity.labels))
    fleet.rows.extend([foreign, legacy])
    _mock_launch(monkeypatch, fleet, result=RunResult(124, "", "", 1, True))
    result = _run(sb, ["/trusted/adapter"])
    assert result.timed_out and fleet.removed == ["new"] and fleet.rows == [foreign, legacy]


@pytest.mark.parametrize("failure", ["remove", "observe", "absent"])
def test_docker_unknown_cleanup_refuses_success(tmp_path, monkeypatch, failure):
    sb = _docker(tmp_path)
    fleet = Fleet()
    _mock_launch(monkeypatch, fleet, register=failure != "absent")
    if failure == "remove":
        fleet.fail_remove = True
    elif failure == "observe":
        fleet.fail_observe = True
    with pytest.raises(StageError, match="unresolved|cleanup is unknown"):
        _run(sb, ["/trusted/adapter"])
    assert any(row["status"] == "ambiguous" for row in json.loads(sb.state.read_control("cleanup.json")))


def test_docker_reaps_previous_call_before_new_launch(tmp_path, monkeypatch):
    sb = _docker(tmp_path)
    fleet = Fleet()
    previous = _row(sb)
    fleet.rows.append(previous)
    sb.state.append_json(
        "external-processes.jsonl",
        {"event": "intent", "container": previous["name"], "call_id": "old:call", "labels": previous["labels"]},
    )
    _mock_launch(monkeypatch, fleet)
    assert _run(sb, ["/trusted/adapter"]).ok
    assert fleet.removed == ["old", "new"]
    assert fleet.events.index(("remove", "old")) < fleet.events.index(("launch",))
    assert not fleet.rows


@pytest.mark.parametrize("terminal", [False, True])
def test_docker_absent_interrupted_create_blocks_retry(tmp_path, monkeypatch, terminal):
    sb = _docker(tmp_path)
    fleet = Fleet()
    previous = _row(sb)
    sb.state.append_json(
        "external-processes.jsonl",
        {"event": "intent", "container": previous["name"], "call_id": "old:call", "labels": previous["labels"]},
    )
    if terminal:
        sb.state.write_control(
            "external-cleanup-" + previous["labels"]["swfactory.external-call"] + ".json", '[{"status":"ambiguous"}]'
        )
    _mock_launch(monkeypatch, fleet)
    with pytest.raises(StageError, match="no observed terminal cleanup"):
        _run(sb, ["/trusted/adapter"])
    assert not any(event[0] == "launch" for event in fleet.events)


@pytest.fixture
def live_image():
    if os.environ.get("SWF_TEST_EXTERNAL_DOCKER") != "1":
        pytest.skip("set SWF_TEST_EXTERNAL_DOCKER=1 for live Docker confinement checks")
    image = os.environ.get("SWF_TEST_EXTERNAL_DOCKER_IMAGE", "python:3.12-slim")
    inspected = subprocess.run(
        ["docker", "image", "inspect", image, "--format", "{{index .RepoDigests 0}}"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert inspected.returncode == 0, inspected.stderr
    digest = inspected.stdout.strip()
    assert "@sha256:" in digest, "live image must have a verified repository digest"
    return digest


def test_live_docker_readonly_root_request_and_protected_ancestor(tmp_path, live_image):
    sb = _docker(tmp_path, image=live_image)
    (sb.root / "src/guarddir").mkdir(parents=True)
    protected = sb.root / "src/guarddir/authority.py"
    protected.write_text("protected")
    worker = _worker(
        sb.root,
        """import json,pathlib,sys
blocked=[]
def denied(label,action):
 try: action()
 except OSError: blocked.append(label)
denied('root',lambda:pathlib.Path('forbidden.py').write_text('bad'))
denied('request',lambda:pathlib.Path(sys.argv[-1]).write_text('bad'))
denied('protected',lambda:pathlib.Path('src/guarddir/authority.py').write_text('bad'))
denied('ancestor',lambda:pathlib.Path('src/guarddir').rename('src/moved'))
pathlib.Path('src/change.py').write_text('allowed')
print(json.dumps({'blocked':blocked,'request':json.loads(pathlib.Path(sys.argv[-1]).read_text())}))
""",
    )
    result = _run(
        sb,
        ["/usr/local/bin/python", str(worker)],
        writes=("src",),
        protected=("src/guarddir/authority.py",),
        timeout_s=15,
    )
    assert result.ok, result.stderr
    assert json.loads(result.stdout)["blocked"] == ["root", "request", "protected", "ancestor"]
    assert protected.read_text() == "protected" and (sb.root / "src/change.py").read_text() == "allowed"
    with pytest.raises(StageError, match="missing inside"):
        _run(sb, ["/usr/local/bin/python", str(worker)], writes=("src",), protected=("src/guarddir/missing.py",))
    assert all(row["status"] == "converged" for row in sb.receipts)


def test_live_docker_timeout_removes_detached_child_container(tmp_path, live_image):
    sb = _docker(tmp_path, image=live_image)
    worker = _worker(
        sb.root,
        """import subprocess,sys,time
subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)
print('started',flush=True)
time.sleep(60)
""",
    )
    result = _run(sb, ["/usr/local/bin/python", str(worker)], timeout_s=2)
    assert result.timed_out and result.duration_s < 7 and result.stdout.strip() == "started"
    assert sandbox_mod.DockerContainers().containers({**sb.identity.labels, "swfactory.external": "true"}) == []
    assert all(row["status"] == "converged" for row in sb.receipts)
