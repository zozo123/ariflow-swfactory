"""Sandbox credential invariants and exit-code semantics. Hermetic: no islo, no network."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from swfactory import sandbox as sandbox_mod
from swfactory.config import Config
from swfactory.models import RunResult, StageError
from swfactory.sandbox import (
    DOCKER_CACHE_VOLUME,
    DOCKER_HOME,
    HOST_SANDBOXES,
    SCRUB_PREFIXES,
    SRT_CLAUDE_DOMAINS,
    DockerSandbox,
    IsloSandbox,
    LocalSandbox,
    Sandbox,
    SrtSandbox,
    make_sandbox,
    scrub_env,
)

# Whole-argument flags that would pass host env into the sandbox (note: `--environment` is the
# islo *secrets environment*, a different thing, so flags are matched exactly, not by prefix).
FORBIDDEN_FLAGS = ("--env", "--env-file")
# Substrings that would mean a credential name/value leaked into argv.
FORBIDDEN_SUBSTRINGS = ("ANTHROPIC", "GH_TOKEN")


def _islo(**overrides) -> IsloSandbox:
    kwargs = dict(
        source="github://zozo123/ariflow-swfactory:main",
        gateway_profile="swfactory",
        environment="swfactory",
        ttl_s=172_800,
        idle_s=900,
        target_dir="demo/target",
        factory_root=Path("/factory"),
    )
    kwargs.update(overrides)
    return IsloSandbox("swf-demo-1-abcd1234", **kwargs)


def _assert_no_credentials(argv: list[str]) -> None:
    for arg in argv:
        assert arg not in FORBIDDEN_FLAGS, f"{arg!r} passes host env into the sandbox: {argv}"
        assert not arg.startswith(tuple(f"{f}=" for f in FORBIDDEN_FLAGS)), argv
        for token in FORBIDDEN_SUBSTRINGS:
            assert token not in arg, f"{token!r} leaked into argv: {argv}"


# ---------------------------------------------------------------- argv invariants


def test_create_argv_shape_and_no_credentials() -> None:
    sb = _islo()
    argv = sb.argv("true", create=True)
    _assert_no_credentials(argv)
    assert argv[:3] == ["islo", "use", "swf-demo-1-abcd1234"]
    for flag, value in (
        ("--source", "github://zozo123/ariflow-swfactory:main"),
        ("--gateway-profile", "swfactory"),
        ("--environment", "swfactory"),
        ("--delete-after", "172800"),
        ("--pause-after-idle", "900"),
        ("--init", "minimal"),
        ("--output", "plain"),
    ):
        assert flag in argv
        assert argv[argv.index(flag) + 1] == value
    assert argv[-2:] == ["--", "true"]
    assert argv[argv.index("--auto-resume") + 1] == "on_activity"
    assert "--snapshot" not in argv  # only when configured


def test_create_argv_snapshot_when_set() -> None:
    argv = _islo(snapshot="swf-golden-20260902").argv("true", create=True)
    _assert_no_credentials(argv)
    assert argv[argv.index("--snapshot") + 1] == "swf-golden-20260902"
    assert argv.index("--snapshot") < argv.index("--")
    assert argv[-2:] == ["--", "true"]
    assert "--snapshot" not in _islo(snapshot="x").argv("ls")  # run never re-sends create flags


def test_run_argv_uses_bash_lc_and_cd_workdir() -> None:
    sb = _islo()
    assert sb.workdir == "/workspace/ariflow-swfactory/demo/target"
    argv = sb.argv("uv run pytest")
    _assert_no_credentials(argv)
    assert argv[:3] == ["islo", "use", "swf-demo-1-abcd1234"]
    assert argv[argv.index("--") + 1 :][:2] == ["bash", "-lc"]
    script = argv[-1]
    assert script.startswith("cd /workspace/ariflow-swfactory/demo/target && ")
    assert script.endswith("uv run pytest")
    assert "--source" not in argv  # run never re-sends create flags


def test_run_argv_cwd_override_is_quoted() -> None:
    cwd = "/workspace/ariflow-swfactory/demo/target/some dir"
    argv = _islo().argv("ls", cwd=cwd)
    assert argv[-1].startswith(f"cd '{cwd}' && ls")


def test_argv_never_carries_credentials_even_when_env_holds_them(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("GH_TOKEN", "ghp_secret")
    sb = _islo()
    for argv in (sb.argv("true", create=True), sb.argv("echo hi"), sb.argv("ls", cwd="x")):
        _assert_no_credentials(argv)
        assert "sk-ant-secret" not in " ".join(argv)
        assert "ghp_secret" not in " ".join(argv)


def test_root_target_dir_has_no_trailing_slash() -> None:
    assert _islo(target_dir="").workdir == "/workspace/ariflow-swfactory"


# ---------------------------------------------------------------- exit-code semantics (fake islo)


def test_islo_run_propagates_returncode(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 3, stdout="hi\n", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    res = _islo().run("echo hi; exit 3", timeout_s=42)
    assert isinstance(res, RunResult)
    assert res.exit_code == 3
    assert res.stdout == "hi\n"
    assert res.timed_out is False
    assert not res.ok
    assert seen["argv"][:2] == ["islo", "use"]
    assert seen["kwargs"]["timeout"] == 42
    assert seen["kwargs"]["cwd"] is None
    _assert_no_credentials(seen["argv"])


def test_islo_run_timeout_sets_timed_out(monkeypatch) -> None:
    def fake_run(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"], output=b"partial", stderr=None)

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    res = _islo().run("sleep 999", timeout_s=1)
    assert res.timed_out is True
    assert res.exit_code == sandbox_mod.TIMEOUT_EXIT_CODE
    assert res.stdout == "partial"
    assert not res.ok


def test_islo_ensure_runs_create_from_factory_root(monkeypatch, tmp_path) -> None:
    seen: dict[str, object] = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["cwd"] = kwargs["cwd"]
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    _islo(factory_root=tmp_path).ensure()
    assert "--source" in seen["argv"]
    assert seen["cwd"] == tmp_path


def test_islo_exists_and_read_write_use_cp(monkeypatch, tmp_path) -> None:
    calls: list[list[str]] = []
    store: dict[str, str] = {}

    def fake_run(argv, **kwargs):
        calls.append(argv)
        if argv[:2] == ["islo", "cp"]:
            src, dst = argv[2], argv[3]
            if ":" in src:  # sandbox -> local
                remote = src.split(":", 1)[1]
                if remote not in store:
                    return subprocess.CompletedProcess(argv, 1, stdout="", stderr="no such file")
                Path(dst).write_text(store[remote])
            else:  # local -> sandbox
                store[dst.split(":", 1)[1]] = Path(src).read_text()
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        script = argv[-1]
        rc = 0 if "test -e" in script and "/exists.txt" in script else 1
        if script.split("&& ", 1)[-1].startswith("mkdir -p"):
            rc = 0
        return subprocess.CompletedProcess(argv, rc, stdout="", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    sb = _islo()
    sb.write("docs/factory/DEMO-1/spec.md", "# spec")
    assert store["/workspace/ariflow-swfactory/demo/target/docs/factory/DEMO-1/spec.md"] == "# spec"
    assert sb.read("docs/factory/DEMO-1/spec.md") == "# spec"
    with pytest.raises(FileNotFoundError):
        sb.read("missing.md")
    assert sb.exists("exists.txt") is True
    assert sb.exists("nope.txt") is False
    for argv in calls:
        _assert_no_credentials(argv)


def test_islo_cp_retries_once_after_resume(monkeypatch, tmp_path) -> None:
    """``islo cp`` does not wake a paused VM: the first failure triggers ``islo resume`` + retry."""
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        if argv[:2] == ["islo", "cp"]:
            n_cp = sum(1 for c in calls if c[:2] == ["islo", "cp"])
            if n_cp == 1:
                return subprocess.CompletedProcess(argv, 1, stdout="", stderr="sandbox paused")
            if ":" in argv[2]:  # sandbox -> local: materialise the file
                Path(argv[3]).write_text("# spec")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    assert _islo().read("docs/factory/DEMO-1/spec.md") == "# spec"
    kinds = [c[:2] for c in calls]
    assert kinds == [["islo", "cp"], ["islo", "resume"], ["islo", "cp"]]
    assert calls[1] == ["islo", "resume", "swf-demo-1-abcd1234", "--output", "plain"]
    assert calls[0][2:] == calls[2][2:]  # identical retry
    for argv in calls:
        _assert_no_credentials(argv)


def test_islo_cp_gives_up_after_one_retry(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        rc = 1 if argv[:2] == ["islo", "cp"] else 0
        return subprocess.CompletedProcess(argv, rc, stdout="", stderr="no such file")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    with pytest.raises(FileNotFoundError):
        _islo().read("missing.md")
    assert [c[:2] for c in calls] == [["islo", "cp"], ["islo", "resume"], ["islo", "cp"]]
    with pytest.raises(sandbox_mod.StageError) as ei:
        _islo().write("x.md", "x")
    assert ei.value.kind == "sandbox" and ei.value.retryable


def _fake_islo(seen: list[list[str]], listing: str):
    def fake_run(argv, **kwargs):
        seen.append(argv)
        out = listing if argv[:2] == ["islo", "ls"] else ""
        return subprocess.CompletedProcess(argv, 0, stdout=out, stderr="")

    return fake_run


def test_islo_close_lists_then_removes_only_own_sandbox(monkeypatch) -> None:
    seen: list[list[str]] = []
    mine = '[{"name": "swf-demo-1-abcd1234", "status": "running", "created_by": "me@x.io"}]'
    monkeypatch.setattr(sandbox_mod.subprocess, "run", _fake_islo(seen, mine))
    _islo().close()
    assert seen == [
        ["islo", "ls", "--output", "json"],
        ["islo", "rm", "swf-demo-1-abcd1234", "--output", "plain"],
    ]
    assert all("--all" not in argv for argv in seen)


def test_islo_close_refuses_unknown_or_foreign_sandbox(monkeypatch) -> None:
    seen: list[list[str]] = []
    # not in own listing at all -> no rm
    monkeypatch.setattr(sandbox_mod.subprocess, "run", _fake_islo(seen, "[]"))
    _islo().close()
    assert [a[1] for a in seen] == ["ls"]
    # present but created by someone else while an owner is configured -> no rm
    seen.clear()
    foreign = '[{"name": "swf-demo-1-abcd1234", "status": "running", "created_by": "them@x.io"}]'
    monkeypatch.setattr(sandbox_mod.subprocess, "run", _fake_islo(seen, foreign))
    sb = _islo()
    sb.owner = "me@x.io"
    sb.close()
    assert [a[1] for a in seen] == ["ls"]


def test_owns_sandbox_pure() -> None:
    listing = '[{"name": "swf-a-00000000", "status": "running", "created_by": "Me@X.io"}]'
    assert sandbox_mod.owns_sandbox(listing, "swf-a-00000000")
    assert sandbox_mod.owns_sandbox(listing, "swf-a-00000000", owner="me@x.io")
    assert not sandbox_mod.owns_sandbox(listing, "swf-a-00000000", owner="other@x.io")
    assert not sandbox_mod.owns_sandbox(listing, "swf-b-00000000")
    assert not sandbox_mod.owns_sandbox("garbage", "swf-a-00000000")


# ---------------------------------------------------------------- LocalSandbox (real bash)


def test_local_run_real_exit_code_and_stdout(tmp_path) -> None:
    sb = LocalSandbox(tmp_path)
    sb.ensure()
    assert (tmp_path / ".git").is_dir()
    res = sb.run("echo hi; exit 3")
    assert res.exit_code == 3
    assert res.stdout == "hi\n"
    assert res.timed_out is False
    assert res.duration_s >= 0
    sb.ensure()  # idempotent
    assert sb.run("pwd").stdout.strip() == str(tmp_path.resolve())


def test_local_run_timeout(tmp_path) -> None:
    res = LocalSandbox(tmp_path).run("sleep 5", timeout_s=1)
    assert res.timed_out is True
    assert not res.ok


def test_local_read_write_exists_round_trip(tmp_path) -> None:
    sb = LocalSandbox(tmp_path)
    sb.ensure()
    assert sb.exists("docs/factory/DEMO-1/spec.md") is False
    sb.write("docs/factory/DEMO-1/spec.md", "# spec\n")
    assert sb.exists("docs/factory/DEMO-1/spec.md") is True
    assert sb.read("docs/factory/DEMO-1/spec.md") == "# spec\n"
    with pytest.raises(FileNotFoundError):
        sb.read("nope.md")
    sb.close()  # no-op
    assert sb.exists("docs/factory/DEMO-1/spec.md")


def test_local_child_env_is_scrubbed(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("GH_TOKEN", "ghp_secret")
    res = LocalSandbox(tmp_path).run('echo "[$ANTHROPIC_API_KEY][$GH_TOKEN][$PATH]"')
    assert res.exit_code == 0
    assert res.stdout.startswith("[][][")
    assert "sk-ant-secret" not in res.stdout
    assert "ghp_secret" not in res.stdout


# ---------------------------------------------------------------- SrtSandbox (fake srt)


def _srt(tmp_path: Path, **overrides) -> SrtSandbox:
    kwargs = dict(allowed_domains=("api.anthropic.com", "pypi.org"), protected=(), pass_env=())
    kwargs.update(overrides)
    return SrtSandbox(tmp_path / "work", **kwargs)


def _fake_srt(monkeypatch, rc: int = 0):
    seen: dict[str, object] = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, rc, stdout="out\n", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    return seen


def test_srt_settings_written_with_workdir_and_protected(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(sandbox_mod.Path, "home", classmethod(lambda cls: tmp_path / "home"))
    for _p in sandbox_mod.SRT_DENY_READ:
        (tmp_path / "home" / _p).mkdir(parents=True, exist_ok=True)
    sb = _srt(tmp_path, protected=("tests/", "src/**/*.py", "factory.toml", "*.lock"))
    for _d in (".claude", ".github", "tests", "src"):
        (tmp_path / "work" / _d).mkdir(parents=True, exist_ok=True)
    (tmp_path / "work" / "factory.toml").touch()
    _fake_srt(monkeypatch)
    sb.ensure()
    assert sb.name == "srt:work"
    assert sb.settings_path == tmp_path / "work" / ".factory" / "srt-settings.json"
    doc = json.loads(sb.settings_path.read_text())
    fs, net = doc["filesystem"], doc["network"]
    work = str((tmp_path / "work").resolve())
    home = tmp_path / "home"
    assert fs["allowWrite"][0] == work
    for p in (home / ".claude", home / ".claude.json", home / ".cache"):
        assert str(p) in fs["allowWrite"]
    assert "/tmp" in fs["allowWrite"] and "/private/tmp" in fs["allowWrite"]
    assert str(home / "Library/Caches") not in fs["allowWrite"]  # absent dir -> not listed
    assert fs["denyWrite"] == [
        f"{work}/.claude",
        f"{work}/.github",
        f"{work}/tests",
        f"{work}/src",
        f"{work}/factory.toml",
    ]  # `*.lock` has no literal prefix: left to the Edit(...) deny rules and the hook
    assert fs["denyRead"] == [
        str(home / p) for p in (".ssh", ".aws", ".config", ".gnupg", ".netrc", ".docker", ".kube")
    ]
    assert net["allowedDomains"][:2] == ["api.anthropic.com", "pypi.org"]
    for d in SRT_CLAUDE_DOMAINS:
        assert net["allowedDomains"].count(d) == 1
    assert net["deniedDomains"] == []


def test_srt_set_protected_rewrites_settings_and_run_resyncs_stale_file(
    tmp_path, monkeypatch
) -> None:
    """The contract is known only after setup seeds the workdir: ``set_protected`` must reach the
    kernel policy, and a fresh object over the same workdir (next DAG task) must not trust a
    settings file written with a different ``protected``."""
    monkeypatch.setattr(sandbox_mod.Path, "home", classmethod(lambda cls: tmp_path / "home"))
    seen = _fake_srt(monkeypatch)
    sb = _srt(tmp_path)
    for _d in (".claude", ".github", "tests", "src"):
        (tmp_path / "work" / _d).mkdir(parents=True, exist_ok=True)
    (tmp_path / "work" / "factory.toml").touch()
    sb.ensure()
    work = str((tmp_path / "work").resolve())
    deny = lambda: json.loads(sb.settings_path.read_text())["filesystem"]["denyWrite"]  # noqa: E731
    assert deny() == [f"{work}/.claude", f"{work}/.github"]
    sb.set_protected(["factory.toml", "tests/"])
    assert sb.protected == ("factory.toml", "tests/")
    assert deny() == [f"{work}/.claude", f"{work}/.github", f"{work}/factory.toml", f"{work}/tests"]

    other = _srt(tmp_path, protected=("factory.toml",))  # same workdir, different policy
    other.run("true")
    assert deny() == [f"{work}/.claude", f"{work}/.github", f"{work}/factory.toml"]
    assert seen["argv"][-1].startswith(f"cd {sb.workdir} && ")
    stamp = sb.settings_path.stat().st_mtime_ns
    other.run("true")  # in sync: not rewritten
    assert sb.settings_path.stat().st_mtime_ns == stamp


def test_srt_ensure_inits_git_host_side_then_runs_confined(tmp_path, monkeypatch) -> None:
    """srt forbids creating `.git`: ensure() runs `git init` unconfined; run() goes through srt."""
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        if argv[:2] == ["bash", "-lc"]:
            (tmp_path / "work" / ".git").mkdir(parents=True)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    monkeypatch.setattr(sandbox_mod.shutil, "which", lambda name: "/usr/local/bin/srt")
    sb = _srt(tmp_path)
    sb.ensure()
    assert calls == [["bash", "-lc", "git init -q -b main"]]
    assert sb.settings_path.exists()
    sb.ensure()  # idempotent: .git present -> no second init
    assert len(calls) == 1
    sb.run("git status")
    assert calls[-1][:3] == ["srt", "-s", str(sb.settings_path)]


def test_srt_ensure_raises_on_git_init_failure(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        sandbox_mod.subprocess,
        "run",
        lambda argv, **kw: subprocess.CompletedProcess(argv, 128, stdout="", stderr="boom"),
    )
    with pytest.raises(sandbox_mod.StageError) as ei:
        _srt(tmp_path).ensure()
    assert ei.value.kind == "sandbox" and "boom" in str(ei.value)


def test_srt_macos_caches_listed_only_when_present(tmp_path, monkeypatch) -> None:
    home = tmp_path / "home"
    (home / "Library" / "Caches").mkdir(parents=True)
    monkeypatch.setattr(sandbox_mod.Path, "home", classmethod(lambda cls: home))
    assert str(home / "Library/Caches") in _srt(tmp_path).settings()["filesystem"]["allowWrite"]


def test_srt_argv_prefers_srt_binary_else_npx(tmp_path, monkeypatch) -> None:
    sb = _srt(tmp_path)
    monkeypatch.setattr(sandbox_mod.shutil, "which", lambda name: "/usr/local/bin/srt")
    argv = sb.argv("uv run pytest")
    assert argv[0] == "srt"
    assert argv[1:3] == ["-s", str(sb.settings_path)]
    assert argv[3] == "-c"
    assert argv[4] == f"cd {sb.workdir} && uv run pytest"
    monkeypatch.setattr(sandbox_mod.shutil, "which", lambda name: None)
    argv = sb.argv("ls", cwd="some dir")
    assert argv[:3] == ["npx", "-y", "@anthropic-ai/sandbox-runtime"]
    assert argv[3:5] == ["-s", str(sb.settings_path)]
    assert argv[-1] == f"cd '{sb.workdir}/some dir' && ls"
    for a in argv:
        assert a not in FORBIDDEN_FLAGS


def test_srt_run_propagates_rc_and_writes_settings_lazily(tmp_path, monkeypatch) -> None:
    sb = _srt(tmp_path)
    seen = _fake_srt(monkeypatch, rc=3)
    assert not sb.settings_path.exists()
    res = sb.run("exit 3", timeout_s=17)
    assert sb.settings_path.exists()
    assert isinstance(res, RunResult) and res.exit_code == 3 and not res.ok
    assert res.stdout == "out\n" and res.timed_out is False
    assert seen["kwargs"]["timeout"] == 17
    assert seen["kwargs"]["cwd"] == sb.root
    assert seen["argv"][0] in ("srt", "npx")
    assert seen["argv"][-1].startswith(f"cd {sb.workdir} && ")


def test_srt_run_timeout_sets_timed_out(tmp_path, monkeypatch) -> None:
    def fake_run(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"], output=b"partial", stderr=None)

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    res = _srt(tmp_path).run("sleep 999", timeout_s=1)
    assert res.timed_out is True and res.exit_code == sandbox_mod.TIMEOUT_EXIT_CODE


def test_srt_credentials_are_scoped_to_agent_process(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://x")
    monkeypatch.setenv("GH_TOKEN", "ghp_secret")
    monkeypatch.setenv("ISLO_API_KEY", "islo_secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws_secret")
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("UV_CACHE_DIR", "/runner-temp/setup-uv-cache")
    seen = _fake_srt(monkeypatch)

    _srt(tmp_path).run("true")
    env = seen["kwargs"]["env"]
    assert env["UV_CACHE_DIR"] == str((tmp_path / "work" / ".factory" / "uv-cache").resolve())
    assert "ANTHROPIC_API_KEY" not in env and "ANTHROPIC_BASE_URL" not in env
    for k in ("GH_TOKEN", "ISLO_API_KEY", "AWS_SECRET_ACCESS_KEY"):
        assert k not in env
    assert env["PATH"] == "/usr/bin"

    credentialed = _srt(tmp_path, pass_env=("ANTHROPIC_API_KEY",))
    credentialed.run("true")
    assert "ANTHROPIC_API_KEY" not in seen["kwargs"]["env"]  # tests/lifecycle stay keyless

    credentialed.run_agent("true")
    env = seen["kwargs"]["env"]
    assert env["ANTHROPIC_API_KEY"] == "sk-ant-secret"  # only the model process gets it
    assert "ANTHROPIC_BASE_URL" not in env  # pass_env is exact keys, not a prefix
    for k in ("GH_TOKEN", "ISLO_API_KEY", "AWS_SECRET_ACCESS_KEY"):
        assert k not in env
    joined = " ".join(seen["argv"])
    assert "sk-ant-secret" not in joined and "ghp_secret" not in joined  # never in argv

    monkeypatch.delenv("ANTHROPIC_API_KEY")
    _srt(tmp_path, pass_env=("ANTHROPIC_API_KEY",)).run_agent("true")
    assert "ANTHROPIC_API_KEY" not in seen["kwargs"]["env"]  # absent on host -> not invented


def test_srt_is_a_local_sandbox_for_files(tmp_path) -> None:
    sb = _srt(tmp_path)
    assert isinstance(sb, LocalSandbox) and isinstance(sb, Sandbox)
    sb.write("docs/x.md", "# x\n")  # orchestrator-side file access is unconfined
    assert sb.exists("docs/x.md") and sb.read("docs/x.md") == "# x\n"
    sb.close()


# ---------------------------------------------------------------- scrub_env / protocol / factory


def test_scrub_env_drops_credentials_keeps_basics() -> None:
    env = {
        "PATH": "/usr/bin",
        "HOME": "/home/u",
        "ANTHROPIC_API_KEY": "x",
        "ANTHROPIC_BASE_URL": "x",
        "GH_TOKEN": "x",
        "GITHUB_TOKEN": "x",
        "AWS_SECRET_ACCESS_KEY": "x",
        "AWS_ACCESS_KEY_ID": "x",
        "ISLO_API_KEY": "x",
        "CI": "1",
    }
    out = scrub_env(env)
    assert out == {"PATH": "/usr/bin", "HOME": "/home/u", "CI": "1"}
    assert "ANTHROPIC_" in SCRUB_PREFIXES


def test_both_implementations_satisfy_protocol(tmp_path) -> None:
    assert isinstance(LocalSandbox(tmp_path), Sandbox)
    assert isinstance(_islo(), Sandbox)


def test_make_sandbox_local(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = Config(issue="demo/issue.md", sandbox="local", workdir=".factory/work")
    sb = make_sandbox(cfg, "DEMO-1")
    assert isinstance(sb, LocalSandbox)
    assert sb.workdir == str((tmp_path / ".factory/work").resolve())


def test_make_sandbox_srt(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = Config(
        issue="42",
        sandbox="srt",
        agent="claude",
        workdir=".factory/work",
        srt_allowed_domains=["api.anthropic.com", "example.org"],
    )
    sb = make_sandbox(cfg, "42", protected=("tests/",))
    assert isinstance(sb, SrtSandbox)
    assert sb.workdir == str((tmp_path / ".factory/work").resolve())
    assert sb.pass_env == ("ANTHROPIC_API_KEY",)  # agent=claude needs the real key
    assert sb.protected == ("tests/",)
    Path(sb.workdir, "tests").mkdir(parents=True, exist_ok=True)
    assert f"{sb.workdir}/tests" in sb.settings()["filesystem"]["denyWrite"]
    assert "example.org" in sb.settings()["network"]["allowedDomains"]
    scripted = make_sandbox(Config(issue="42", sandbox="srt"), "42")
    assert isinstance(scripted, SrtSandbox) and scripted.pass_env == ()


def test_make_sandbox_islo_snapshot_and_repo_in_name() -> None:
    cfg = Config(issue="42", sandbox="islo", islo_snapshot="swf-golden-1")
    sb = make_sandbox(cfg, "42", repo="acme/widgets")
    assert isinstance(sb, IsloSandbox)
    assert sb.name == cfg.sandbox_name("42", "acme/widgets")
    assert "-widgets-" in sb.name
    argv = sb.argv("true", create=True)
    assert argv[argv.index("--snapshot") + 1] == "swf-golden-1"
    _assert_no_credentials(argv)


def test_make_sandbox_islo_wires_config() -> None:
    cfg = Config(
        issue="42",
        sandbox="islo",
        repo="acme/widgets",
        base_branch="develop",
        target_dir="",
        gateway_profile="gw",
        islo_environment="envx",
        sandbox_ttl_s=200_000,
        sandbox_idle_s=60,
    )
    sb = make_sandbox(cfg, "42")
    assert isinstance(sb, IsloSandbox)
    assert sb.name == cfg.sandbox_name("42")
    assert sb.name.startswith("swf-42-")
    assert sb.workdir == "/workspace/widgets"
    argv = sb.argv("true", create=True)
    assert argv[argv.index("--source") + 1] == "github://acme/widgets:develop"
    assert argv[argv.index("--gateway-profile") + 1] == "gw"
    assert argv[argv.index("--environment") + 1] == "envx"
    assert argv[argv.index("--delete-after") + 1] == "200000"
    assert argv[argv.index("--pause-after-idle") + 1] == "60"
    _assert_no_credentials(argv)


# ---------------------------------------------------------------- DockerSandbox (fake docker)


def _docker(tmp_path: Path, **overrides) -> DockerSandbox:
    kwargs = dict(image="swfactory-sandbox:test", pass_env=(), protected=())
    kwargs.update(overrides)
    return DockerSandbox(tmp_path / "work", **kwargs)


def _e_flags(argv: list[str]) -> list[str]:
    return [argv[i + 1] for i, a in enumerate(argv) if a == "-e"]


def test_docker_argv_shape_mounts_workdir_in_place(tmp_path, monkeypatch) -> None:
    seen = _fake_srt(monkeypatch)
    sb = _docker(tmp_path)
    assert sb.name == "docker:work"
    res = sb.run("uv run pytest")
    argv, w = seen["argv"], sb.workdir
    assert res.ok
    assert argv[:6] == ["docker", "run", "--rm", "--init", "-v", f"{w}:{w}"]  # rw, same path
    assert f"{DOCKER_CACHE_VOLUME}:{DOCKER_HOME}/.cache" in argv
    assert argv[argv.index("-w") + 1] == w
    assert argv[argv.index("--network") + 1] == "bridge"
    assert "--user" not in argv
    assert _e_flags(argv) == [f"{k}={v}" for k, v in sandbox_mod.DOCKER_GIT_ENV]  # git only
    assert argv[-4:] == ["swfactory-sandbox:test", "bash", "-lc", "uv run pytest"]
    assert not any(a.endswith(":ro") for a in argv)  # nothing protected exists yet
    assert seen["kwargs"]["cwd"] == sb.root
    _assert_no_credentials(argv)


def test_docker_argv_cwd_network_user(tmp_path, monkeypatch) -> None:
    seen = _fake_srt(monkeypatch)
    sb = _docker(tmp_path, network="none", user="1000:1000")
    sb.run("true", cwd=f"{sb.workdir}/sub dir")
    argv = seen["argv"]
    assert argv[argv.index("-w") + 1] == f"{sb.workdir}/sub dir"  # argv token, no quoting
    assert argv[argv.index("--network") + 1] == "none"
    assert argv[argv.index("--user") + 1] == "1000:1000"
    assert argv.index("--user") < argv.index("swfactory-sandbox:test")  # options before image


def test_docker_protected_prefixes_mounted_ro_only_when_present(tmp_path, monkeypatch) -> None:
    seen = _fake_srt(monkeypatch)
    sb = _docker(tmp_path, protected=("tests/", "src/**/*.py", "factory.toml", "*.lock"))
    (sb.root / "tests").mkdir(parents=True)
    (sb.root / ".github").mkdir()
    (sb.root / "factory.toml").write_text("[commands]\ntest='true'\n")
    sb.run("true")
    argv = seen["argv"]
    ro = [a for a in argv if a.endswith(":ro")]
    r = sb.root
    # fixed (.github, like srt) + literal prefixes that exist; src/ is absent and *.lock has none
    assert set(ro) == {
        f"{r}/.github:{r}/.github:ro",
        f"{r}/tests:{r}/tests:ro",
        f"{r}/factory.toml:{r}/factory.toml:ro",
    }
    assert argv.index(f"{r}:{r}") < min(argv.index(m) for m in ro)  # rw parent mounted first
    assert all(argv[argv.index(m) - 1] == "-v" for m in ro)

    sb.set_protected(("factory.toml",))  # a fix call re-opens tests/ (protected_for narrowing)
    sb.run("true")
    ro = [a for a in seen["argv"] if a.endswith(":ro")]
    assert set(ro) == {f"{r}/.github:{r}/.github:ro", f"{r}/factory.toml:{r}/factory.toml:ro"}


def test_docker_credentials_are_scoped_to_agent_process_and_passed_by_name(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://x")
    monkeypatch.setenv("GH_TOKEN", "ghp_secret")
    monkeypatch.setenv("ISLO_API_KEY", "islo_secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws_secret")
    monkeypatch.setenv("DOCKER_HOST", "unix:///var/run/docker.sock")
    monkeypatch.setenv("PATH", "/usr/bin")
    seen = _fake_srt(monkeypatch)

    _docker(tmp_path).run("true")
    env = seen["kwargs"]["env"]
    assert "ANTHROPIC_API_KEY" not in env and "ANTHROPIC_BASE_URL" not in env
    for k in ("GH_TOKEN", "ISLO_API_KEY", "AWS_SECRET_ACCESS_KEY"):
        assert k not in env
    assert env["PATH"] == "/usr/bin" and env["DOCKER_HOST"] == "unix:///var/run/docker.sock"
    _assert_no_credentials(seen["argv"])  # no -e ANTHROPIC_* without pass_env

    credentialed = _docker(tmp_path, pass_env=("ANTHROPIC_API_KEY",))
    credentialed.run("true")
    assert "ANTHROPIC_API_KEY" not in seen["kwargs"]["env"]
    assert "ANTHROPIC_API_KEY" not in _e_flags(seen["argv"])

    credentialed.run_agent("true")
    argv, env = seen["argv"], seen["kwargs"]["env"]
    assert env["ANTHROPIC_API_KEY"] == "sk-ant-secret"  # only the model container gets it
    assert "ANTHROPIC_BASE_URL" not in env
    assert "ANTHROPIC_API_KEY" in _e_flags(argv)  # by NAME: docker copies it from its env
    joined = " ".join(argv)
    assert "sk-ant-secret" not in joined and "ghp_secret" not in joined  # values never in argv
    assert "GH_TOKEN" not in joined

    monkeypatch.delenv("ANTHROPIC_API_KEY")
    _docker(tmp_path, pass_env=("ANTHROPIC_API_KEY",)).run_agent("true")
    assert "ANTHROPIC_API_KEY" not in seen["kwargs"]["env"]  # absent on host -> not invented
    assert "ANTHROPIC_API_KEY" not in _e_flags(seen["argv"])


def test_docker_host_login_is_mounted_only_for_agent_process_when_present(
    tmp_path, monkeypatch
) -> None:
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    monkeypatch.setattr(sandbox_mod.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    seen = _fake_srt(monkeypatch)

    credentialed = _docker(tmp_path, credentials="host")
    credentialed.run("true")
    assert not any(str(home) in a for a in seen["argv"])  # target commands see no login

    credentialed.run_agent("true")
    argv = seen["argv"]
    assert f"{home}/.claude:{DOCKER_HOME}/.claude" in argv  # rw: Claude writes its session there
    assert not any(a.endswith("/.claude.json") for a in argv)  # absent on host -> not mounted
    assert "ANTHROPIC_API_KEY" not in seen["kwargs"]["env"]  # host mode: pass_env empty

    (home / ".claude.json").write_text("{}")
    _docker(tmp_path, credentials="host").run_agent("true")
    assert f"{home}/.claude.json:{DOCKER_HOME}/.claude.json" in seen["argv"]

    _docker(tmp_path).run("true")  # env mode: nothing from $HOME crosses
    assert not any(str(home) in a for a in seen["argv"])


def test_docker_run_propagates_rc_and_timeout(tmp_path, monkeypatch) -> None:
    seen = _fake_srt(monkeypatch, rc=3)
    res = _docker(tmp_path).run("exit 3")
    assert res.exit_code == 3 and not res.ok and res.stdout == "out\n"
    assert seen["argv"][0] == "docker"

    def boom(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs.get("timeout"))

    monkeypatch.setattr(sandbox_mod.subprocess, "run", boom)
    res = _docker(tmp_path).run("sleep 999", timeout_s=1)
    assert res.timed_out and res.exit_code == sandbox_mod.TIMEOUT_EXIT_CODE


def test_docker_ensure_inits_git_host_side_then_runs_in_container(tmp_path, monkeypatch) -> None:
    """Like srt: `git init` is the orchestrator's own action on the host; run() is a container."""
    calls: list[list[str]] = []
    real_run = subprocess.run

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        if argv[0] == "bash":
            return real_run(argv, **kwargs)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    sb = _docker(tmp_path)
    sb.ensure()
    assert (sb.root / ".git").is_dir()
    assert calls[0][:2] == ["bash", "-lc"] and "git init" in calls[0][2]
    sb.ensure()  # idempotent: no second git init
    assert len(calls) == 1
    sb.run("true")
    assert calls[-1][:2] == ["docker", "run"]


def test_docker_ensure_raises_on_git_init_failure(tmp_path, monkeypatch) -> None:
    def fail(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 128, stdout="", stderr="fatal: nope")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fail)
    with pytest.raises(StageError, match="git init failed"):
        _docker(tmp_path).ensure()


def test_docker_is_a_local_sandbox_for_files(tmp_path) -> None:
    sb = _docker(tmp_path)
    assert isinstance(sb, LocalSandbox) and isinstance(sb, Sandbox)
    sb.write("docs/x.md", "# x\n")  # orchestrator-side file access is the host path
    assert sb.exists("docs/x.md") and sb.read("docs/x.md") == "# x\n"
    assert (tmp_path / "work" / "docs" / "x.md").exists()
    sb.close()


def test_make_sandbox_docker(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert "docker" in HOST_SANDBOXES  # workdir is seeded on the host, LocalGitScm base repo
    cfg = Config(
        issue="42",
        sandbox="docker",
        agent="claude",  # allowed without --allow-local-agent
        workdir=".factory/work",
        docker_image="ghcr.io/acme/sbx:1",
        docker_network="none",
        docker_user="0:0",
    )
    sb = make_sandbox(cfg, "42", protected=("tests/",))
    assert isinstance(sb, DockerSandbox)
    assert sb.workdir == str((tmp_path / ".factory/work").resolve())
    assert sb.image == "ghcr.io/acme/sbx:1"
    assert sb.pass_env == ("ANTHROPIC_API_KEY",) and sb.credentials == "env"
    assert sb.protected == ("tests/",) and sb.network == "none" and sb.user == "0:0"

    host = make_sandbox(Config(issue="42", sandbox="docker", docker_credentials="host"), "42")
    assert isinstance(host, DockerSandbox) and host.credentials == "host" and host.pass_env == ()
    host = make_sandbox(
        Config(issue="42", sandbox="docker", agent="claude", docker_credentials="host"), "42"
    )
    assert host.pass_env == ()  # host login OR api key, never both

    scripted = make_sandbox(Config(issue="42", sandbox="docker"), "42")
    assert isinstance(scripted, DockerSandbox) and scripted.pass_env == ()
    assert scripted.image == "ghcr.io/zozo123/swfactory-sandbox:latest"
    assert scripted.network == "bridge"
    assert scripted.user == sandbox_mod.default_docker_user()  # host uid on Linux, None on macOS


def test_config_docker_fields_and_validators() -> None:
    cfg = Config(issue="42", sandbox="docker")
    assert cfg.docker_credentials == "env" and cfg.docker_network == "bridge"
    assert cfg.docker_user is None
    with pytest.raises(ValueError):
        Config(issue="42", sandbox="docker", docker_credentials="keychain")
    with pytest.raises(ValueError, match="crabbox"):
        Config(issue="42", sandbox="docker", tests="crabbox")


def test_srt_denies_only_existing_paths(tmp_path, monkeypatch) -> None:
    """bubblewrap (Linux) refuses deny rules for paths that do not exist yet."""
    sb = sandbox_mod.SrtSandbox(tmp_path, allowed_domains=(), protected=("tests/",))
    deny = sb.settings()["filesystem"]["denyWrite"]
    assert str(tmp_path / ".claude") not in deny and str(tmp_path / "tests") not in deny
    (tmp_path / ".claude").mkdir()
    (tmp_path / "tests").mkdir()
    deny = sb.settings()["filesystem"]["denyWrite"]
    assert str(tmp_path / ".claude") in deny and str(tmp_path / "tests") in deny


def test_docker_custom_uid_gets_tmp_home_and_no_cache_volume(tmp_path) -> None:
    sb = sandbox_mod.DockerSandbox(tmp_path, image="img", user="1001:1001")
    argv = sb.argv("uv run pytest")
    assert "--user" in argv and "1001:1001" in argv
    assert f"HOME={sandbox_mod.DOCKER_TMP_HOME}" in argv
    assert not any(a.startswith(sandbox_mod.DOCKER_CACHE_VOLUME) for a in argv)
    assert argv[-1].startswith('mkdir -p "$HOME" && uv run pytest')
    root_sb = sandbox_mod.DockerSandbox(tmp_path, image="img", user="root")
    assert "HOME=" not in " ".join(root_sb.argv("true"))


def test_default_docker_user_is_host_uid_on_linux(monkeypatch) -> None:
    monkeypatch.setattr(sandbox_mod.sys, "platform", "linux")
    assert sandbox_mod.default_docker_user() == f"{os.getuid()}:{os.getgid()}"
    monkeypatch.setattr(sandbox_mod.sys, "platform", "darwin")
    assert sandbox_mod.default_docker_user() is None


# ------------------------------------------------- ToolsetSandbox (Airflow common.ai provider)


class FakeBackend:
    """Stand-in for Airflow's SandboxBackend: records calls, no Airflow, no network."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.files: dict[str, bytes] = {}
        self.destroyed: list[str] = []
        self.rc = 0

    def create(self, *, spec=None):
        self.calls.append(("create", spec))
        return "sbx-1"

    def run_command(self, sandbox, command, *, timeout, max_output_bytes):
        self.calls.append(("run", sandbox, command, timeout, max_output_bytes))

        class R:
            exit_code = self.rc
            stdout = "out"
            stderr = ""
            timed_out = False

        return R()

    def read_file(self, sandbox, path, *, max_bytes):
        self.calls.append(("read", sandbox, path))
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def write_file(self, sandbox, path, content):
        self.calls.append(("write", sandbox, path))
        self.files[path] = content

    def destroy(self, sandbox):
        self.destroyed.append(sandbox)


def _toolset(**kw):
    be = FakeBackend()
    return be, sandbox_mod.ToolsetSandbox(be, workdir="/workspace/target", **kw)


def test_toolset_creates_once_and_carries_cwd_in_the_command() -> None:
    be, sb = _toolset()
    sb.ensure()
    sb.ensure()  # idempotent: one create, the mkdir may repeat
    assert [c for c in be.calls if c[0] == "create"] == [("create", be.calls[0][1])]
    res = sb.run("uv run pytest", cwd="/workspace/target/demo")
    assert res.exit_code == 0 and res.stdout == "out"
    run_cmds = [c[2] for c in be.calls if c[0] == "run"]
    assert "cd /workspace/target/demo && uv run pytest" in run_cmds


def test_toolset_read_write_exists_and_close() -> None:
    be, sb = _toolset()
    sb.write("docs/factory/1/spec.md", "hello")
    assert be.files["/workspace/target/docs/factory/1/spec.md"] == b"hello"
    assert sb.read("docs/factory/1/spec.md") == "hello"
    with pytest.raises(FileNotFoundError):
        sb.read("missing.md")
    assert sb.exists("anything") is True  # fake backend returns rc 0
    sb.close()
    assert be.destroyed == ["sbx-1"]
    sb.close()  # second close is a no-op, never raises


def test_toolset_backend_registry_and_clear_error_for_pending_prs() -> None:
    assert set(sandbox_mod.TOOLSET_BACKENDS) == {"sbx", "islo", "opensandbox", "asciibox"}
    with pytest.raises(StageError) as e:
        sandbox_mod.load_toolset_backend("nope")
    assert "unknown toolset backend" in str(e.value)


def test_make_sandbox_toolset(monkeypatch) -> None:
    seen = {}

    def fake_load(name, **kw):
        seen["name"] = name
        return FakeBackend()

    monkeypatch.setattr(sandbox_mod, "load_toolset_backend", fake_load)
    cfg = Config(issue="42", sandbox="toolset", toolset_backend="sbx")
    sb = make_sandbox(cfg, "42")
    assert isinstance(sb, sandbox_mod.ToolsetSandbox)
    assert seen["name"] == "sbx" and sb.repo_root == cfg.toolset_workdir
    assert sb.workdir == f"{cfg.toolset_workdir}/demo/target"


def test_toolset_backend_names_match_the_upstream_prs() -> None:
    """The pending backends must name the module and class those PRs actually add.

    Guessed names look fine until the PR merges and the import still fails. Checked against the
    diffs: #71676 defines OpenSandboxBackend (not OpenSandboxSandboxBackend) and #71725 adds
    sandbox/ascii_box.py (underscore), not asciibox.py.
    """
    assert sandbox_mod.TOOLSET_BACKENDS["opensandbox"] == (
        "airflow.providers.common.ai.sandbox.opensandbox",
        "OpenSandboxBackend",
        71676,
    )
    assert sandbox_mod.TOOLSET_BACKENDS["asciibox"] == (
        "airflow.providers.common.ai.sandbox.ascii_box",
        "AsciiBoxSandboxBackend",
        71725,
    )
    assert sandbox_mod.TOOLSET_BACKENDS["islo"][2] == 71672
    assert sandbox_mod.TOOLSET_BACKENDS["sbx"][2] is None  # released, no PR to name


def test_toolset_unavailable_backend_names_its_pull_request() -> None:
    with pytest.raises(StageError) as e:
        sandbox_mod.load_toolset_backend("opensandbox")
    assert "apache/airflow#71676" in str(e.value)


def test_toolset_surfaces_truncation_and_termination() -> None:
    """Dropping these flags would let truncated output or a dead sandbox read as a clean result."""
    be, sb = _toolset()
    sb.sandbox_id = "sbx-1"  # isolate result mapping from provisioning

    class R:
        exit_code = 0
        stdout = "partial"
        stderr = ""
        timed_out = False
        stdout_truncated = True
        stderr_truncated = False
        sandbox_terminated = True

    be.run_command = lambda *a, **k: R()
    res = sb.run("pytest")
    assert res.exit_code == 1 and res.timed_out is True
    assert "stdout truncated" in res.stderr and "sandbox terminated" in res.stderr
