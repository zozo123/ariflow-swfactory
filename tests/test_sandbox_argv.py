"""Sandbox credential invariants and exit-code semantics. Hermetic: no islo, no network."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from swfactory import sandbox as sandbox_mod
from swfactory.config import Config
from swfactory.models import RunResult
from swfactory.sandbox import (
    SCRUB_PREFIXES,
    SRT_CLAUDE_DOMAINS,
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
    argv = _islo().argv("ls", cwd="/workspace/some dir")
    assert argv[-1].startswith("cd '/workspace/some dir' && ls")


def test_argv_never_carries_credentials_even_when_env_holds_them(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("GH_TOKEN", "ghp_secret")
    sb = _islo()
    for argv in (sb.argv("true", create=True), sb.argv("echo hi"), sb.argv("ls", cwd="/x")):
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
    sb = _srt(tmp_path, protected=("tests/", "src/**/*.py", "factory.toml", "*.lock"))
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
    assert fs["denyRead"] == [str(home / ".ssh"), str(home / ".aws"), str(home / ".config/gh")]
    assert net["allowedDomains"][:2] == ["api.anthropic.com", "pypi.org"]
    for d in SRT_CLAUDE_DOMAINS:
        assert net["allowedDomains"].count(d) == 1
    assert net["deniedDomains"] == []


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
    argv = sb.argv("ls", cwd="/some dir")
    assert argv[:3] == ["npx", "-y", "@anthropic-ai/sandbox-runtime"]
    assert argv[3:5] == ["-s", str(sb.settings_path)]
    assert argv[-1] == "cd '/some dir' && ls"
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


def test_srt_env_is_scrubbed_and_pass_env_is_explicit(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://x")
    monkeypatch.setenv("GH_TOKEN", "ghp_secret")
    monkeypatch.setenv("ISLO_API_KEY", "islo_secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws_secret")
    monkeypatch.setenv("PATH", "/usr/bin")
    seen = _fake_srt(monkeypatch)

    _srt(tmp_path).run("true")
    env = seen["kwargs"]["env"]
    assert "ANTHROPIC_API_KEY" not in env and "ANTHROPIC_BASE_URL" not in env
    for k in ("GH_TOKEN", "ISLO_API_KEY", "AWS_SECRET_ACCESS_KEY"):
        assert k not in env
    assert env["PATH"] == "/usr/bin"

    _srt(tmp_path, pass_env=("ANTHROPIC_API_KEY",)).run("true")
    env = seen["kwargs"]["env"]
    assert env["ANTHROPIC_API_KEY"] == "sk-ant-secret"  # the ONE allowed credential
    assert "ANTHROPIC_BASE_URL" not in env  # pass_env is exact keys, not a prefix
    for k in ("GH_TOKEN", "ISLO_API_KEY", "AWS_SECRET_ACCESS_KEY"):
        assert k not in env
    joined = " ".join(seen["argv"])
    assert "sk-ant-secret" not in joined and "ghp_secret" not in joined  # never in argv

    monkeypatch.delenv("ANTHROPIC_API_KEY")
    _srt(tmp_path, pass_env=("ANTHROPIC_API_KEY",)).run("true")
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
