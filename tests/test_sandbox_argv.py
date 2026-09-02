"""Sandbox credential invariants and exit-code semantics. Hermetic: no islo, no network."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from swfactory import sandbox as sandbox_mod
from swfactory.config import Config
from swfactory.models import RunResult
from swfactory.sandbox import (
    SCRUB_PREFIXES,
    IsloSandbox,
    LocalSandbox,
    Sandbox,
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


def test_islo_close_runs_rm(monkeypatch) -> None:
    seen: list[list[str]] = []

    def fake_run(argv, **kwargs):
        seen.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    _islo().close()
    assert seen == [["islo", "rm", "swf-demo-1-abcd1234", "--output", "plain"]]


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
