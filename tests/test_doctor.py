"""doctor: hermetic pre-flight checks over a fake runner (canned islo/gh/claude output)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory import doctor
from swfactory.cli import app
from swfactory.config import Config
from swfactory.doctor import Check, exit_code, run_doctor, table

ROOT = Path(__file__).resolve().parents[1]

STATUS_JSON = {
    "auth": {"authenticated": True, "method": "login", "tenant_name": "guli-test", "tier": "free"},
    "integrations": [
        {"tool": "github", "status": "connected"},
        {"tool": "claude", "status": "connected"},
    ],
    "config": {"sandbox": "swfactory"},
}
GATEWAYS = [
    {"name": "default", "default_action": "allow", "internet_enabled": True, "rule_count": 0},
    {"name": "swfactory", "default_action": "deny", "internet_enabled": True, "rule_count": 6},
]
ENVIRONMENTS = [{"name": "swfactory", "is_default": False}]
SNAPSHOTS = [{"name": "swf-golden-20260902", "status": "ready"}]
GH_AUTH = (
    "github.com\n  ✓ Logged in to github.com account zozo123 (keyring)\n  - Active account: true\n"
)
STATUS_TEXT_NONE = (
    "Authentication\n  Status: Logged in\n\n"
    "Connected Integrations\n"
    "  No integrations connected (run 'islo login --tool github')\n"
    "  Integrations power Islo features, but sandbox CLIs such as 'gh' may still need...\n\n"
    "Project Configuration\n  Sandbox: swfactory\n"
)
STATUS_TEXT_BOTH = (
    "Connected Integrations\n  github: connected as zozo123\n  claude: connected\n\n"
    "Project Configuration\n"
)


class FakeRunner:
    """Canned stdout per argv (joined); unknown argv or ``None`` -> raises like a failed command."""

    def __init__(self, table: dict[str, str | None]) -> None:
        self.table = table
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str]) -> str:
        argv = list(argv)
        self.calls.append(argv)
        key = " ".join(argv)
        if key not in self.table:
            raise FileNotFoundError(argv[0])
        out = self.table[key]
        if out is None:
            raise doctor.DoctorCommandError(f"{key} exited 1: boom")
        return out


def green(**over: str | None) -> FakeRunner:
    base: dict[str, str | None] = {
        "islo --version": "islo 0.48.1\n",
        "islo status --output json": json.dumps(STATUS_JSON),
        "islo status": STATUS_TEXT_BOTH,
        "islo gateway ls --output json": json.dumps(GATEWAYS),
        "islo environment list --output json": json.dumps(ENVIRONMENTS),
        "islo snapshot ls --output json": json.dumps(SNAPSHOTS),
        "gh auth status": GH_AUTH,
        "gh repo view zozo123/ariflow-swfactory --json name": '{"name":"ariflow-swfactory"}\n',
        "claude --version": "2.1.259 (Claude Code)\n",
    }
    base.update(over)
    return FakeRunner(base)


def cfg(**kw: object) -> Config:
    return Config(**{"issue": "doctor", "sandbox": "islo", **kw})  # type: ignore[arg-type]


def which_all(name: str) -> str | None:
    return f"/usr/bin/{name}"


def which_none(name: str) -> str | None:
    return None


def by_name(checks: list[Check]) -> dict[str, Check]:
    return {c.name: c for c in checks}


# ---------------------------------------------------------------- all green


def test_all_green_exit_zero() -> None:
    runner = green()
    checks = run_doctor(
        cfg(islo_snapshot="swf-golden-20260902"), runner, which=which_all, root=ROOT
    )
    assert all(c.ok for c in checks), [c for c in checks if not c.ok]
    assert exit_code(checks) == 0
    names = [c.name for c in checks]
    assert names == [
        "islo cli",
        "islo auth",
        "integration github",
        "integration claude",
        "gateway profile",
        "islo environment",
        "islo snapshot",
        "gh auth",
        "gh repo",
        "claude cli",
        "srt",
        "blueprint",
        "factory.toml",
    ]
    # Exact argv the doctor issues (flags verified against islo 0.48.1 / gh 2.83).
    assert ["islo", "status", "--output", "json"] in runner.calls
    assert ["islo", "gateway", "ls", "--output", "json"] in runner.calls
    assert ["islo", "environment", "list", "--output", "json"] in runner.calls
    assert ["islo", "snapshot", "ls", "--output", "json"] in runner.calls
    assert ["gh", "auth", "status"] in runner.calls
    assert ["gh", "repo", "view", "zozo123/ariflow-swfactory", "--json", "name"] in runner.calls
    assert ["islo", "status"] not in runner.calls  # JSON had integrations: no text fallback
    for argv in runner.calls:
        assert "--env" not in argv and "--env-file" not in argv


def test_no_snapshot_check_when_unconfigured() -> None:
    runner = green()
    checks = run_doctor(cfg(), runner, which=which_all, root=ROOT)
    assert "islo snapshot" not in by_name(checks)
    assert ["islo", "snapshot", "ls", "--output", "json"] not in runner.calls


def test_table_and_json_shapes() -> None:
    checks = run_doctor(cfg(), green(), which=which_all, root=ROOT)
    text = table(checks)
    assert text.startswith("ok   islo cli")
    assert text.rstrip().endswith("12 checks, 0 failed")
    data = json.loads(doctor.to_json(checks))
    assert data[0] == {
        "name": "islo cli",
        "ok": True,
        "detail": "islo 0.48.1",
        "fix": "",
        "required": True,
        "status": "ok",
    }


# ---------------------------------------------------------------- failures carry fixes


def test_missing_gateway_profile() -> None:
    checks = run_doctor(
        cfg(), green(**{"islo gateway ls --output json": json.dumps(GATEWAYS[:1])}), root=ROOT
    )
    gw = by_name(checks)["gateway profile"]
    assert not gw.ok and gw.detail == "'swfactory' not found; have: default"
    assert gw.fix.startswith(
        "islo gateway create --name swfactory --default-action deny --internet-access true"
    )
    assert "add-rule --host <host> --action allow" in gw.fix
    for host in doctor.GATEWAY_ALLOW_HOSTS:
        assert host in gw.fix
    assert exit_code(checks) == 1
    assert "fix: islo gateway create" in table(checks)


def test_gateway_present_but_allow_by_default_is_flagged_in_detail() -> None:
    gws = [{"name": "swfactory", "default_action": "allow", "rule_count": 0}]
    checks = run_doctor(
        cfg(), green(**{"islo gateway ls --output json": json.dumps(gws)}), root=ROOT
    )
    gw = by_name(checks)["gateway profile"]
    assert gw.ok and "expected deny" in gw.detail


def test_missing_environment() -> None:
    checks = run_doctor(cfg(), green(**{"islo environment list --output json": "[]"}), root=ROOT)
    env = by_name(checks)["islo environment"]
    assert not env.ok and env.detail == "'swfactory' not found; have: none"
    assert env.fix == (
        "islo environment create --name swfactory --gateway-secret "
        "'ANTHROPIC_API_KEY=<real key>;host=api.anthropic.com;auth=bearer'"
    )
    assert exit_code(checks) == 1


def test_custom_profile_and_environment_names_flow_into_fixes() -> None:
    runner = green(
        **{"islo gateway ls --output json": "[]", "islo environment list --output json": "[]"}
    )
    checks = run_doctor(cfg(gateway_profile="gw-x", islo_environment="env-y"), runner, root=ROOT)
    assert "--name gw-x" in by_name(checks)["gateway profile"].fix
    assert "--name env-y" in by_name(checks)["islo environment"].fix


def test_missing_snapshot_when_configured_and_empty_stdout_means_none() -> None:
    # `islo snapshot ls --output json` prints nothing at all when there are no snapshots.
    checks = run_doctor(
        cfg(islo_snapshot="swf-golden-1"),
        green(**{"islo snapshot ls --output json": ""}),
        root=ROOT,
    )
    snap = by_name(checks)["islo snapshot"]
    assert not snap.ok and snap.detail == "'swf-golden-1' not found; have: none"
    assert "bootstrap.sh" in snap.fix
    assert exit_code(checks) == 1


def test_missing_integrations_from_json() -> None:
    status = {**STATUS_JSON, "integrations": [{"tool": "github", "status": "connected"}]}
    checks = run_doctor(
        cfg(), green(**{"islo status --output json": json.dumps(status)}), root=ROOT
    )
    got = by_name(checks)
    assert got["integration github"].ok
    claude = got["integration claude"]
    assert not claude.ok and claude.fix == "islo login --tool claude"
    assert claude.detail == "not connected; have: github"
    assert exit_code(checks) == 1


def test_disconnected_integration_does_not_count() -> None:
    status = {
        **STATUS_JSON,
        "integrations": [{"name": "github", "status": "expired"}, {"name": "anthropic"}],
    }
    checks = run_doctor(
        cfg(), green(**{"islo status --output json": json.dumps(status)}), root=ROOT
    )
    got = by_name(checks)
    assert not got["integration github"].ok and got["integration github"].fix == (
        "islo login --tool github"
    )
    assert got["integration claude"].ok  # `islo login --tool anthropic` counts as Claude


def test_integrations_fall_back_to_text_when_json_lacks_key() -> None:
    status = {k: v for k, v in STATUS_JSON.items() if k != "integrations"}
    runner = green(
        **{"islo status --output json": json.dumps(status), "islo status": STATUS_TEXT_BOTH}
    )
    checks = run_doctor(cfg(), runner, root=ROOT)
    got = by_name(checks)
    assert ["islo", "status"] in runner.calls
    assert got["integration github"].ok and got["integration github"].detail == "connected (text)"
    assert got["integration claude"].ok


def test_text_fallback_none_connected() -> None:
    status = {k: v for k, v in STATUS_JSON.items() if k != "integrations"}
    runner = green(
        **{"islo status --output json": json.dumps(status), "islo status": STATUS_TEXT_NONE}
    )
    got = by_name(run_doctor(cfg(), runner, root=ROOT))
    assert not got["integration github"].ok and got["integration github"].fix == (
        "islo login --tool github"
    )
    assert not got["integration claude"].ok and got["integration claude"].fix == (
        "islo login --tool claude"
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (STATUS_TEXT_NONE, set()),
        (STATUS_TEXT_BOTH, {"github", "claude"}),
        ("Connected Integrations\n  - GitHub (zozo123)\n\nOther\n  claude\n", {"github"}),
        ("no such section", set()),
    ],
)
def test_integration_names_from_text(text: str, expected: set[str]) -> None:
    assert doctor.integration_names_from_text(text) == expected


def test_integration_names_shapes() -> None:
    assert doctor.integration_names({"auth": {}}) is None
    assert doctor.integration_names({"integrations": ["GitHub", "claude"]}) == {"github", "claude"}
    assert doctor.integration_names({"integrations": {"github": True, "claude": False}}) == {
        "github"
    }
    assert (
        doctor.integration_names({"integrations": [{"provider": "github", "connected": False}]})
        == set()
    )
    assert doctor.integration_names("nope") is None


def test_not_authenticated() -> None:
    status = {**STATUS_JSON, "auth": {"authenticated": False}}
    got = by_name(
        run_doctor(cfg(), green(**{"islo status --output json": json.dumps(status)}), root=ROOT)
    )
    assert not got["islo auth"].ok and got["islo auth"].fix == "islo login"


def test_islo_missing_skips_dependent_checks_with_fixes() -> None:
    runner = green(**{"islo --version": None})
    runner.table.pop("islo --version")  # FileNotFoundError path
    checks = run_doctor(cfg(), runner, which=which_all, root=ROOT)
    got = by_name(checks)
    assert got["islo cli"].detail == "islo not found on PATH"
    for name in ("islo auth", "integration github", "integration claude", "gateway profile"):
        assert (
            not got[name].ok
            and got[name].detail == "skipped: islo CLI unavailable"
            and got[name].fix
        )
    assert got["islo environment"].fix.startswith("islo environment create --name swfactory")
    assert not any(argv[0] == "islo" and argv[1] != "--version" for argv in runner.calls)
    assert got["gh auth"].ok  # the non-islo checks still run
    assert exit_code(checks) == 1


def test_gh_failures() -> None:
    runner = green(
        **{"gh auth status": None, "gh repo view zozo123/ariflow-swfactory --json name": None}
    )
    got = by_name(run_doctor(cfg(), runner, root=ROOT))
    assert not got["gh auth"].ok and "gh auth login" in got["gh auth"].fix
    assert not got["gh repo"].ok and "zozo123/ariflow-swfactory" in got["gh repo"].fix


def test_claude_required_only_for_host_sandboxes() -> None:
    runner = green()
    runner.table.pop("claude --version")
    islo = by_name(run_doctor(cfg(), runner, root=ROOT))["claude cli"]
    assert not islo.ok and not islo.required and islo.status == "warn"
    srt = by_name(run_doctor(cfg(sandbox="srt"), runner, root=ROOT))["claude cli"]
    assert not srt.ok and srt.required and srt.status == "FAIL"
    assert exit_code(run_doctor(cfg(), runner, root=ROOT)) == 0
    assert exit_code(run_doctor(cfg(sandbox="srt"), runner, root=ROOT)) == 1


def test_srt_is_info_only() -> None:
    checks = run_doctor(cfg(), green(), which=which_none, root=ROOT)
    srt = by_name(checks)["srt"]
    assert not srt.ok and not srt.required and srt.status == "warn"
    assert exit_code(checks) == 0
    assert "1 warnings" in table(checks)


def test_blueprint_and_factory_toml_failures(tmp_path: Path) -> None:
    got = by_name(run_doctor(cfg(blueprint="nope", target_dir="elsewhere"), green(), root=tmp_path))
    assert not got["blueprint"].ok and "blueprints/nope.toml" in got["blueprint"].fix
    assert not got["factory.toml"].ok and got["factory.toml"].detail.endswith(
        "factory.toml missing"
    )
    bad = tmp_path / "t"
    bad.mkdir()
    (bad / "factory.toml").write_text("[commands]\nlint = 'x'\n", encoding="utf-8")
    ft = by_name(run_doctor(cfg(target_dir="t"), green(), root=tmp_path))["factory.toml"]
    assert not ft.ok and "[commands].test" in ft.detail


def test_subprocess_runner_contract() -> None:
    assert doctor.subprocess_runner(["python3", "-c", "print('hi')"]) == "hi\n"
    with pytest.raises(doctor.DoctorCommandError, match="exited 3"):
        doctor.subprocess_runner(["python3", "-c", "import sys; sys.exit(3)"])
    with pytest.raises(FileNotFoundError):
        doctor.subprocess_runner(["swf-no-such-binary-xyz"])


# ---------------------------------------------------------------- CLI


def test_cli_doctor_exit_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(doctor, "subprocess_runner", green())
    monkeypatch.chdir(ROOT)
    res = runner.invoke(app, ["doctor"])
    assert res.exit_code == 0, res.output
    assert "12 checks, 0 failed" in res.output
    assert "sandbox=islo" in res.output

    res = runner.invoke(app, ["doctor", "--json"])
    assert res.exit_code == 0, res.output
    assert [c["name"] for c in json.loads(res.output)][:2] == ["islo cli", "islo auth"]

    monkeypatch.setattr(
        doctor, "subprocess_runner", green(**{"islo environment list --output json": "[]"})
    )
    res = runner.invoke(app, ["doctor", "--blueprint", "hotfix"])
    assert res.exit_code == 1, res.output
    assert "fix: islo environment create --name swfactory" in res.output


def test_cli_doctor_broken_blueprint_still_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctor, "subprocess_runner", green())
    monkeypatch.chdir(ROOT)
    res = CliRunner().invoke(app, ["doctor", "--blueprint", "no-such-line"])
    assert res.exit_code == 1, res.output
    assert "FAIL blueprint" in res.output and "blueprints/no-such-line.toml" in res.output
