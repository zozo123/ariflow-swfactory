"""``swfactory.runtime``: the single wiring the CLI and every Airflow task share.

The Airflow half of the parity check needs the ``airflow`` group, so it lives next to the other
DAG tests: ``tests/test_dag_parity.py::test_dag_ctx_config_matches_runtime_job_config``. Both
sides compare against the same ``runtime.job_config`` call for the same (blueprint, job, run id).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from swfactory import cli, runtime
from swfactory.blueprint import load
from swfactory.models import RunReport
from swfactory.runtime import build_ctx, job_config, job_run_dir, run_id_for
from swfactory.sandbox import IsloSandbox, LocalSandbox
from swfactory.scm import LocalGitScm

ROOT = Path(__file__).resolve().parents[1]
ISSUE = "demo/issue.md"
# The scripted, all-local preset `swfactory demo` uses: no keys, no MicroVM, no network.
LOCAL = {"agent": "scripted", "sandbox": "local", "scm": "local", "approve": "auto"}
AIRFLOW_RUN_ID = "manual__2026-09-02T03:00:00+00:00"


@pytest.fixture
def job() -> dict:
    """The single job of the default blueprint for the demo issue (1 issue x 1 target)."""
    (one,) = load("factory").jobs({"issues": [ISSUE]})
    return one


# ---------------------------------------------------------------- run ids


def test_run_id_for_is_pure_hex8_and_job_sensitive() -> None:
    rid = run_id_for(AIRFLOW_RUN_ID)
    assert rid == run_id_for(AIRFLOW_RUN_ID, 0) == run_id_for(AIRFLOW_RUN_ID)  # retries re-attach
    assert len(rid) == 8 and int(rid, 16) >= 0
    assert len({rid, run_id_for(AIRFLOW_RUN_ID, 1), run_id_for(AIRFLOW_RUN_ID, 2)}) == 3
    assert run_id_for("scheduled__2026-09-02T03:00:00+00:00") != rid


# ---------------------------------------------------------------- job_config


def test_job_config_pins_every_path_to_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, job: dict
) -> None:
    monkeypatch.chdir(tmp_path)  # a worker's cwd is not the factory checkout
    cfg = job_config(load("factory"), job, run_id="pa7h0001", overrides=LOCAL, root=tmp_path)
    assert cfg.run_id == "pa7h0001" and cfg.blueprint == "factory"
    assert job_run_dir(cfg, tmp_path) == (tmp_path / ".factory" / "pa7h0001").resolve()
    # host sandbox => one workdir per run under the run dir, so parallel jobs never share it
    assert Path(cfg.workdir) == job_run_dir(cfg, tmp_path) / "work"
    # fixtures are written relative to the factory root; a worker's cwd is somewhere else
    assert Path(cfg.fixtures_dir) == ROOT / "demo" / "scripted"


def test_scripted_replay_falls_back_to_the_local_sandbox(job: dict) -> None:
    """A replay makes no model calls, so it never needs the blueprint's MicroVM."""
    bp = load("factory")
    assert bp.sandbox.kind == "islo"
    scripted = job_config(bp, job, run_id="fb000001", overrides={"agent": "scripted"})
    assert scripted.sandbox == "local"
    over = {"agent": "scripted", "sandbox": "srt"}
    assert job_config(bp, job, run_id="fb000001", overrides=over).sandbox == "srt"  # never guessed
    real = job_config(bp, job, run_id="fb000001", overrides={"agent": "claude"})
    assert real.sandbox == "islo"


# ---------------------------------------------------------------- build_ctx


def test_build_ctx_for_a_host_sandbox_seeds_the_workdir_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, job: dict
) -> None:
    """local/srt/docker work on a host checkout: it is seeded from the target dir, and its
    ``factory.toml`` read, before ``make_sandbox`` — srt turns those globs into kernel denyWrite
    from its very first command."""
    monkeypatch.chdir(tmp_path)
    seen: dict[str, object] = {}
    real = runtime.make_sandbox

    def spy(cfg, issue_id, **kw):
        seen.update(kw, issue_id=issue_id)
        return real(cfg, issue_id, **kw)

    monkeypatch.setattr(runtime, "make_sandbox", spy)
    ctx = build_ctx(load("factory"), job, run_id="l0cal001", overrides=LOCAL, root=tmp_path)

    work = (tmp_path / ".factory" / "l0cal001" / "work").resolve()
    assert ctx.run_dir == work.parent and ctx.run_dir.is_dir()
    assert (work / "factory.toml").is_file() and (work / "src").is_dir()  # seeded from demo/target
    assert seen == {"issue_id": "DEMO-1", "protected": ["factory.toml"], "repo": ctx.cfg.repo}
    assert isinstance(ctx.sb, LocalSandbox) and ctx.sb.root == work
    # the local "GitHub" is seeded from that same workdir, not from a public clone
    assert isinstance(ctx.scm, LocalGitScm)
    assert ctx.scm.base_repo == work and ctx.scm.seed_url is None
    assert ctx.issue.id == "DEMO-1" and ctx.agent.kind == "scripted"
    assert ctx.blueprint is not None and ctx.blueprint.name == "factory"
    assert ctx.cfg.target_dir == "demo/target" and ctx.cfg.base_branch == "main"


def test_build_ctx_for_islo_touches_nothing_on_the_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, job: dict
) -> None:
    """An islo run has no host checkout: nothing is seeded, the local remote is seeded from the
    public clone url instead, and the sandbox name carries the target repo so one issue applied
    to N targets gets N MicroVMs."""
    monkeypatch.chdir(tmp_path)
    ctx = build_ctx(
        load("factory"),
        job,
        run_id="1510aaaa",
        overrides={**LOCAL, "sandbox": "islo"},
        root=tmp_path,
    )
    assert isinstance(ctx.sb, IsloSandbox)
    assert ctx.sb.name.startswith("swf-demo-1-") and ctx.sb.name.endswith("-1510aaaa")
    assert ctx.cfg.repo.rsplit("/", 1)[-1] in ctx.sb.name
    assert ctx.cfg.workdir == ".factory/work"  # untouched default: no host workdir is used
    assert not (tmp_path / ".factory" / "1510aaaa" / "work").exists()
    assert isinstance(ctx.scm, LocalGitScm)
    assert ctx.scm.base_repo is None
    assert ctx.scm.seed_url == f"https://github.com/{ctx.cfg.repo}.git"


# ---------------------------------------------------------------- CLI parity


def test_cli_run_derives_its_config_with_job_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, job: dict
) -> None:
    """``swfactory run`` goes through ``build_ctx``, so the config it runs on is the one
    ``job_config`` derives from (blueprint, job, run id) — the DAG's half is pinned in
    tests/test_dag_parity.py."""
    monkeypatch.chdir(tmp_path)
    bp = load("factory")
    seen: list[tuple] = []
    monkeypatch.setattr(
        runtime, "ctx_for", lambda cfg, **kw: (seen.append((cfg, kw)), SimpleNamespace(cfg=cfg))[1]
    )
    report = RunReport(
        run_id="c1i00001",
        issue_id="DEMO-1",
        agent="scripted",
        sandbox="local",
        scm="local",
        stages=[],
        approvals=[],
        tests_passed=True,
    )
    monkeypatch.setattr(cli, "run_ctx", lambda ctx, *args: report)

    cli._run_jobs(bp, [ISSUE], {**LOCAL, "run_id": "c1i00001"})

    ((cfg, kw),) = seen
    assert cfg == job_config(bp, job, run_id="c1i00001", overrides=LOCAL)
    assert kw == {"blueprint": bp, "run_dir": job_run_dir(cfg), "agent": None}
