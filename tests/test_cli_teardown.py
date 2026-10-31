"""The CLI closes its sandbox as the DAG's teardown does, so a rehearsal leaves no billable cell."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from swfactory import cli
from swfactory.models import StageError, StageResult


def _ctx(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(sb=SimpleNamespace(name="fake:cell"), run_dir=tmp_path)


@pytest.fixture
def closed(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    calls: list[object] = []
    monkeypatch.setattr(cli, "setup", lambda ctx: StageResult(stage="setup"))
    monkeypatch.setattr(cli, "teardown", calls.append)
    return calls


def test_a_finished_run_closes_its_sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closed: list) -> None:
    report = SimpleNamespace(model_dump_json=lambda indent: "{}")
    monkeypatch.setattr(cli, "run_pipeline", lambda ctx, approver: report)
    ctx = _ctx(tmp_path)

    assert cli.run_ctx(ctx) is report
    assert closed == [ctx] and (tmp_path / "report.json").read_text() == "{}\n"


def test_a_failed_run_still_closes_its_sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closed: list) -> None:
    def fail(ctx: object, approver: object) -> None:
        raise StageError("sandbox", "uv sync failed")

    monkeypatch.setattr(cli, "run_pipeline", fail)
    ctx = _ctx(tmp_path)

    with pytest.raises(StageError, match="uv sync failed"):
        cli.run_ctx(ctx)
    assert closed == [ctx]


def test_keep_sandbox_leaves_the_cell_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closed: list) -> None:
    monkeypatch.setattr(cli, "run_pipeline", lambda ctx, approver: SimpleNamespace(model_dump_json=lambda indent: "{}"))

    cli.run_ctx(_ctx(tmp_path), keep_sandbox=True)
    assert closed == []
