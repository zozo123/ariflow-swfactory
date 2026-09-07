"""Contract fixtures: the pure functions the Rust port mirrors, pinned as data.

``tests/fixtures/contract/*.json`` is the one artefact both halves of the migration read. This
module asserts the *Python* still answers ``expected``; ``rust/crates/swf-domain/tests/contract.rs``
asserts the *Rust* does. Neither language can drift without a red build, and a reviewer reads the
fixtures instead of two implementations.

A fixture file naming a function this module cannot dispatch fails loudly, and so does a dispatch
entry with no fixture file: a silent skip is exactly how the two halves would drift apart while the
suite stayed green. The cases are deliberately adversarial — empty inputs, all-``None`` states,
falsy-but-present values, mixed check shapes, even-length medians, unicode whitespace — because the
happy paths were never the ones a reimplementation gets wrong.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from swfactory.control import (
    NO_ISSUE,
    Gate,
    JobRow,
    PullRequest,
    Run,
    Sandbox,
    Snapshot,
    TaskState,
    group_jobs,
    job_state,
    summarize_checks,
)
from swfactory.herd import job_index, parse_issues, snapshot_data, stage_progress
from swfactory.metrics import summarize

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "contract"


# ---------------------------------------------------------------- JSON -> dataclasses


def _dt(value: Any) -> datetime | None:
    """Fixture timestamps are ISO strings so the file stays diffable; the code wants datetimes."""
    if value is None:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _task(d: dict) -> TaskState:
    return TaskState(task_id=d["task_id"], map_index=int(d["map_index"]), state=d.get("state"))


def _job_row(d: dict) -> JobRow:
    return JobRow(
        dag_id=d["dag_id"],
        run_id=d["run_id"],
        map_index=int(d["map_index"]),
        issue=d.get("issue", NO_ISSUE),
        state=d.get("state", "queued"),
        tasks=[_task(t) for t in d.get("tasks") or []],
    )


def _run(d: dict) -> Run:
    return Run(
        dag_id=d["dag_id"],
        run_id=d["run_id"],
        state=d["state"],
        start=_dt(d.get("start")),
        end=_dt(d.get("end")),
        conf=d.get("conf") if isinstance(d.get("conf"), dict) else {},
        jobs=[_job_row(j) for j in d.get("jobs") or []],
    )


def _gate(d: dict) -> Gate:
    return Gate(
        dag_id=d["dag_id"],
        run_id=d["run_id"],
        task_id=d["task_id"],
        map_index=int(d["map_index"]),
        subject=d.get("subject", ""),
        body=d.get("body", ""),
        created_at=_dt(d.get("created_at")),
        options=d.get("options"),
    )


def _pr(d: dict) -> PullRequest:
    return PullRequest(
        number=int(d["number"]),
        title=d.get("title", ""),
        url=d.get("url", ""),
        labels=d.get("labels"),
        state=d.get("state", ""),
        checks=d.get("checks", ""),
        head=d.get("head", ""),
    )


def _sandbox(d: dict) -> Sandbox:
    return Sandbox(
        name=d["name"],
        status=d.get("status", ""),
        created_by=d.get("created_by", ""),
        created_at=_dt(d.get("created_at")),
    )


def _snapshot(d: dict) -> Snapshot:
    return Snapshot(
        collected_at=_dt(d.get("collected_at")),
        runs=[_run(r) for r in d.get("runs") or []],
        gates=[_gate(g) for g in d.get("gates") or []],
        prs=[_pr(p) for p in d.get("prs") or []],
        sandboxes=[_sandbox(s) for s in d.get("sandboxes") or []],
        metrics=d.get("metrics") or {},
        errors=d.get("errors") or {},
    )


def _job_row_json(row: JobRow) -> dict:
    """A ``JobRow`` flattened back to JSON so ``expected`` stays readable in review."""
    return {
        "dag_id": row.dag_id,
        "run_id": row.run_id,
        "map_index": row.map_index,
        "issue": row.issue,
        "state": row.state,
        "tasks": [{"task_id": t.task_id, "map_index": t.map_index, "state": t.state} for t in row.tasks],
    }


# ---------------------------------------------------------------- dispatch


def _call_job_state(data: dict) -> Any:
    return job_state([_task(t) for t in data["tasks"]])


def _call_group_jobs(data: dict) -> Any:
    rows = group_jobs(
        data["dag_id"],
        data["run_id"],
        [_task(t) for t in data["tasks"]],
        data.get("fan_out") or (),
        fallback_issues=data.get("fallback_issues") or (),
    )
    return [_job_row_json(r) for r in rows]


def _call_stage_progress(data: dict) -> Any:
    return stage_progress([_task(t) for t in data["tasks"]])


def _call_summarize_checks(data: dict) -> Any:
    return summarize_checks(data["rollup"])


def _call_parse_issues(data: dict) -> Any:
    return parse_issues(data["text"])


def _call_job_index(data: dict) -> Any:
    return job_index(data["map_index"])


def _call_summarize(data: dict) -> Any:
    return summarize(data["runs"])


def _call_snapshot_json(data: dict) -> Any:
    return snapshot_data(_snapshot(data["snapshot"]))


DISPATCH = {
    "job_state": _call_job_state,
    "group_jobs": _call_group_jobs,
    "stage_progress": _call_stage_progress,
    "summarize_checks": _call_summarize_checks,
    "parse_issues": _call_parse_issues,
    "job_index": _call_job_index,
    "summarize": _call_summarize,
    "snapshot_json": _call_snapshot_json,
}


# ---------------------------------------------------------------- loading


def _fixture_files() -> list[Path]:
    return sorted(FIXTURE_DIR.glob("*.json"))


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _cases() -> list[tuple[str, str, dict]]:
    """``(function, case name, case)`` for every dispatchable fixture, flattened for ``ids``."""
    out: list[tuple[str, str, dict]] = []
    for path in _fixture_files():
        doc = _load(path)
        name = doc.get("function")
        if name not in DISPATCH:
            continue  # reported by test_every_fixture_is_dispatchable, not silently swallowed
        out.extend((name, case["name"], case) for case in doc["cases"])
    return out


CASES = _cases()


# ---------------------------------------------------------------- tests


def test_every_fixture_is_dispatchable() -> None:
    """A fixture nobody can run is worse than no fixture: it looks like coverage and is not."""
    assert _fixture_files(), f"no contract fixtures under {FIXTURE_DIR}"
    on_disk = {}
    for path in _fixture_files():
        doc = _load(path)
        assert doc["function"] == path.stem, f"{path.name} declares function {doc['function']!r}"
        assert doc.get("doc"), f"{path.name} has no doc line saying what it pins"
        assert doc["cases"], f"{path.name} has no cases"
        on_disk[doc["function"]] = path.name
    unknown = sorted(set(on_disk) - set(DISPATCH))
    assert not unknown, f"fixtures for functions this module cannot call: {unknown}"
    missing = sorted(set(DISPATCH) - set(on_disk))
    assert not missing, f"dispatchable functions with no fixture file: {missing}"


@pytest.mark.parametrize("path", _fixture_files(), ids=lambda p: p.stem)
def test_case_names_are_unique(path: Path) -> None:
    names = [c["name"] for c in _load(path)["cases"]]
    assert len(names) == len(set(names)), f"{path.name} repeats a case name"
    assert len(names) >= 8, f"{path.name} has {len(names)} cases; the contract wants at least 8"


@pytest.mark.parametrize(("function", "case_name", "case"), CASES, ids=[f"{f}-{n}" for f, n, _ in CASES])
def test_python_still_matches_the_contract(function: str, case_name: str, case: dict) -> None:
    assert DISPATCH[function](case["input"]) == case["expected"], case_name
