"""Read-only local run evidence. Inspection never reconnects to a work cell or calls an agent."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from swfactory.models import StageResult
from swfactory.paths import confined_path, validate_run_id
from swfactory.state import OPERATIONS_LOG, RunState


def inspect_run(root: Path, run_id: str, *, event_limit: int = 50) -> dict[str, Any]:
    """Inspect trusted host evidence, preserving unknown/corrupt values as explicit errors."""
    if not 1 <= event_limit <= 1000:
        raise ValueError("event_limit must be between 1 and 1000")
    run_id = validate_run_id(run_id)
    directory = confined_path(Path(root).expanduser().resolve(), run_id)
    state = RunState(directory)
    if not state.root.is_dir():
        raise FileNotFoundError(f"no saved state for run {run_id!r}")
    result: dict[str, Any] = {
        "run_id": run_id,
        "identity": None,
        "ownership": None,
        "stages": [],
        "recorded_cost_usd": None,
        "operations": [],
        "journals": {},
        "errors": {},
    }
    try:
        identity = json.loads(state.read_control("identity.json"))
        if not isinstance(identity, dict):
            raise ValueError("identity.json must contain an object")
        if identity.get("run_id") != run_id:
            raise ValueError("identity.json does not match the run directory")
        result["identity"] = identity
    except FileNotFoundError:
        pass  # A preparation attempt can fail before setup creates the identity.
    except (OSError, ValueError) as error:
        result["errors"]["identity"] = str(error)
    try:
        records = [StageResult.model_validate(row) for row in state.read_jsonl("stages.jsonl")]
        latest: dict[str, StageResult] = {}
        for record in records:
            if record.status != "skipped" or record.stage not in latest:
                latest[record.stage] = record
        result["stages"] = [record.model_dump(mode="json") for record in latest.values()]
        result["recorded_cost_usd"] = round(sum(record.cost_usd for record in records), 6)
    except (OSError, ValueError) as error:
        result["errors"]["stages"] = str(error)
    try:
        result["ownership"] = state.ownership()
        result["operations"] = state.operation_records()[-event_limit:]
    except (OSError, ValueError) as error:
        result["errors"]["operations"] = str(error)
    for name in ("stages.jsonl", OPERATIONS_LOG):
        try:
            result["journals"][name] = state.journal_status(name)
        except (OSError, ValueError) as error:
            result["errors"][name] = str(error)
    try:
        recovery = state._path("recovery")
        result["recovered_fragments"] = sorted(path.name for path in recovery.glob("*.tail") if path.is_file())
    except (OSError, ValueError) as error:
        result["errors"]["recovery"] = str(error)
    return result


def list_runs(root: Path, *, limit: int = 50) -> list[dict[str, Any]]:
    """Only visit actual run-state directories; never enter webhook DBs or unrelated scratch."""
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"run root does not exist: {root}")
    directories: list[tuple[float, str]] = []
    for directory in root.iterdir():
        if directory.is_symlink() or not directory.is_dir():
            continue
        try:
            validate_run_id(directory.name)
            state_dir = confined_path(directory, "state")
            if state_dir.is_symlink() or not state_dir.is_dir():
                continue
            # Journal append does not update its directory mtime, so rank on the evidence too.
            evidence = [state_dir, state_dir / "stages.jsonl", state_dir / OPERATIONS_LOG]
            changed = max(path.stat().st_mtime for path in evidence if path.exists())
        except (ValueError, OSError):
            continue  # A concurrently removed or non-run directory is not a discoverable run.
        directories.append((changed, directory.name))
    results: list[dict[str, Any]] = []
    for _, run_id in sorted(directories, reverse=True)[:limit]:
        try:
            details = inspect_run(root, run_id, event_limit=1)
        except FileNotFoundError:
            continue  # A cleanup may have removed this run after the directory enumeration.
        identity = details["identity"] or {}
        ownership = details["ownership"] or {}
        last = ownership.get("last_operation") or {}
        results.append(
            {
                "run_id": run_id,
                "repo": identity.get("repo"),
                "issue_id": identity.get("issue_id"),
                "blueprint": identity.get("blueprint"),
                "held": ownership.get("held"),
                "interrupted": ownership.get("interrupted"),
                "last_operation": last.get("operation"),
                "last_event": last.get("event"),
                "recorded_cost_usd": details["recorded_cost_usd"],
                "torn_tail_bytes": sum(j["torn_tail_bytes"] for j in details["journals"].values()),
                "errors": details["errors"],
            }
        )
    return results
