#!/usr/bin/env python3
"""No-charge external harness fixture: a real stdlib subprocess, not an Agent test double.

Reuse the demo's deliberately broken build and source-only repair. Edits remain uncommitted;
the factory owns verification, commits, gates, and delivery.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[3] / "demo" / "scripted"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", type=Path, required=True)
    args = parser.parse_args()
    request = json.loads(args.request.read_text(encoding="utf-8"))
    stage, iteration = request["stage"], request["iteration"]
    assert request["schema_version"] == 1 and request["model"] == "fixture"
    assert request["call_id"].startswith(f"{stage}.{iteration}:")
    assert request["accepted_inputs_digest"].startswith("inputs:")
    assert request["prompt"] and request["budget"]["usd"] > 0
    assert request["budget"]["max_turns"] > 0 and request["budget"]["timeout_s"] > 0
    assert request["policy"]["writes"] is (stage in ("build", "fix"))
    assert (request["output_schema"] is None) is (stage == "spec")
    if not request["policy"]["writes"]:
        assert request["policy"]["writable_paths"] == []
    if stage == "fix":
        assert "tests" in request["policy"]["protected_paths"]
        assert "failed=2" in request["prompt"]
    result = {
        "schema_version": 1,
        "call_id": request["call_id"],
        "profile_id": request["profile_id"],
        "status": "success",
        "num_turns": 1,
        "session_id": "external-fixture",
    }
    if stage == "spec":
        result["text"] = (FIXTURES / "spec.md").read_text(encoding="utf-8")
    elif stage in ("plan", "review"):
        result["data"] = json.loads((FIXTURES / f"{stage}.json").read_text(encoding="utf-8"))
    elif (stage, iteration) in (("build", 1), ("fix", 2)):
        subprocess.run(
            ["git", "apply", "--whitespace=nowarn", str(FIXTURES / f"{stage}.{iteration}.patch")],
            check=True,
            capture_output=True,
            text=True,
        )
        result["data"] = {
            "summary": "Applied demo fixture patch; verification and commits belong to the factory.",
            "files_changed": ["src/calc/core.py"]
            if stage == "fix"
            else ["src/calc/core.py", "src/calc/__init__.py", "tests/test_percent_change.py"],
        }
    else:
        raise ValueError(f"unexpected fixture call: {stage}.{iteration}")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
