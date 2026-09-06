#!/usr/bin/env python3
"""Compare two control-room snapshots of the SAME live factory: Python's and Rust's.

``swfactory herd --once --json`` and ``swf snapshot --json`` are two clients rendering one server.
They will never be byte-identical — they are collected milliseconds apart, so a task can move
between the two reads — but they must agree about what the factory *is*: which runs exist, which
jobs each fanned out into, which issue each job carries, which gates are outstanding and against
which job, and which deliveries were published.

So this compares a NORMALISED projection. Dropped, with the reason:

* ``collected_at`` and every other timestamp — the two reads are not simultaneous;
* task-level ``state`` of a job still in flight — genuinely volatile between two reads;
* ``metrics`` — a filesystem scan, not an Airflow read, and each client may root it differently;
* ``errors`` — one client may see a transient failure the other does not.

What survives is identity and shape, which is exactly the claim under test: an operator who
switches from the Python control room to the Rust one sees the same factory.

    scripts/snapshot_diff.py <python.json> <rust.json>

Exit 0 when they agree, 1 with a readable diff when they do not.
"""

from __future__ import annotations

import json
import sys
from typing import Any

# A job's roll-up is stable only once the job has finished; while it is moving, the two clients
# legitimately disagree by one task. These are the states we hold the clients to.
SETTLED = {"success", "failed", "skipped"}


def norm(snapshot: dict[str, Any]) -> dict[str, Any]:
    """The part of a snapshot that both clients must agree on, in a canonical order."""
    runs = []
    for run in snapshot.get("runs") or []:
        jobs = []
        for job in run.get("jobs") or []:
            state = str(job.get("state") or "")
            jobs.append(
                {
                    "map_index": int(job.get("map_index", -1)),
                    "issue": str(job.get("issue") or "-"),
                    # A job in flight is allowed to differ; a settled one is not.
                    "state": state if state in SETTLED else "in_flight",
                }
            )
        runs.append(
            {
                "dag_id": str(run.get("dag_id") or ""),
                "run_id": str(run.get("run_id") or ""),
                "issues": sorted(str(i) for i in run.get("issues") or []),
                "jobs": sorted(jobs, key=lambda j: j["map_index"]),
            }
        )
    gates = [
        {
            "dag_id": str(g.get("dag_id") or ""),
            "run_id": str(g.get("run_id") or ""),
            "task_id": str(g.get("task_id") or ""),
            "map_index": int(g.get("map_index", -1)),
            "subject": str(g.get("subject") or ""),
            "options": sorted(str(o) for o in g.get("options") or []),
        }
        for g in snapshot.get("gates") or []
    ]
    prs = [
        {
            "number": int(p.get("number", 0)),
            "state": str(p.get("state") or ""),
            "head": str(p.get("head") or ""),
            "labels": sorted(str(x) for x in p.get("labels") or []),
        }
        for p in snapshot.get("prs") or []
    ]
    return {
        "runs": sorted(runs, key=lambda r: (r["dag_id"], r["run_id"])),
        "gates": sorted(
            gates, key=lambda g: (g["dag_id"], g["run_id"], g["task_id"], g["map_index"])
        ),
        "prs": sorted(prs, key=lambda p: p["number"]),
    }


def walk(path: str, a: Any, b: Any, out: list[str]) -> None:
    """Depth-first structural diff; ``out`` collects one line per disagreement."""
    if type(a) is not type(b):
        out.append(f"{path}: python {type(a).__name__} vs rust {type(b).__name__}")
    elif isinstance(a, dict):
        for key in sorted(set(a) | set(b)):
            if key not in a:
                out.append(f"{path}.{key}: missing in python, rust has {b[key]!r}")
            elif key not in b:
                out.append(f"{path}.{key}: missing in rust, python has {a[key]!r}")
            else:
                walk(f"{path}.{key}", a[key], b[key], out)
    elif isinstance(a, list):
        if len(a) != len(b):
            out.append(f"{path}: python has {len(a)} entries, rust has {len(b)}")
        for i, (x, y) in enumerate(zip(a, b, strict=False)):
            walk(f"{path}[{i}]", x, y, out)
    elif a != b:
        out.append(f"{path}: python {a!r} vs rust {b!r}")


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as fh:
        python = norm(json.load(fh))
    with open(argv[2], encoding="utf-8") as fh:
        rust = norm(json.load(fh))
    problems: list[str] = []
    walk("snapshot", python, rust, problems)
    counts = (
        f"{len(python['runs'])} runs, "
        f"{sum(len(r['jobs']) for r in python['runs'])} jobs, "
        f"{len(python['gates'])} gates, {len(python['prs'])} prs"
    )
    if problems:
        print(f"the two control rooms disagree ({counts}):", file=sys.stderr)
        for line in problems[:40]:
            print(f"  {line}", file=sys.stderr)
        if len(problems) > 40:
            print(f"  ... and {len(problems) - 40} more", file=sys.stderr)
        return 1
    print(f"snapshots agree: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
