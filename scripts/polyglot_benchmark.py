#!/usr/bin/env python3
"""Reproducible native Cargo+uv affectedness and local-cache benchmark.

This script is intentionally advisory evidence. It measures Turborepo as an execution accelerator
and never calls candidate readiness, approvals, publication, or promotion code.
"""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TURBO = ("npx", "--yes", "turbo@2.11.1")
FACTORY_LEAVES = frozenset(
    {
        "//#python-verify",
        "//#rust-contract-verify",
        "swfactory-rust#test",
        "swf-cli#build",
    }
)
TASK_QUERY = """
query {
  affectedTasks(
    base: "HEAD^"
    head: "HEAD"
    tasks: ["//#python-verify", "//#rust-contract-verify", "swfactory-rust#test", "swf-cli#build"]
  ) {
    items { fullName }
  }
}
""".strip()


def _run(
    argv: tuple[str, ...] | list[str],
    *,
    cwd: Path = ROOT,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a command and optionally raise with bounded diagnostic output."""
    result = subprocess.run(
        list(argv),
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(argv)}\n"
            f"stdout:\n{result.stdout[-4000:]}\nstderr:\n{result.stderr[-4000:]}"
        )
    return result


def _affected_names(worktree: Path) -> tuple[str, ...]:
    """Return the sorted Turbo task names affected in a benchmark worktree."""
    result = _run((*TURBO, "query", TASK_QUERY), cwd=worktree)
    document = json.loads(result.stdout)
    node: Any = document.get("data", document)
    affected = node.get("affectedTasks") if isinstance(node, dict) else None
    if not isinstance(affected, dict):
        raise RuntimeError(f"turbo query returned no affectedTasks object: {document!r}")
    items = affected.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"turbo query returned invalid affectedTasks.items: {document!r}")
    names = []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("fullName"), str):
            raise RuntimeError(f"turbo query returned invalid task item: {item!r}")
        full_name = item["fullName"]
        if full_name in FACTORY_LEAVES:
            names.append(full_name)
    return tuple(sorted(names))


def _modify(path: Path, scenario: str) -> None:
    """Apply the synthetic source change for an affectedness scenario."""
    if scenario == "contract":
        candidates = sorted(path.joinpath("tests/fixtures/contract").glob("*.json"))
        if not candidates:
            raise RuntimeError("shared contract fixture corpus contains no JSON fixture")
        target = candidates[0]
        target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        return

    mapping = {
        "python": (path / "src/swfactory/cli.py", "\n# polyglot affectedness benchmark\n"),
        "rust": (path / "rust/crates/swf-domain/src/lib.rs", "\n// polyglot affectedness benchmark\n"),
        "docs": (path / "docs/polyglot-task-graph.md", "\npolyglot affectedness benchmark\n"),
    }
    target, suffix = mapping[scenario]
    target.write_text(target.read_text(encoding="utf-8") + suffix, encoding="utf-8")


def affectedness_matrix() -> dict[str, list[str]]:
    """Measure affected Turbo tasks for each representative change type."""
    baseline = _run(("git", "rev-parse", "HEAD")).stdout.strip()
    with tempfile.TemporaryDirectory(prefix="swf-polyglot-affected-") as tmp:
        worktree = Path(tmp) / "repo"
        _run(("git", "worktree", "add", "--detach", str(worktree), baseline))
        try:
            _run(("git", "config", "user.name", "Polyglot Benchmark"), cwd=worktree)
            _run(("git", "config", "user.email", "benchmark@example.invalid"), cwd=worktree)
            matrix: dict[str, list[str]] = {}
            for scenario in ("python", "rust", "docs", "contract"):
                _run(("git", "reset", "--hard", baseline), cwd=worktree)
                _run(("git", "clean", "-fd"), cwd=worktree)
                _modify(worktree, scenario)
                _run(("git", "add", "-A"), cwd=worktree)
                _run(("git", "commit", "-qm", f"benchmark: {scenario} change"), cwd=worktree)
                matrix[scenario] = list(_affected_names(worktree))
        finally:
            _run(("git", "worktree", "remove", "--force", str(worktree)), check=False)
    return matrix


def _is_rust_verifier(name: str) -> bool:
    return name == "//#rust-contract-verify" or name.startswith("swf-") or name.startswith("swfactory-rust#")


def _assert_affectedness(matrix: dict[str, list[str]]) -> None:
    """Validate that affected tasks preserve the polyglot graph boundaries."""
    docs = set(matrix["docs"])
    if "//#python-verify" not in docs:
        raise RuntimeError(f"docs-only change missed Python/site verification: {sorted(docs)}")
    if any(_is_rust_verifier(name) for name in docs):
        raise RuntimeError(f"docs-only change selected Rust work: {sorted(docs)}")

    python = set(matrix["python"])
    if "//#python-verify" not in python:
        raise RuntimeError(f"python-only change missed factory Python verification: {sorted(python)}")
    if any(_is_rust_verifier(name) for name in python):
        raise RuntimeError(f"python-only change selected Rust work: {sorted(python)}")

    rust = set(matrix["rust"])
    if "//#python-verify" in rust:
        raise RuntimeError(f"rust-only change selected factory Python verification: {sorted(rust)}")
    if not any(_is_rust_verifier(name) for name in rust):
        raise RuntimeError(f"rust-only change selected no Rust task: {sorted(rust)}")

    contract = set(matrix["contract"])
    if "//#python-verify" not in contract:
        raise RuntimeError(f"shared-contract change missed factory Python verification: {sorted(contract)}")
    if not any(_is_rust_verifier(name) for name in contract):
        raise RuntimeError(f"shared-contract change missed Rust verification: {sorted(contract)}")


def _usage() -> tuple[float, float]:
    """Return cumulative child-process user and system CPU time."""
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime, usage.ru_stime


def _timed(argv: tuple[str, ...]) -> dict[str, object]:
    """Run a command and capture its exit status, timing, and output tails."""
    before_user, before_sys = _usage()
    started = time.perf_counter()
    result = _run(argv, check=False)
    wall_ms = round((time.perf_counter() - started) * 1000)
    after_user, after_sys = _usage()
    return {
        "argv": list(argv),
        "exit_code": result.returncode,
        "wall_ms": wall_ms,
        "cpu_user_s": round(after_user - before_user, 6),
        "cpu_system_s": round(after_sys - before_sys, 6),
        "stdout_tail": result.stdout[-1200:],
        "stderr_tail": result.stderr[-1200:],
    }


def _latest_summary() -> Path:
    """Return the newest Turbo run-summary file."""
    summaries = sorted(
        (ROOT / ".turbo" / "runs").glob("*.json"),
        key=lambda path: path.stat().st_mtime_ns,
    )
    if not summaries:
        raise RuntimeError("turbo --summarize produced no .turbo/runs JSON")
    return summaries[-1]


def _execution(summary: Path) -> dict[str, object]:
    """Extract normalized execution counters from a Turbo run summary."""
    document = json.loads(summary.read_text(encoding="utf-8"))
    execution = document.get("execution")
    if not isinstance(execution, dict):
        raise RuntimeError(f"run summary has no execution object: {summary}")
    attempted = int(execution.get("attempted", 0))
    cached = int(execution.get("cached", 0))
    return {
        "summary": str(summary.relative_to(ROOT)),
        "summary_bytes": summary.stat().st_size,
        "attempted_tasks": attempted,
        "cache_hits": cached,
        "executed_tasks": max(attempted - cached, 0),
        "successful_tasks": int(execution.get("success", 0)),
        "failed_tasks": int(execution.get("failed", 0)),
        "exit_code": int(execution.get("exitCode", -1)),
    }


def verification_benchmark() -> dict[str, object]:
    """Compare equivalent clean verification with a no-op warm Turbo repeat."""
    baseline_commands = (
        ("uv", "run", "--active", "--frozen", "--all-packages", "pytest"),
        ("cargo", "test", "--workspace", "--locked"),
        ("cargo", "build", "-p", "swf-cli", "--locked"),
    )

    # Keep tool download/registry caches warm on purpose, but make compiler outputs identical:
    # both the no-Turbo baseline and Turbo-cold begin without a repository target directory.
    shutil.rmtree(ROOT / "target", ignore_errors=True)
    baseline_runs = [_timed(command) for command in baseline_commands]
    baseline_ok = all(run["exit_code"] == 0 for run in baseline_runs)

    shutil.rmtree(ROOT / "target", ignore_errors=True)
    shutil.rmtree(ROOT / ".turbo", ignore_errors=True)
    cold_run = _timed((*TURBO, "run", "//#polyglot-verification", "--summarize", "--log-order=grouped"))
    cold_summary = _execution(_latest_summary())

    warm_run = _timed((*TURBO, "run", "//#polyglot-verification", "--summarize", "--log-order=grouped"))
    warm_summary = _execution(_latest_summary())

    turbo_ok = cold_run["exit_code"] == 0 and warm_run["exit_code"] == 0
    equivalent = baseline_ok == turbo_ok
    if not equivalent:
        raise RuntimeError(f"Turbo changed the verification verdict: baseline={baseline_ok} turbo={turbo_ok}")
    if turbo_ok and int(warm_summary["cache_hits"]) <= int(cold_summary["cache_hits"]):
        raise RuntimeError(
            "warm verification produced no additional local Turbo cache hits; "
            f"cold={cold_summary['cache_hits']} warm={warm_summary['cache_hits']}"
        )

    return {
        "baseline": {
            "verdict": "pass" if baseline_ok else "fail",
            "wall_ms": sum(int(run["wall_ms"]) for run in baseline_runs),
            "cpu_user_s": round(sum(float(run["cpu_user_s"]) for run in baseline_runs), 6),
            "cpu_system_s": round(sum(float(run["cpu_system_s"]) for run in baseline_runs), 6),
            "commands": baseline_runs,
        },
        "turbo_cold": {
            "verdict": "pass" if cold_run["exit_code"] == 0 else "fail",
            "timing": cold_run,
            **cold_summary,
        },
        "turbo_warm": {
            "verdict": "pass" if warm_run["exit_code"] == 0 else "fail",
            "timing": warm_run,
            **warm_summary,
        },
        "verification_verdict_equivalent": equivalent,
        "cold_start_contract": {
            "baseline_target_removed": True,
            "turbo_target_removed": True,
            "turbo_local_cache_removed": True,
            "tool_download_caches_preserved": True,
        },
        "remote_cache_enabled": False,
        "run_summary_count": len(list((ROOT / ".turbo" / "runs").glob("*.json"))),
    }


def main() -> int:
    """Generate the affectedness and verification benchmark report."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path(".factory/turbo/benchmark.json"))
    args = parser.parse_args()

    matrix = affectedness_matrix()
    _assert_affectedness(matrix)
    benchmark = verification_benchmark()
    report = {
        "schema_version": 1,
        "turbo_version": "2.11.1",
        "authority": "advisory-execution-only",
        "affectedness": {
            "selection_scope": "factory-fan-in-leaves",
            "matrix": matrix,
        },
        "benchmark": benchmark,
    }
    output = args.out if args.out.is_absolute() else ROOT / args.out
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
