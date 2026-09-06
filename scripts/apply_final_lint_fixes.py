#!/usr/bin/env python3
"""Apply the final deterministic Ruff repairs discovered by stabilization CI."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    if old in text:
        target.write_text(text.replace(old, new, 1), encoding="utf-8")
        return
    if new not in text:
        raise RuntimeError(f"pattern not found in {path}: {old[:100]!r}")


def main() -> None:
    replace(
        "src/swfactory/backend/service.py",
        '                        "detail": f"backend tool installed={present}, configured={configured}; credentials not probed",',
        '                        "detail": (\n                            f"backend tool installed={present}, configured={configured}; "\n                            "credentials not probed"\n                        ),',
    )
    replace(
        "src/swfactory/backend_scm.py",
        "The worker can read local issue files itself, but numeric GitHub issue reads and all GitHub writes go\nthrough the authenticated Python backend. No GH_TOKEN/GITHUB_TOKEN is needed in the worker process.",
        "The worker can read local issue files itself, but numeric GitHub issue reads and all GitHub\nwrites go through the authenticated Python backend. No GH_TOKEN/GITHUB_TOKEN is needed in the\nworker process.",
    )
    replace(
        "src/swfactory/backend_store.py",
        '                        "INSERT INTO backend_state(key,version,value_json,updated_at) VALUES(?,1,?,?)",',
        '                        "INSERT INTO backend_state(key,version,value_json,updated_at) "\n                        "VALUES(?,1,?,?)",',
    )
    replace(
        "src/swfactory/backend_store.py",
        '                "UPDATE backend_state SET version=?,value_json=?,updated_at=? WHERE key=? AND version=?",',
        '                "UPDATE backend_state SET version=?,value_json=?,updated_at=? "\n                "WHERE key=? AND version=?",',
    )
    replace(
        "src/swfactory/backend_store.py",
        '                "ON CONFLICT(key) DO UPDATE SET owner=excluded.owner,epoch=excluded.epoch,expires_at=excluded.expires_at",',
        '                "ON CONFLICT(key) DO UPDATE SET owner=excluded.owner,epoch=excluded.epoch,"\n                "expires_at=excluded.expires_at",',
    )
    replace(
        "src/swfactory/cleanup_receipt.py",
        "Cleanup is a convergent external mutation, not a best-effort `close()` side effect.  Providers report\nwhat they observed; callers decide whether that observation is authoritative for the current cell",
        "Cleanup is a convergent external mutation, not a best-effort `close()` side effect. Providers\nreport what they observed; callers decide whether that observation is authoritative for the current\ncell",
    )
    replace(
        "src/swfactory/cleanup_receipt.py",
        '                "UPDATE repair_leases SET expires_at=? WHERE lease_key=? AND owner=? AND lease_epoch=?",',
        '                "UPDATE repair_leases SET expires_at=? "\n                "WHERE lease_key=? AND owner=? AND lease_epoch=?",',
    )
    replace(
        "src/swfactory/control_kernel.py",
        '        """Return the canonical receipt document; persistence is the operation/evidence layer\'s job."""',
        '        """Return the canonical receipt document.\n\n        Persistence is the operation/evidence layer\'s job.\n        """',
    )
    replace(
        "src/swfactory/durable_admission.py",
        '                """UPDATE admission_work SET state=?,reason=\'cell_terminal\',terminal_at=?,updated_at=?\n                   WHERE work_id=? AND state=\'active\' AND cell_id=? AND cell_epoch=?""",',
        '                "UPDATE admission_work SET state=?,reason=\'cell_terminal\',"\n                "terminal_at=?,updated_at=? "\n                "WHERE work_id=? AND state=\'active\' AND cell_id=? AND cell_epoch=?",',
    )
    replace(
        "src/swfactory/durable_admission.py",
        "                # Another class may still fit, so temporarily mark this sequence skipped for this pass.",
        "                # Another class may still fit, so temporarily mark this sequence\n                # skipped for this pass.",
    )
    replace(
        "src/swfactory/durable_admission.py",
        '                "UPDATE admission_fairness SET deficit=MAX(deficit-1,0),served=served+1 WHERE priority=?",',
        '                "UPDATE admission_fairness SET deficit=MAX(deficit-1,0),served=served+1 "\n                "WHERE priority=?",',
    )
    replace(
        "src/swfactory/durable_admission.py",
        "                \"UPDATE admission_work SET sequence=?,updated_at=? WHERE work_id=? AND state='queued'\",",
        '                "UPDATE admission_work SET sequence=?,updated_at=? "\n                "WHERE work_id=? AND state=\'queued\'",',
    )
    replace(
        "src/swfactory/lifecycle_evidence.py",
        "This is the single write facade lifecycle code should converge on.  High-cardinality identifiers live\nin append-only evidence, not aggregate metric labels.",
        "This is the single write facade lifecycle code should converge on. High-cardinality identifiers\nlive in append-only evidence, not aggregate metric labels.",
    )
    replace(
        "src/swfactory/lifecycle_evidence.py",
        """        if key in {\n            "repo",\n            "blueprint",\n            "generation",\n            "provider",\n            "stage",\n            "state",\n            "kind",\n            "result",\n        }:\n            if "cell_" in lower or "run_" in lower or len(value) > 128:\n                raise ValueError(f"metric label {key} appears high-cardinality")""",
        """        if key in {\n            "repo",\n            "blueprint",\n            "generation",\n            "provider",\n            "stage",\n            "state",\n            "kind",\n            "result",\n        } and ("cell_" in lower or "run_" in lower or len(value) > 128):\n            raise ValueError(f"metric label {key} appears high-cardinality")""",
    )
    replace(
        "src/swfactory/trust_evidence.py",
        '            "mutation policy digest is stale; refuse external side effect until the cell is reactivated"',
        '            "mutation policy digest is stale; refuse external side effect until the cell "\n            "is reactivated"',
    )


if __name__ == "__main__":
    main()
