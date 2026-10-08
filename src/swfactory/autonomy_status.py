"""Read-only operator views of autonomous decisions; no approvals or retry authority."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swfactory.autonomy import load_policy
from swfactory.backend_http import ResponseTooLarge, no_redirect_open, post_json
from swfactory.cells import is_cell_id
from swfactory.webhook import _safe_backend_base

NEXT_ACTION = {
    "required_labels_missing": "Apply the policy's required labels, then retriage the issue.",
    "denied_label": "Resolve the blocking condition and remove the denied label, then retriage.",
    "issue_not_open": "Reopen the issue before retriage.",
    "missing_intent": "Add a nonempty issue title and body, then retriage.",
    "repository_outside_policy": "Use the repository authorized by the checked-in policy.",
    "policy_disabled": "A maintainer must enable the policy before admission.",
}


def status(root: Path, *, limit: int = 50) -> dict:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    policy = load_policy()
    result = {"policy_revision": policy.revision, "enabled": policy.enabled, "decisions": []}
    database = root / "autonomy.sqlite3"
    if not database.exists():
        return result
    # URI read-only mode ensures status never creates or migrates a backend database.
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        has_times = db.execute("SELECT 1 FROM sqlite_master WHERE name='decision_times'").fetchone()
        stamp = "t.created_at" if has_times else "NULL"
        join = "LEFT JOIN decision_times t ON t.key=d.key" if has_times else ""
        rows = db.execute(
            f"SELECT d.key,d.value,{stamp} FROM decisions d {join} ORDER BY d.rowid DESC LIMIT ?", (limit,)
        ).fetchall()
    for key, raw, recorded_at in rows:
        value = json.loads(raw)
        parts = key.split(":")
        reason = value.get("reason")
        approval = value.get("approval") or {}
        revision = value.get("revision")
        if not revision and str(approval.get("actor", "")).startswith("policy:"):
            revision = approval["actor"][len("policy:") :]
        kind = "triage" if key.startswith("triage:") else "gate" if approval else "publication"
        if key.startswith("blocked-gate:"):
            kind = "gate"
        state = value.get("state") or ("approved" if approval else "published")
        result["decisions"].append(
            {
                "kind": kind,
                "issue": parts[2] if kind == "triage" else None,
                "cell_id": approval.get("cell_id") or (parts[0] if is_cell_id(parts[0]) else None),
                "state": state,
                "reason": reason,
                "recorded_at": recorded_at,
                "policy_revision": revision,
                "next_action": NEXT_ACTION.get(
                    reason, "Inspect the rejected inputs; use a new admission after correcting them."
                )
                if state == "blocked"
                else "Await admission; eligibility alone is not proof of execution."
                if state == "eligible"
                else "Inspect the Cell and Airflow run for progress.",
            }
        )
    return result


def remote_status(url: str, token: str, *, limit: int = 50) -> dict:
    base = _safe_backend_base(url)
    if not token:
        raise ValueError("SWF_BACKEND_TOKEN is missing")
    try:
        code, result = post_json(
            base,
            token,
            "/v1/scm/autonomy-status",
            {"limit": limit},
            timeout=30,
            limit=1024 * 1024,
            opener=no_redirect_open,
        )
    except ResponseTooLarge:
        raise ValueError("autonomy status exceeds response limit") from None
    if code >= 300:
        raise ValueError(f"HTTP {code}")
    if not isinstance(result, dict) or not isinstance(result.get("decisions"), list):
        raise ValueError("backend returned an invalid autonomy status")
    return result
