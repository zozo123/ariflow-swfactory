#!/usr/bin/env python3
"""Launch real concurrent harness sessions through `swf` and assert backend authority semantics.

This helper deliberately does not mock the backend or Airflow. `scripts/swf_e2e.sh --backend` starts
both services, configures a real `swf` context, then calls this program. The helper launches three
independent harness sessions plus two contenders for the same issue/target Cells at one barrier.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Session:
    name: str
    harness: str
    factory_id: str
    issue: str


def command(swf: str, blueprint: str, session: Session) -> list[str]:
    return [
        swf,
        "submit",
        "--blueprint",
        blueprint,
        "--issue",
        session.issue,
        "--harness",
        session.harness,
        "--factory-id",
        session.factory_id,
        "--json",
    ]


def run_one(swf: str, blueprint: str, session: Session, barrier: threading.Barrier | None = None) -> dict[str, Any]:
    if barrier is not None:
        barrier.wait(timeout=15)
    proc = subprocess.run(
        command(swf, blueprint, session),
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        timeout=90,
    )
    payload: dict[str, Any] | None = None
    if proc.stdout.strip():
        try:
            parsed = json.loads(proc.stdout)
            if isinstance(parsed, dict):
                payload = parsed
        except json.JSONDecodeError:
            pass
    return {
        "session": session,
        "returncode": proc.returncode,
        "payload": payload,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def fail(message: str, rows: list[dict[str, Any]] | None = None) -> None:
    print(f"multi_harness_submit: {message}", file=sys.stderr)
    for row in rows or []:
        session: Session = row["session"]
        print(
            f"  {session.name}: rc={row['returncode']} stdout={row['stdout']!r} stderr={row['stderr']!r}",
            file=sys.stderr,
        )
    raise SystemExit(1)


def airflow_run(backend_url: str, token: str, dag_id: str, run_id: str) -> dict[str, Any]:
    path = "/v1/airflow/api/v2/dags/{}/dagRuns/{}".format(
        urllib.parse.quote(dag_id, safe=""),
        urllib.parse.quote(run_id, safe=""),
    )
    request = urllib.request.Request(
        backend_url.rstrip("/") + path,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"Airflow inspection through backend failed: HTTP {error.code}: {detail}") from error
    if not isinstance(payload, dict):
        raise RuntimeError("Airflow inspection through backend returned a non-object")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--swf", required=True)
    parser.add_argument("--blueprint", required=True)
    parser.add_argument("--backend-url", required=True)
    parser.add_argument("--token-env", default="SWF_BACKEND_TOKEN")
    args = parser.parse_args()

    token = os.getenv(args.token_env, "")
    if len(token) < 32:
        fail(f"{args.token_env} is missing or too short")

    primary = [
        Session("codex", "codex", "live-codex", "e2e-codex"),
        Session("claude", "claude-code", "live-claude", "e2e-claude"),
        Session("grok", "grok", "live-grok", "e2e-grok"),
    ]
    race = [
        Session("race-a", "custom", "live-race-a", "e2e-race"),
        Session("race-b", "custom", "live-race-b", "e2e-race"),
    ]
    sessions = primary + race
    barrier = threading.Barrier(len(sessions))
    with ThreadPoolExecutor(max_workers=len(sessions)) as pool:
        rows = list(pool.map(lambda session: run_one(args.swf, args.blueprint, session, barrier), sessions))

    primary_rows = rows[: len(primary)]
    if any(row["returncode"] != 0 or row["payload"] is None for row in primary_rows):
        fail("one of the three independent harness sessions was not admitted", rows)

    race_rows = rows[len(primary) :]
    race_winners = [row for row in race_rows if row["returncode"] == 0 and row["payload"] is not None]
    race_losers = [row for row in race_rows if row["returncode"] != 0]
    if len(race_winners) != 1 or len(race_losers) != 1:
        fail("duplicate Cell race must produce exactly one admitted writer and one fenced/refused contender", rows)

    winners = primary_rows + race_winners
    run_ids = [str(row["payload"].get("run_id", "")) for row in winners]
    if any(not run_id for run_id in run_ids):
        fail("an admitted session returned no run_id", rows)
    if len(set(run_ids)) != len(run_ids):
        fail(f"independent sessions did not receive distinct run identities: {run_ids}", rows)

    # Replay the exact Codex request while its Cell is active. The durable admission key and
    # mutation journal must recover the same deterministic run instead of creating another writer.
    replay = run_one(args.swf, args.blueprint, primary[0])
    if replay["returncode"] != 0 or replay["payload"] is None:
        fail("exact Codex replay was not accepted idempotently", [replay])
    if replay["payload"].get("run_id") != primary_rows[0]["payload"].get("run_id"):
        fail("exact Codex replay created a different Airflow run", [primary_rows[0], replay])

    # The backend writes the stable outer harness/factory actor into Airflow conf. Inspect that
    # through the authenticated backend compatibility boundary, not through backend-local state.
    inspected: list[dict[str, str]] = []
    for row in winners:
        session: Session = row["session"]
        run_id = str(row["payload"]["run_id"])
        document = airflow_run(args.backend_url, token, args.blueprint, run_id)
        conf = document.get("conf") if isinstance(document.get("conf"), dict) else {}
        expected_actor = f"harness:{session.harness}:{session.factory_id}".lower()
        actor = str(conf.get("_factory_actor", ""))
        if actor != expected_actor:
            fail(
                f"Airflow conf lost harness identity for {session.name}: expected {expected_actor!r}, got {actor!r}",
                [row],
            )
        if not conf.get("_factory_submission_id") or not conf.get("_factory_cells"):
            fail(f"Airflow conf is missing durable submission/Cell bindings for {session.name}", [row])
        inspected.append({"name": session.name, "actor": actor, "run_id": run_id, "issue": session.issue})

    loser: Session = race_losers[0]["session"]
    loser_text = (race_losers[0]["stderr"] + "\n" + race_losers[0]["stdout"]).lower()
    if not any(word in loser_text for word in ("busy", "conflict", "409", "active", "fenc", "refus")):
        fail("duplicate-race loser failed for an unrelated reason", race_losers)

    report = {
        "runs": inspected,
        "race": {
            "winner": race_winners[0]["session"].name,
            "loser": loser.name,
            "loser_returncode": race_losers[0]["returncode"],
        },
        "replay": {
            "session": primary[0].name,
            "run_id": replay["payload"]["run_id"],
            "same_run": True,
        },
    }
    json.dump(report, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
