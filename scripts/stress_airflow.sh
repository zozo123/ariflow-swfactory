#!/usr/bin/env bash
# Fan-out on a LIVE Airflow: boot `airflow standalone` in a throwaway AIRFLOW_HOME on a free port,
# trigger blueprints/stress.toml over >= 2 issues, answer every job's two gates through the HITL
# API, and print what each mapped job produced on disk.
#
# WHY a script and not just tests/test_dag_stress.py: `dag.test()` / `airflow dags test` never
# resolves a HITL task — it can only mark the gate success — so the one thing it cannot prove is
# that the gates are real, addressable approvals answered by a named user. `approvals.json`
# records actor "auto" there and `admin` here. This is also the repeatable version of the manual
# recipe in docs/design.md, so "the factory is Airflow-driven" is a command, not a claim.
#
#   scripts/stress_airflow.sh [issue ...]        # default: demo/issue.md demo/issue2.md
#
# Env: SWF_APPROVE=auto  do NOT answer the gates — let `gates[].auto` default them to Approve,
#                        which the ApprovalOperator only does after timeout_h (1 h). Slow on
#                        purpose: that is the unattended backstop, not the fast path.
#      SWF_STRESS_KEEP=1 keep the work dir (standalone home, run dirs, logs) after exit.
#
# No keys and no network: scripted agent, local sandbox, local git remote. Exit code is non-zero
# if any task instance failed, any job's artifacts are missing, or the run did not end green.
set -Eeuo pipefail

REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DAG_ID="stress"
TARGET_B="demo/target-b" # blueprints/stress.toml's second [[targets]].dir (materialised below)
HEALTH_TIMEOUT_S=240
PARSE_TIMEOUT_S=240
RUN_TIMEOUT_S=1800

if [ $# -eq 0 ]; then set -- demo/issue.md demo/issue2.md; fi
if [ $# -lt 2 ]; then
  echo "stress: pass at least 2 issues (fan-out is the point); got $#" >&2
  exit 2
fi
ISSUES=("$@")

WORK="$(mktemp -d "${TMPDIR:-/tmp}/swf-stress.XXXXXX")"
export AIRFLOW_HOME="$WORK/airflow_home"
STANDALONE_LOG="$WORK/standalone.log"
STANDALONE_PID=""

say() { printf '\n=== %s\n' "$*"; }

cleanup() {
  rc=$?
  if [ -n "$STANDALONE_PID" ]; then
    say "shutting down standalone (process group $STANDALONE_PID)"
    # SIGINT: `airflow standalone` stops its scheduler / api-server / dag-processor / triggerer
    # children on KeyboardInterrupt. To the whole group, since they are its subprocesses.
    kill -INT -- "-$STANDALONE_PID" 2>/dev/null || true
    i=0
    while kill -0 "$STANDALONE_PID" 2>/dev/null && [ $i -lt 60 ]; do
      sleep 0.5
      i=$((i + 1))
    done
    kill -KILL -- "-$STANDALONE_PID" 2>/dev/null || true
    wait "$STANDALONE_PID" 2>/dev/null || true
  fi
  if [ "$rc" -ne 0 ] && [ -f "$STANDALONE_LOG" ]; then
    say "standalone log (tail)"
    tail -40 "$STANDALONE_LOG" || true
  fi
  if [ "${SWF_STRESS_KEEP:-}" = "1" ]; then
    echo "work dir kept: $WORK"
  else
    rm -rf "$WORK"
  fi
  exit "$rc"
}
trap cleanup EXIT

# ---------------------------------------------------------------- the project's interpreter
#
# One `uv run` to materialise/locate the venv, then the venv's own binaries: nothing else in this
# script holds uv's lock, so a long `airflow standalone` cannot block another `uv run` (or be
# blocked by one).
PY="$(uv run --project "$REPO" --group airflow python -c 'import sys; print(sys.executable)')"
BIN="$(dirname "$PY")"
[ -x "$BIN/airflow" ] || {
  echo "no airflow in $BIN — run: uv sync --group airflow" >&2
  exit 1
}
# `airflow standalone` starts its scheduler / api-server / dag-processor / triggerer by running
# `airflow <subcommand>` off PATH, so the venv has to be on it and not just addressed by path.
export PATH="$BIN:$PATH"

# ---------------------------------------------------------------- REST helpers

api() { # api METHOD PATH [BODY]
  _method=$1
  _path=$2
  _body=${3:-}
  set -- -fsS -X "$_method" -H "Accept: application/json"
  if [ -n "${TOKEN:-}" ]; then set -- "$@" -H "Authorization: Bearer $TOKEN"; fi
  if [ -n "$_body" ]; then set -- "$@" -H "Content-Type: application/json" -d "$_body"; fi
  curl "$@" "$BASE/api/v2$_path"
}

# One field of the JSON object on stdin (stdlib only: jq is not assumed to be installed).
field() { "$PY" -c "import json,sys;print(json.load(sys.stdin).get('$1',''))"; }

# ---------------------------------------------------------------- work dir

say "work dir $WORK"
mkdir -p "$AIRFLOW_HOME" "$WORK/$(dirname "$TARGET_B")"
# The blueprint's second target: a copy of demo/target, materialised here rather than committed
# (the recorded patches carry blob hashes, so a "second" target has to BE that copy). A job
# resolves its target dir against the worker's cwd before the checkout, and standalone — hence
# every task — runs with cwd=$WORK.
cp -R "$REPO/demo/target" "$WORK/$TARGET_B"
find "$WORK/$TARGET_B" \( -name __pycache__ -o -name .pytest_cache -o -name .venv \) -prune \
  -exec rm -rf {} + 2>/dev/null || true
cd "$WORK"

PORT="$("$PY" -c '
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
')"
BASE="http://localhost:$PORT"
TARGETS="$("$PY" -c "from swfactory.blueprint import load; print(len(load('$DAG_ID').targets))")"

export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT="$PORT"
# The execution API url defaults to `{api.base_url}/execution/`, so a non-default port needs both
# or the task workers dial 8080 and every task hangs.
export AIRFLOW__API__BASE_URL="$BASE"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="$BASE/execution/"
export SWF_AGENT=scripted SWF_SANDBOX=local SWF_SCM=local

# ---------------------------------------------------------------- boot

say "airflow standalone on $BASE (log: $STANDALONE_LOG)"
set -m # own process group, so cleanup can signal airflow's children too
"$BIN/airflow" standalone >"$STANDALONE_LOG" 2>&1 &
STANDALONE_PID=$!
set +m

say "waiting for $BASE/api/v2/monitor/health"
i=0
while :; do
  if ! kill -0 "$STANDALONE_PID" 2>/dev/null; then
    echo "standalone died during startup" >&2
    exit 1
  fi
  health="$(curl -fsS "$BASE/api/v2/monitor/health" 2>/dev/null || true)"
  if [ -n "$health" ] && printf '%s' "$health" | "$PY" -c '
import json
import sys

data = json.load(sys.stdin)
parts = ("metadatabase", "scheduler", "dag_processor", "triggerer")
sys.exit(0 if all(data.get(p, {}).get("status") == "healthy" for p in parts) else 1)
'; then
    echo "healthy after ${i}s: $health"
    break
  fi
  if [ $i -ge "$HEALTH_TIMEOUT_S" ]; then
    echo "health never went green in ${HEALTH_TIMEOUT_S}s: ${health:-<no response>}" >&2
    exit 1
  fi
  sleep 1
  i=$((i + 1))
done

# `airflow standalone` writes the admin password on first boot. The gates are answered as that
# user, so approvals.json records a real HITL `responded_by_user` instead of "auto".
PASSWORDS="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
[ -f "$PASSWORDS" ] || {
  echo "no $PASSWORDS — is [core] simple_auth_manager_all_admins on?" >&2
  exit 1
}
PASSWORD="$("$PY" -c "
import json

users = json.load(open('$PASSWORDS'))
print(users.get('admin') or next(iter(users.values())))
")"
# /auth/token is the auth manager's own endpoint: no /api/v2 prefix, hence not through api().
TOKEN="$(curl -fsS -X POST "$BASE/auth/token" -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$PASSWORD\"}" | field access_token || true)"
[ -n "$TOKEN" ] || {
  echo "POST /auth/token returned no access_token" >&2
  exit 1
}
export AIRFLOW_URL="$BASE" AIRFLOW_TOKEN="$TOKEN" # read by `swfactory approve`
echo "authenticated as admin"

say "waiting for the dag-processor to parse $DAG_ID"
i=0
while [ $i -lt "$PARSE_TIMEOUT_S" ] && ! api GET "/dags/$DAG_ID" >/dev/null 2>&1; do
  sleep 1
  i=$((i + 1))
done
api GET "/dags/$DAG_ID" >/dev/null || {
  echo "$DAG_ID never appeared in ${PARSE_TIMEOUT_S}s" >&2
  exit 1
}
echo "parsed after ${i}s"

# New DAGs start paused in standalone: without this the run sits queued forever.
say "unpausing $DAG_ID"
echo "is_paused now: $(api PATCH "/dags/$DAG_ID" '{"is_paused": false}' | field is_paused)"

# ---------------------------------------------------------------- trigger

say "triggering $DAG_ID with ${#ISSUES[@]} issues x $TARGETS targets: ${ISSUES[*]}"
CONF="$("$PY" -c '
import json
import sys

print(json.dumps({"logical_date": None, "conf": {"issues": sys.argv[1:]}}))
' "${ISSUES[@]}")"
RUN_ID="$(api POST "/dags/$DAG_ID/dagRuns" "$CONF" | field dag_run_id || true)"
[ -n "$RUN_ID" ] || {
  echo "trigger returned no dag_run_id" >&2
  exit 1
}
echo "dag_run_id $RUN_ID"

# ---------------------------------------------------------------- answer the gates, poll the run

cat >"$WORK/gates.py" <<'PY'
"""Gates of one run that are ready to be answered, as `<gate> <map_index>` lines.

Only task instances that have actually reached `awaiting_input` count. A HITL detail exists from the
moment the operator creates it, which is just BEFORE the task defers; answering inside that window
makes the scheduler see a stale executor event for the deferring run ("finished with state
success, but the task instance's state attribute is queued") and fail the gate. A human cannot hit
a sub-second window, a polling script can — so this filter plus the one-poll settle below is what
keeps the script's own speed from being mistaken for a factory bug.
"""

import json
import sys

dag_id, run_id, hitl_path, tis_path = sys.argv[1:5]
# Airflow 3.3 parks a HITL task in its own `awaiting_input` state; older builds show `deferred`.
AWAITING = {"awaiting_input", "deferred"}
with open(tis_path) as fh:
    parked = {
        (str(t.get("task_id")), int(t.get("map_index", -1)))
        for t in json.load(fh).get("task_instances", [])
        if str(t.get("state")) in AWAITING
    }
with open(hitl_path) as fh:
    details = json.load(fh).get("hitl_details", [])
for detail in details:
    ti = detail.get("task_instance") or {}
    if ti.get("dag_id") != dag_id or ti.get("dag_run_id") != run_id:
        continue
    key = (str(ti.get("task_id", "")), int(ti.get("map_index", -1)))
    if key[0].startswith("job.approve_") and key in parked:
        print(key[0].split("approve_", 1)[1], key[1])
PY

if [ "${SWF_APPROVE:-}" = "auto" ]; then
  say "polling; NOT answering gates (SWF_APPROVE=auto -> each gate self-approves after timeout_h)"
else
  say "polling; answering every gate as admin through the HITL API"
fi
STATE="queued"
SEEN="$WORK/seen-gates" # a gate is answered on the poll AFTER the one that first saw it parked
: >"$SEEN"
answered=0
i=0
while [ $i -lt "$RUN_TIMEOUT_S" ]; do
  STATE="$(api GET "/dags/$DAG_ID/dagRuns/$RUN_ID" | field state)"
  case "$STATE" in success | failed) break ;; esac
  if [ "${SWF_APPROVE:-}" != "auto" ]; then
    api GET "/dags/$DAG_ID/dagRuns/$RUN_ID/taskInstances?limit=500" >"$WORK/poll-tis.json"
    api GET "/dags/~/dagRuns/~/hitlDetails?response_received=false&limit=100" \
      >"$WORK/poll-hitl.json"
    pending="$("$PY" "$WORK/gates.py" "$DAG_ID" "$RUN_ID" "$WORK/poll-hitl.json" \
      "$WORK/poll-tis.json" || true)"
    while read -r gate idx; do
      [ -n "${gate:-}" ] || continue
      if ! grep -qxF "$gate $idx" "$SEEN"; then
        echo "$gate $idx" >>"$SEEN" # first sighting: let it settle one poll interval
        continue
      fi
      printf 'answering %s for job %s ... ' "$gate" "$idx"
      if "$BIN/swfactory" approve "$RUN_ID" "$gate" --blueprint "$DAG_ID" \
        --map-index "$idx" >/dev/null 2>&1; then
        echo "ok"
        answered=$((answered + 1))
      else
        echo "refused (already answered?)"
      fi
    done <<EOF
$pending
EOF
  fi
  sleep 3
  i=$((i + 3))
done
echo "run state: $STATE after ${i}s ($answered gates answered through the HITL API)"

# ---------------------------------------------------------------- report

say "task instances"
api GET "/dags/$DAG_ID/dagRuns/$RUN_ID/taskInstances?limit=500" >"$WORK/tis.json"
TI_RC=0
"$PY" - "$WORK/tis.json" <<'PY' || TI_RC=$?
"""Task-instance states of one run; anything not success/skipped is a failure of this script."""

import json
import sys
from collections import Counter

tis = json.load(open(sys.argv[1]))["task_instances"]
counts = Counter(str(t.get("state")) for t in tis)
print(f"{len(tis)} task instances: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
bad = [t for t in tis if str(t.get("state")) not in {"success", "skipped"}]
for t in sorted(bad, key=lambda t: (t["task_id"], t.get("map_index", -1))):
    print(f"  NOT OK  {t['task_id']}[{t.get('map_index', -1)}] = {t.get('state')}")
sys.exit(1 if bad else 0)
PY

cat >"$WORK/report.py" <<'PY'
"""The per-job table: what each mapped job of one stress run left on the orchestrator's disk.

job -> run id -> run dir -> workdir is derived with ``swfactory.runtime``, the same assembly the
DAG's tasks used, so a row that cannot be found is a real divergence and not a guess.
"""

import json
import sys
from pathlib import Path

from swfactory.blueprint import load
from swfactory.runtime import job_config, job_run_dir, run_id_for

dag_id, dag_run_id, work = sys.argv[1], sys.argv[2], Path(sys.argv[3]).resolve()
bp = load(dag_id)
rows, missing = [], []
for job in bp.jobs({"issues": sys.argv[4:]}):
    idx = job["job_idx"]
    cfg = job_config(bp, job, run_id=run_id_for(dag_run_id, idx), root=work)
    run_dir, workdir = job_run_dir(cfg, work), Path(cfg.workdir)
    chains = sorted(p for p in (workdir / "docs" / "factory").glob("*") if p.is_dir())
    if len(chains) != 1:  # exactly its own issue's chain: no leakage from another job
        missing.append(f"job {idx}: {len(chains)} artifact chains under {workdir}, expected 1")
        continue
    chain = chains[0]
    try:
        metrics = json.loads((chain / "metrics.json").read_text(encoding="utf-8"))
        approvals = json.loads((chain / "approvals.json").read_text(encoding="utf-8"))
    except OSError as e:
        missing.append(f"job {idx}: {e}")
        continue
    if not (run_dir / "pr.md").is_file():
        missing.append(f"job {idx}: no pr.md in {run_dir}")
    if not metrics.get("tests_passed"):
        missing.append(f"job {idx}: tests_passed={metrics.get('tests_passed')}")
    rows.append(
        [
            str(idx),
            chain.name,
            job["dir"],
            cfg.run_id,
            str(metrics.get("blueprint")),
            "pass" if metrics.get("tests_passed") else "FAIL",
            str(metrics.get("iterations")),
            " ".join(f"{a['gate']}={a['decision']}/{a['actor']}" for a in approvals),
            str(Path(run_dir).relative_to(work) / "pr.md"),
        ]
    )

head = ["job", "issue", "target", "run_id", "blueprint", "tests", "iter", "gates", "pr"]
width = [max(len(r[i]) for r in [head, *rows]) for i in range(len(head))]
for row in [head, *rows]:
    print("  ".join(c.ljust(w) for c, w in zip(row, width, strict=True)).rstrip())
print(f"\n{len(rows)} jobs, each with its own run id, run dir, workdir, remote and chain")
for m in missing:
    print(f"MISSING {m}", file=sys.stderr)
sys.exit(1 if missing or len(rows) < 2 else 0)
PY

say "per-job results"
REPORT_RC=0
"$PY" "$WORK/report.py" "$DAG_ID" "$RUN_ID" "$WORK" "${ISSUES[@]}" || REPORT_RC=$?

if [ "$TI_RC" -ne 0 ] || [ "$REPORT_RC" -ne 0 ] || [ "$STATE" != "success" ]; then
  echo "FAILED: state=$STATE task_instances_rc=$TI_RC report_rc=$REPORT_RC" >&2
  exit 1
fi
say "OK: $DAG_ID green — ${#ISSUES[@]} issues x $TARGETS targets, $answered gates answered"
echo "$RUN_ID"
