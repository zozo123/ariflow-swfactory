#!/usr/bin/env bash
# The `swf` release scenario, end to end, against a LIVE Airflow — the acceptance test for the
# Rust operator binary.
#
# It is deliberately the twin of scripts/stress_airflow.sh: same boot, same blueprint, same two
# issues across two targets, same eight authenticated approvals. The difference is who drives.
# There, every REST call is curl and every approval is `swfactory approve`; here the ONLY thing
# that talks to Airflow after the boot is `swf`. That is the claim this script exists to prove:
# an operator with no Python on their machine can install → connect → submit → inspect → approve →
# verify, and get the same factory the Python control room gets.
#
#   scripts/swf_e2e.sh [issue ...]          # default: demo/issue.md demo/issue2.md
#
# Env: SWF_E2E_KEEP=1        keep the work dir (standalone home, run dirs, logs, config) after exit
#      SWF_AIRFLOW_NO_SYNC=1 use an installed Airflow main overlay instead of the pinned release
#      SWF_BIN=<path>        an already-built `swf` (default: cargo build --release in rust/)
#
# No keys and no network: scripted agent, local sandbox, local git remote. Exit code is non-zero if
# any gate could not be answered through `swf`, any job's evidence is missing, any delivery fails
# independent verification, or the snapshot `swf` renders disagrees with the one `swfactory herd`
# renders from the same live server.
set -Eeuo pipefail

REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DAG_ID="stress"
TARGET_B="demo/target-b"   # blueprints/stress.toml's second [[targets]].dir, materialised below
HEALTH_TIMEOUT_S=240
PARSE_TIMEOUT_S=240
RUN_TIMEOUT_S=1800
CONTEXT="e2e"

if [ $# -eq 0 ]; then set -- demo/issue.md demo/issue2.md; fi
if [ $# -lt 2 ]; then
  echo "swf_e2e: pass at least 2 issues (fan-out is the point); got $#" >&2
  exit 2
fi
ISSUES=("$@")

WORK="$(mktemp -d "${TMPDIR:-/tmp}/swf-e2e.XXXXXX")"
export AIRFLOW_HOME="$WORK/airflow_home"
# `$SWF_CONFIG` names the config file outright, so this script can never disturb — or be disturbed
# by — the operator's real ~/.config/swf/config.toml. It deliberately does NOT move
# XDG_CONFIG_HOME: `gh` reads its credentials from there, and hijacking it to isolate one tool
# silently unauthenticates another. That cost this script a full run to learn.
export SWF_CONFIG="$WORK/config/swf/config.toml"
STANDALONE_LOG="$WORK/standalone.log"
STANDALONE_PID=""

say() { printf '\n=== %s\n' "$*"; }
fail() { echo "FAILED: $*" >&2; exit 1; }

cleanup() {
  rc=$?
  if [ -n "$STANDALONE_PID" ]; then
    say "shutting down standalone (process group $STANDALONE_PID)"
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
  if [ "${SWF_E2E_KEEP:-}" = "1" ]; then echo "work dir kept: $WORK"; else rm -rf "$WORK"; fi
  exit "$rc"
}
trap cleanup EXIT

# ---------------------------------------------------------------- the binary under test

if [ -n "${SWF_BIN:-}" ]; then
  SWF="$SWF_BIN"
else
  say "building swf (release)"
  cargo build --release --manifest-path "$REPO/rust/Cargo.toml" >"$WORK/cargo.log" 2>&1 ||
    { tail -30 "$WORK/cargo.log"; fail "cargo build"; }
  SWF="$REPO/rust/target/release/swf"
fi
[ -x "$SWF" ] || fail "no swf binary at $SWF"
say "swf under test: $("$SWF" --version)"

# ---------------------------------------------------------------- the project's interpreter
#
# Python appears in this script for exactly three jobs: booting Airflow, reading small JSON fields,
# and producing the reference snapshot the Rust one is diffed against. It never drives the factory.
if [ "${SWF_AIRFLOW_NO_SYNC:-}" = "1" ]; then
  PY="$(uv run --no-sync --project "$REPO" python -c 'import sys; print(sys.executable)')"
else
  PY="$(uv run --project "$REPO" --group airflow python -c 'import sys; print(sys.executable)')"
fi
"$PY" -c 'import airflow; print("Live E2E Airflow:", airflow.__version__)'
BIN="$(dirname "$PY")"
[ -x "$BIN/airflow" ] || fail "no airflow in $BIN — run: uv sync --group airflow"
export PATH="$BIN:$PATH"

field() { "$PY" -c "import json,sys;print(json.load(sys.stdin).get('$1',''))"; }

# ---------------------------------------------------------------- work dir

say "work dir $WORK"
mkdir -p "$AIRFLOW_HOME" "$(dirname "$SWF_CONFIG")" "$WORK/$(dirname "$TARGET_B")"
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

export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT="$PORT"
export AIRFLOW__API__BASE_URL="$BASE"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="$BASE/execution/"
export SWF_AGENT=scripted SWF_SANDBOX=local SWF_SCM=local

# ---------------------------------------------------------------- boot

say "airflow standalone on $BASE (log: $STANDALONE_LOG)"
set -m
"$BIN/airflow" standalone >"$STANDALONE_LOG" 2>&1 &
STANDALONE_PID=$!
set +m

say "waiting for $BASE/api/v2/monitor/health"
i=0
while :; do
  kill -0 "$STANDALONE_PID" 2>/dev/null || fail "standalone died during startup"
  health="$(curl -fsS "$BASE/api/v2/monitor/health" 2>/dev/null || true)"
  if [ -n "$health" ] && printf '%s' "$health" | "$PY" -c '
import json
import sys

data = json.load(sys.stdin)
parts = ("metadatabase", "scheduler", "dag_processor", "triggerer")
sys.exit(0 if all(data.get(p, {}).get("status") == "healthy" for p in parts) else 1)
'; then
    echo "healthy after ${i}s"
    break
  fi
  [ $i -lt "$HEALTH_TIMEOUT_S" ] || fail "health never went green in ${HEALTH_TIMEOUT_S}s"
  sleep 1
  i=$((i + 1))
done

PASSWORDS="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
[ -f "$PASSWORDS" ] || fail "no $PASSWORDS — is [core] simple_auth_manager_all_admins on?"
PASSWORD="$("$PY" -c "
import json

users = json.load(open('$PASSWORDS'))
print(users.get('admin') or next(iter(users.values())))
")"

# ---------------------------------------------------------------- 1. connect
#
# A password never enters the config file: the context stores the NAME of the variable holding it,
# which is the same discipline a shared machine needs.
export SWF_E2E_PASSWORD="$PASSWORD"

say "swf context add $CONTEXT"
"$SWF" context add "$CONTEXT" --direct \
  --airflow-url "$BASE" \
  --repo "zozo123/ariflow-swfactory" \
  --user admin --password-env SWF_E2E_PASSWORD \
  --metrics-root "$WORK" \
  --use
"$SWF" context show --json >"$WORK/context.json"
grep -q "$PASSWORD" "$WORK/context.json" &&
  fail "swf context show leaked the password — a context must never carry a secret"
grep -q "$PASSWORD" "$SWF_CONFIG" &&
  fail "swf wrote the password into its config file"
"$SWF" context list

say "swf doctor"
"$SWF" doctor --json >"$WORK/doctor.json" || echo "(doctor reports red rows; continuing — the \
sandbox providers are not configured on this machine and the e2e does not need them)"
"$PY" - "$WORK/doctor.json" <<'PY' || fail "doctor did not confirm a reachable, authenticated Airflow"
"""The one doctor row this scenario depends on: Airflow is reachable AND the token minted."""

import json
import sys

checks = json.load(open(sys.argv[1]))
by_name = {c["name"]: c for c in checks}
print(f"{len(checks)} checks: " + ", ".join(f"{c['name']}={c['status']}" for c in checks))
airflow = [c for n, c in by_name.items() if n.startswith("airflow")]
if not airflow:
    sys.exit("doctor produced no airflow check at all")
bad = [c for c in airflow if not c["ok"]]
for c in bad:
    print(f"  NOT OK  {c['name']}: {c['detail']}  fix: {c['fix']}")
sys.exit(1 if bad else 0)
PY

say "waiting for the dag-processor to parse $DAG_ID"
i=0
until "$SWF" runs list --dag "$DAG_ID" --json >/dev/null 2>&1; do
  [ $i -lt "$PARSE_TIMEOUT_S" ] || fail "$DAG_ID never appeared in ${PARSE_TIMEOUT_S}s"
  sleep 1
  i=$((i + 1))
done
echo "parsed after ${i}s"

# New DAGs start paused in standalone; without this the run sits queued forever.
say "unpausing $DAG_ID"
"$SWF" runs unpause "$DAG_ID"

# ---------------------------------------------------------------- 2. submit

say "swf submit: ${#ISSUES[@]} issues x 2 targets"
SUBMIT_ARGS=()
for issue in "${ISSUES[@]}"; do SUBMIT_ARGS+=(--issue "$issue"); done
"$SWF" submit --blueprint "$DAG_ID" "${SUBMIT_ARGS[@]}" --json >"$WORK/submit.json"
cat "$WORK/submit.json"
RUN_ID="$("$PY" -c "import json;print(json.load(open('$WORK/submit.json'))['run_id'])")"
[ -n "$RUN_ID" ] || fail "swf submit returned no run id"
echo "run $DAG_ID/$RUN_ID"

# ---------------------------------------------------------------- 3. inspect + approve
#
# `swf gates list` only reports a gate as ready once its task instance is actually parked in
# `awaiting_input`. A HITL detail exists from the moment the operator creates it, which is just
# BEFORE the task defers; answering inside that window makes the scheduler fail the gate. A human
# cannot hit a sub-second window, a polling script can — so this readiness rule, not a sleep, is
# what keeps the harness's speed from being mistaken for a factory bug.

say "polling; answering every ready gate as admin through swf (in batches)"
#
# One command per gate was the old shape of this loop, and it is the shape a factory outgrows: a
# fan-out of 20 issues x 3 targets is 120 gates, each invocation paying for its own full collect.
# `--all` with a filter answers the whole ready batch at once. It still has to poll, because gates
# ripen in waves — every job's intent gate parks before any plan gate exists — so each pass answers
# whatever is ready and comes back for the rest.
STATE="queued"
answered=0
i=0
dry_run_checked=""
while [ $i -lt "$RUN_TIMEOUT_S" ]; do
  STATE="$("$SWF" runs inspect "$DAG_ID/$RUN_ID" --json | field state)"
  case "$STATE" in success | failed) break ;; esac

  ready="$("$SWF" gates list --dag "$DAG_ID" --ready --json | "$PY" -c 'import json,sys; print(len(json.load(sys.stdin)))')"
  if [ "${ready:-0}" -gt 0 ]; then
    # The first time there is anything to answer, prove the dry run is honest: it must name the
    # same gates and answer none of them. A --dry-run that quietly writes is worse than no flag.
    if [ -z "$dry_run_checked" ]; then
      say "swf gates approve --all --dry-run (must touch nothing)"
      "$SWF" gates approve --all --dag "$DAG_ID" --dry-run --json >"$WORK/dry-run.json" ||
        fail "swf gates approve --dry-run"
      "$PY" - "$WORK/dry-run.json" "$ready" <<'PYEOF' || fail "the dry run did not describe the ready batch"
import json
import sys

report = json.load(open(sys.argv[1]))
expected = int(sys.argv[2])
rows = report if isinstance(report, list) else report.get("gates", report.get("results", []))
planned = [r for r in rows if r.get("outcome") == "planned"]
wrote = [r for r in rows if r.get("outcome") == "answered"]
print(f"  dry run: planned {len(planned)} gate(s), answered {len(wrote)}")
sys.exit(0 if planned and not wrote else 1)
PYEOF
      still="$("$SWF" gates list --dag "$DAG_ID" --ready --json | "$PY" -c 'import json,sys; print(len(json.load(sys.stdin)))')"
      [ "$still" = "$ready" ] ||
        fail "the dry run changed the world: $ready ready gates before, $still after"
      echo "  the same $still gates are still waiting — the dry run wrote nothing"
      dry_run_checked=1
    fi

    printf 'approving %s ready gate(s) in one call ... ' "$ready"
    if "$SWF" gates approve --all --dag "$DAG_ID" --yes --json >"$WORK/bulk-$i.json" 2>"$WORK/bulk-$i.err"; then
      got="$("$PY" -c '
import json,sys
rows = json.load(open(sys.argv[1]))
rows = rows if isinstance(rows, list) else rows.get("gates", rows.get("results", []))
print(len([r for r in rows if r.get("outcome") == "answered"]))' "$WORK/bulk-$i.json")"
      answered=$((answered + got))
      echo "answered $got (total $answered)"
    else
      echo "batch reported a failure"
      sed 's/^/    /' "$WORK/bulk-$i.err" >&2
    fi
  fi
  sleep 3
  i=$((i + 3))
done
echo "run state: $STATE after ${i}s ($answered gates answered through swf)"

EXPECTED_GATES=$(( ${#ISSUES[@]} * 2 * 2 ))   # issues x targets x (intent, plan)
[ "$answered" -eq "$EXPECTED_GATES" ] ||
  fail "answered $answered gates through swf, expected $EXPECTED_GATES"

# ---------------------------------------------------------------- 4. what the operator sees

say "swf jobs list"
"$SWF" jobs list --json >"$WORK/jobs.json"
"$SWF" jobs list | tee "$WORK/screen-jobs.txt"

say "swf attention"
"$SWF" attention | tee "$WORK/screen-attention.txt"

say "swf jobs inspect (every job)"
"$PY" -c "
import json
print('\n'.join(j['id'] for r in json.load(open('$WORK/jobs.json')) for j in [r] if r.get('id')))
" >"$WORK/job-ids.txt" || true
while read -r job_id; do
  [ -n "${job_id:-}" ] || continue
  "$SWF" jobs inspect "$job_id" >"$WORK/job-$(echo "$job_id" | tr '/#:' '___').txt" ||
    fail "swf jobs inspect $job_id"
done <"$WORK/job-ids.txt"
echo "inspected $(wc -l <"$WORK/job-ids.txt" | tr -d ' ') jobs"

say "swf logs (one task attempt, no follow)"
FIRST_JOB="$(head -1 "$WORK/job-ids.txt")"
[ -n "$FIRST_JOB" ] || fail "no jobs to read logs for"
"$SWF" logs "$FIRST_JOB" --task setup >"$WORK/logs.txt" || fail "swf logs $FIRST_JOB"
head -5 "$WORK/logs.txt"

# ---------------------------------------------------------------- 5. the equivalence claim
#
# Same live server, same moment: the snapshot the Rust client renders and the one the Python
# control room renders must describe the same factory. Volatile fields (timestamps, in-flight task
# states) are normalised away; the identities, states, gates and issues are not.

# ---------------------------------------------------------------- 5b. the interactive face
#
# Render snapshots prove the widgets draw correctly. They cannot prove that the binary takes a real
# terminal over, paints THIS factory into it, answers a keystroke and gives the terminal back — so
# that is done here, in a pty, against the server the rest of this script has been driving.

say "swf tui (a real terminal, this live factory, then q)"
# `2` is the jobs view. The default screen is `attention`, which on a healthy finished run is
# correctly empty — asserting a run id against it would be asserting that something went wrong.
"$PY" "$REPO/scripts/tui_smoke.py" --bin "$SWF" --key 2 \
  --expect "$DAG_ID" --expect "demo/issue.md" --expect "success" \
  --out "$WORK/tui-frame.txt" || fail "swf tui did not render, quit and restore the terminal"
say "the frame swf tui painted"
sed -n '1,26p' "$WORK/tui-frame.txt"

say "swf snapshot == swfactory herd --once --json"
"$SWF" snapshot --json >"$WORK/snapshot-rust.json"
AIRFLOW_URL="$BASE" AIRFLOW_USER=admin AIRFLOW_PASSWORD="$PASSWORD" \
  "$BIN/swfactory" herd --once --json --metrics-root "$WORK" >"$WORK/snapshot-py.json"
"$PY" "$REPO/scripts/snapshot_diff.py" "$WORK/snapshot-py.json" "$WORK/snapshot-rust.json" ||
  fail "the two control rooms disagree about the same live factory"

# ---------------------------------------------------------------- 6. verify the deliveries

say "swf deliveries verify (independent re-run from a clean checkout)"
#
# This line runs with `scm = local`, so the factory published each job's branch into a bare
# repository inside that job's run directory rather than to GitHub. That is a real delivery and
# it gets the real treatment: `swf` clones the branch from that remote into a fresh directory,
# reads the contract out of the CHECKOUT, and re-runs the target's own test command there.
# Nothing about the verdict is taken from the worker's workdir.
VERIFIED=0
while read -r job_id; do
  [ -n "${job_id:-}" ] || continue
  idx="${job_id##*#}"
  run_id="$("$PY" -c "
from swfactory.runtime import run_id_for
print(run_id_for('$RUN_ID', int('$idx')))
")"
  remote="$WORK/.factory/$run_id/remote.git"
  [ -d "$remote" ] || fail "job $idx published nothing: no $remote"
  branch="$(git -C "$remote" for-each-ref --format='%(refname:short)' 'refs/heads/factory/*' | head -1)"
  [ -n "$branch" ] || fail "job $idx has no factory/* branch in $remote"
  printf 'verifying job %s: %s\n' "$idx" "$branch"
  # A verdict short of `independently_verified` exits non-zero, and that is the answer, not an
  # error: the report says which evidence row did not hold. So read the report either way and let
  # it do the explaining — aborting on the exit code alone would hide the diagnosis.
  "$SWF" deliveries verify "$branch" --from "$remote" --branch "$branch" --clone \
    --repo "zozo123/ariflow-swfactory" --json >"$WORK/verify-$idx.json" \
    2>"$WORK/verify-$idx.err" || true
  "$PY" "$REPO/scripts/verify_report.py" "$WORK/verify-$idx.json" "$idx" || {
    sed 's/^/    /' "$WORK/verify-$idx.err" >&2
    fail "job $idx was not independently verified"
  }
  VERIFIED=$((VERIFIED + 1))
done <"$WORK/job-ids.txt"

EXPECTED_DELIVERIES=$(( ${#ISSUES[@]} * 2 ))
[ "$VERIFIED" -eq "$EXPECTED_DELIVERIES" ] ||
  fail "independently verified $VERIFIED deliveries, expected $EXPECTED_DELIVERIES"
echo "$VERIFIED deliveries independently verified"

if [ "$STATE" != "success" ]; then
  fail "run state=$STATE"
fi
say "OK: $DAG_ID green — ${#ISSUES[@]} issues x 2 targets, $answered gates answered through swf, \
$(( ${#ISSUES[@]} * 2 )) deliveries independently verified"
echo "$RUN_ID"
