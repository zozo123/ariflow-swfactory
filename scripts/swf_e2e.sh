#!/usr/bin/env bash
# The `swf` release scenario, end to end, against a LIVE Airflow — the acceptance test for the Rust
# operator binary. One body of assertions, run TWICE, once per connection mode:
#
#   direct   `swf context add --direct` — the console holds the Airflow credential. The CLI's own
#            help calls this "explicit compatibility mode".
#   backend  what docs/factory-backend.md calls normal: a live `swfactory backend` holds the
#            credentials and the console speaks `/v1` to it.
#
# The backend leg is new, and untested is how it stayed broken: a `/v1/doctor` schema mismatch made
# `swf doctor` structurally unable to pass (#1217), and Airflow workers with no SWF_BACKEND_URL /
# SWF_BACKEND_TOKEN killed every backend-submitted job in `setup` (managed cells fail closed on
# purpose — src/swfactory/cell_callback.py). So the legs differ in exactly one step, `connect`, and
# share every assertion after it; proving one and assuming the other is how the assumed one rots.
#
# Otherwise the twin of scripts/stress_airflow.sh: same boot, same blueprint, same two issues across
# two targets, same authenticated approvals. The difference is who drives — there every REST call is
# curl and every approval is `swfactory approve`; here the only thing talking to Airflow after the
# boot is `swf`, direct or through the backend.
#
# ONE Airflow serves both legs: they run in sequence and each asserts only against its own run id,
# so a second boot would buy three minutes of nothing — and would hide the mixed deployment every
# migrating operator has, where workers carrying SWF_BACKEND_URL must still run a direct submission.
#
#   scripts/swf_e2e.sh [issue ...]          # default: demo/issue.md demo/issue2.md
#
# Env: SWF_E2E_KEEP=1        keep the work dir (standalone home, run dirs, logs, config) after exit
#      SWF_E2E_LEGS=...      which legs to run, space separated (default "direct backend")
#                            the two-session leg always runs after them: it is the only leg that
#                            fails the way a real deployment does, with several harness sessions on
#                            one repository at once
#      SWF_AIRFLOW_NO_SYNC=1 use an installed Airflow main overlay instead of the pinned release
#      SWF_BIN=<path>        an already-built `swf` (default: cargo build --release in rust/)
#      SWF_GATE_SETTLE_SECS  how long a gate must have existed before `swf` will answer it
#                            (default 5). Raise it where the scheduler reconciles slowly — a small
#                            CI runner — and read the failure it prevents above settle_run.
#
# No keys and no network: scripted agent, local sandbox, local git remote. Exit code is non-zero if
# either leg cannot answer a gate through `swf`, any job's evidence is missing, any delivery fails
# independent verification, the snapshot `swf` renders disagrees with `swfactory herd`'s of the same
# server, or `swf doctor` cannot succeed against a healthy backend.
set -Eeuo pipefail

REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DAG_ID="stress"
TARGET_B="demo/target-b"   # blueprints/stress.toml's second [[targets]].dir, materialised below
E2E_REPO="zozo123/ariflow-swfactory"
HEALTH_TIMEOUT_S=240; PARSE_TIMEOUT_S=240; BACKEND_TIMEOUT_S=60; RUN_TIMEOUT_S=1800
LEGS="${SWF_E2E_LEGS:-direct backend}"

if [ $# -eq 0 ]; then set -- demo/issue.md demo/issue2.md; fi
if [ $# -lt 2 ]; then
  echo "swf_e2e: pass at least 2 issues (fan-out is the point); got $#" >&2
  exit 2
fi
ISSUES=("$@")
EXPECTED_JOBS=$(( $# * 2 ))          # issues x targets
EXPECTED_GATES=$(( $# * 2 * 2 ))     # issues x targets x (intent, plan)

WORK="$(mktemp -d "${TMPDIR:-/tmp}/swf-e2e.XXXXXX")"
export AIRFLOW_HOME="$WORK/airflow_home"
# `$SWF_CONFIG` names the config file outright, so this script can never disturb — or be disturbed
# by — the operator's real ~/.config/swf/config.toml. It deliberately does NOT move XDG_CONFIG_HOME:
# `gh` reads its credentials from there, and hijacking it to isolate one tool silently
# unauthenticates another. That cost this script a full run to learn.
export SWF_CONFIG="$WORK/config/swf/config.toml"
STANDALONE_LOG="$WORK/standalone.log"; STANDALONE_PID=""
BACKEND_LOG="$WORK/backend.log";       BACKEND_PID=""

say() { printf '\n=== %s\n' "$*"; }
fail() { echo "FAILED: $*" >&2; exit 1; }

# SIGINT to the whole GROUP, then a bounded wait, then SIGKILL. The group, because `airflow
# standalone` stops its scheduler / api-server / dag-processor / triggerer children on
# KeyboardInterrupt and they are its subprocesses. The bound, because a background job started with
# job control OFF inherits SIGINT ignored: `swfactory backend` outlived every INT this trap sent and
# the bare `wait` after it blocked forever — a hung harness behind a green factory, which reads
# exactly like a hung factory. Hence `set -m` on both starts, and no unbounded wait.
stop_group() { # stop_group <pid> <what>
  local pid=${1:-} what=$2 i=0
  [ -n "$pid" ] || return 0
  say "shutting down $what (process group $pid)"
  kill -INT -- "-$pid" 2>/dev/null || true
  while kill -0 "$pid" 2>/dev/null && [ $i -lt 60 ]; do sleep 0.5; i=$((i + 1)); done
  kill -KILL -- "-$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  rc=$?
  # The backend goes first: a worker still finishing then gets a refused callback it can report,
  # rather than a socket to a half-dead process.
  stop_group "$BACKEND_PID" "swfactory backend"
  stop_group "$STANDALONE_PID" "airflow standalone"
  if [ "$rc" -ne 0 ]; then
    for log in "$STANDALONE_LOG" "$BACKEND_LOG"; do
      [ -s "$log" ] || continue
      say "$(basename "$log") (tail)"; tail -40 "$log" || true
    done
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
# Python has four jobs here: booting Airflow, serving the backend leg, reading small JSON fields,
# and rendering the reference snapshot. It never drives the factory.
if [ "${SWF_AIRFLOW_NO_SYNC:-}" = "1" ]; then
  PY="$(uv run --no-sync --project "$REPO" python -c 'import sys; print(sys.executable)')"
else
  PY="$(uv run --project "$REPO" --group airflow python -c 'import sys; print(sys.executable)')"
fi
"$PY" -c 'import airflow; print("Live E2E Airflow:", airflow.__version__)'
BIN="$(dirname "$PY")"
[ -x "$BIN/airflow" ] || fail "no airflow in $BIN — run: uv sync --group airflow"
export PATH="$BIN:$PATH"

py() { "$PY" - "$@"; }   # a heredoc script with arguments; stdin carries the script, never data
field() { "$PY" -c "import json,sys;print(json.load(sys.stdin).get('$1',''))"; }
port() { "$PY" -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

# ---------------------------------------------------------------- work dir and environment

say "work dir $WORK"
mkdir -p "$AIRFLOW_HOME" "$(dirname "$SWF_CONFIG")" "$WORK/$(dirname "$TARGET_B")"
cp -R "$REPO/demo/target" "$WORK/$TARGET_B"
find "$WORK/$TARGET_B" \( -name __pycache__ -o -name .pytest_cache -o -name .venv \) -prune \
  -exec rm -rf {} + 2>/dev/null || true
cd "$WORK"

BASE="http://localhost:$(port)"
BACKEND_URL="http://127.0.0.1:$(port)"
# Minted before anything boots, because the Airflow workers need it from the moment they start —
# see boot_airflow. `swfactory backend` refuses a token under 32 characters.
SWF_BACKEND_TOKEN="$("$PY" -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SWF_BACKEND_TOKEN

export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT="${BASE##*:}"
# The execution API url defaults to `{api.base_url}/execution/`, so a non-default port needs both
# or the task workers dial 8080 and every task hangs.
export AIRFLOW__API__BASE_URL="$BASE"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="$BASE/execution/"
export SWF_AGENT=scripted SWF_SANDBOX=local SWF_SCM=local

# ---------------------------------------------------------------- boot

boot_airflow() {
  say "airflow standalone on $BASE (log: $STANDALONE_LOG)"
  set -m   # own process group, so cleanup can signal airflow's children too
  # SWF_BACKEND_URL is exported HERE and nowhere else, and both halves matter. The workers need it:
  # a backend-submitted run's jobs are managed Factory Cells, and `cell_callback.transition` fails
  # closed without it ("managed Airflow workers require SWF_BACKEND_URL and SWF_BACKEND_TOKEN"),
  # which killed every such job in `setup`. This shell must NOT have it: `swf` reads it as an
  # override of the context's own backend_url, so exporting it would route the DIRECT leg through
  # the backend and collapse the two legs into one.
  SWF_BACKEND_URL="$BACKEND_URL" "$BIN/airflow" standalone >"$STANDALONE_LOG" 2>&1 &
  STANDALONE_PID=$!
  set +m
  say "waiting for $BASE/api/v2/monitor/health"
  local i=0 health
  while :; do
    kill -0 "$STANDALONE_PID" 2>/dev/null || fail "standalone died during startup"
    health="$(curl -fsS "$BASE/api/v2/monitor/health" 2>/dev/null || true)"
    if [ -n "$health" ] && printf '%s' "$health" | "$PY" -c '
import json, sys

data = json.load(sys.stdin)
parts = ("metadatabase", "scheduler", "dag_processor", "triggerer")
sys.exit(0 if all(data.get(p, {}).get("status") == "healthy" for p in parts) else 1)
'; then echo "healthy after ${i}s"; break; fi
    [ $i -lt "$HEALTH_TIMEOUT_S" ] || fail "health never went green in ${HEALTH_TIMEOUT_S}s"
    sleep 1; i=$((i + 1))
  done
}

# `airflow standalone` writes the admin password on first boot. Gates are answered as that user, so
# the factory records a real HITL respondent instead of "auto". The password reaches `swf` only as
# the NAME of the variable holding it — the discipline a shared machine needs.
mint_admin() {
  local file="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
  [ -f "$file" ] || fail "no $file — is [core] simple_auth_manager_all_admins on?"
  PASSWORD="$("$PY" -c 'import json,sys
u = json.load(open(sys.argv[1])); print(u.get("admin") or next(iter(u.values())))' "$file")"
  export SWF_E2E_PASSWORD="$PASSWORD"
}

# The credential-holding process the backend leg talks to: it gets Airflow's password, the console
# never does. Its state root is deliberately not `.factory`, where the factory's own run dirs live —
# a backend writing cells into that tree would make "job N published nothing" ambiguous.
start_backend() {
  say "swfactory backend on $BACKEND_URL (log: $BACKEND_LOG)"
  set -m   # its own process group, and not the SIGINT-ignoring background job of a shell without it
  AIRFLOW_URL="$BASE" AIRFLOW_USER=admin AIRFLOW_PASSWORD="$PASSWORD" \
    SWF_REPO="$E2E_REPO" SWF_METRICS_ROOT="$WORK" SWF_STATE_ROOT="$WORK/.backend-state" \
    "$BIN/swfactory" backend --host 127.0.0.1 --port "${BACKEND_URL##*:}" >"$BACKEND_LOG" 2>&1 &
  BACKEND_PID=$!
  set +m
  # `/v1/readiness` is the backend's own public probe and reports `read_ready`, so this waits for a
  # SERVING backend rather than a listening socket; polling the authenticated API instead would
  # confuse "not up yet" with "token wrong".
  local i=0
  until curl -fsS "$BACKEND_URL/v1/readiness" >"$WORK/readiness.json" 2>/dev/null; do
    kill -0 "$BACKEND_PID" 2>/dev/null ||
      { tail -20 "$BACKEND_LOG" >&2; fail "backend died at startup"; }
    [ $i -lt "$BACKEND_TIMEOUT_S" ] || fail "backend never became ready in ${BACKEND_TIMEOUT_S}s"
    sleep 1; i=$((i + 1))
  done
  echo "ready after ${i}s: $(cat "$WORK/readiness.json")"
}

wait_parsed() {
  say "[$MODE] waiting for the dag-processor to parse $DAG_ID"
  local i=0
  until "$SWF" runs list --dag "$DAG_ID" --json >/dev/null 2>&1; do
    [ $i -lt "$PARSE_TIMEOUT_S" ] || fail "$DAG_ID never appeared in ${PARSE_TIMEOUT_S}s"
    sleep 1; i=$((i + 1))
  done
  echo "parsed after ${i}s"
  "$SWF" runs unpause "$DAG_ID"   # new DAGs start paused; without this the run sits queued forever
}

# ---------------------------------------------------------------- 1. connect + doctor
#
# The only step that differs between the legs. Everything after it is identical.
connect() {
  local auth=() secret file
  case "$MODE" in
    direct) auth=(--direct --user admin --password-env SWF_E2E_PASSWORD) ;;
    # No Airflow credential at all — the backend holds it, and --airflow-url is only the browser
    # address the console prints. This is the invocation docs/factory-backend.md documents.
    backend) auth=(--backend-url "$BACKEND_URL") ;;
    *) fail "unknown leg $MODE" ;;
  esac
  say "[$MODE] swf context add $CTX"
  "$SWF" context add "$CTX" "${auth[@]}" --airflow-url "$BASE" --repo "$E2E_REPO" \
    --metrics-root "$WORK" --use
  "$SWF" context show --json >"$LEG/context.json"
  # Two different code paths, so both are checked: `show` has to redact, and the store has to never
  # have written it. Both secrets, so the backend leg cannot pass by having no password to leak.
  for secret in "$PASSWORD" "$SWF_BACKEND_TOKEN"; do
    for file in "$LEG/context.json" "$SWF_CONFIG"; do
      ! grep -qF -- "$secret" "$file" || fail "[$MODE] a secret reached $file"
    done
  done
  "$SWF" context list

  say "[$MODE] swf doctor"
  if [ "$MODE" = backend ]; then
    # #1217: `/v1/doctor` answered rows carrying `status` but no `ok`. `swf_domain::doctor::Check`
    # has `ok: bool` with no default and no alias, so every row failed to deserialize, the whole
    # response was discarded, and `swf doctor` printed one fabricated failure blaming the operator's
    # token — against a backend that was working. `doctor` is what someone runs when nothing else
    # works, so here it must be able to SUCCEED: this asserts the exit code, not a row.
    "$SWF" doctor --json >"$LEG/doctor.json" ||
      fail "[backend] swf doctor could not succeed against a healthy backend (#1217)"
  else
    "$SWF" doctor --json >"$LEG/doctor.json" || echo "(doctor reports red rows; continuing — the \
sandbox providers are not configured on this machine and the e2e does not need them)"
  fi
  py "$LEG/doctor.json" <<'PY' || fail "[$MODE] doctor did not confirm a reachable Airflow"
"""The one doctor row this scenario depends on: Airflow is reachable AND the token minted."""

import json, sys

checks = json.load(open(sys.argv[1]))
print(f"{len(checks)} checks: " + ", ".join(f"{c['name']}={c['status']}" for c in checks))
if not (airflow := [c for c in checks if c["name"].startswith("airflow")]):
    sys.exit("doctor produced no airflow check at all")
for c in (bad := [c for c in airflow if not c["ok"]]):
    print(f"  NOT OK  {c['name']}: {c['detail']}  fix: {c['fix']}")
sys.exit(1 if bad else 0)
PY
}

# ---------------------------------------------------------------- 2. submit
submit_run() {
  say "[$MODE] swf submit: ${#ISSUES[@]} issues x 2 targets"
  local args=() issue
  for issue in "${ISSUES[@]}"; do args+=(--issue "$issue"); done
  "$SWF" submit --blueprint "$DAG_ID" "${args[@]}" --json >"$LEG/submit.json"
  cat "$LEG/submit.json"
  RUN_ID="$(field run_id <"$LEG/submit.json")"
  [ -n "$RUN_ID" ] || fail "[$MODE] swf submit returned no run id"
  echo "run $DAG_ID/$RUN_ID"
}

# ---------------------------------------------------------------- 3. inspect + approve
#
# `swf gates list --ready` reports a gate only once its task instance is actually parked in
# `awaiting_input`. A HITL detail exists from the moment the operator creates it, just BEFORE the
# task defers, and answering inside that window makes the scheduler fail the gate. A human cannot
# hit a sub-second window, a polling script can — so this readiness rule, not a sleep, keeps the
# harness's speed from being mistaken for a factory bug. Readiness alone is not enough either: the
# window closes only once the SCHEDULER has reconciled the worker that parked the task, so `swf`
# also refuses a gate younger than $SWF_GATE_SETTLE_SECS (default 5) by the SERVER's own
# `created_at`. Each pass below is a fresh `swf` with no memory of the last, so a rule counted from
# the client's first sighting would restart every pass and become a flat sleep; counted from the
# server's stamp, a gate that ripened during the `sleep 3` costs this loop nothing.

check_dry_run() {
  say "[$MODE] swf gates approve --all --dry-run (must touch nothing)"
  "$SWF" gates approve --all --dag "$DAG_ID" --dry-run --json >"$LEG/dry-run.json" ||
    fail "[$MODE] swf gates approve --dry-run"
  "$SWF" gates list --dag "$DAG_ID" --ready --json >"$LEG/after-dry-run.json"
  py "$LEG/dry-run.json" "$LEG/after-dry-run.json" <<'PY' || fail "[$MODE] the dry run was not honest"
"""Three properties of one --dry-run: its report, and the world immediately after it.

1. It described the batch — planned gates, answered none. A --dry-run that quietly writes is worse
   than no flag at all.
2. Every gate it planned is STILL PENDING, by IDENTITY and never by count: gates ripen continuously,
   so a fourth can park while the dry run is still printing the first three. A changed count would
   prove nothing and an equal one would have been luck.
3. Every ready gate carries the server's own `created_at`, which anchors the settle window. If that
   field stops arriving (an Airflow rename, a proxy that strips it) `swf` falls back to its weaker
   per-process clock and every run here would still pass while the rule this exists to prove had
   quietly stopped being enforced. Assert the input, not only the outcome.
"""

import json, sys

report = json.load(open(sys.argv[1]))
rows = report if isinstance(report, list) else report.get("results", report.get("gates", []))
planned = {r["id"] for r in rows if r.get("outcome") == "planned"}
wrote = [r for r in rows if r.get("outcome") == "answered"]
print(f"  dry run: planned {len(planned)} gate(s), answered {len(wrote)}")
if not planned or wrote:
    sys.exit("  the dry run did not describe the ready batch")
still = json.load(open(sys.argv[2]))
pending = {g["id"] for g in still}
print(f"  {len(planned & pending)}/{len(planned)} planned gates still waiting after the dry run")
for gate in sorted(planned - pending):
    print(f"    a dry run answered {gate}", file=sys.stderr)
missing = [r.get("id", "?") for r in still if not r.get("created_at")]
if missing:
    print("  no server created_at on: " + ", ".join(map(str, missing)), file=sys.stderr)
print(f"  {len(still)} ready gate(s) carry a server created_at — the settle window is anchored")
sys.exit(1 if (planned - pending) or missing else 0)
PY
}

# Poll until the run settles, answering whatever is ready on each pass. One command per gate is the
# shape a factory outgrows — 20 issues x 3 targets is 120 gates, each invocation paying for its own
# full collect — so `--all` with a filter answers the ready batch at once. It still polls, because
# gates ripen in waves: every job's intent gate parks before any plan gate exists.
settle_run() {
  say "[$MODE] polling; answering every ready gate as admin through swf (in batches)"
  STATE="queued"; ANSWERED=0
  local i=0 ready got dry_run_checked=""
  while [ $i -lt "$RUN_TIMEOUT_S" ]; do
    STATE="$("$SWF" runs inspect "$DAG_ID/$RUN_ID" --json | field state)"
    case "$STATE" in success | failed) break ;; esac
    ready="$("$SWF" gates list --dag "$DAG_ID" --ready --json |
      "$PY" -c 'import json,sys; print(len(json.load(sys.stdin)))')"
    if [ "${ready:-0}" -gt 0 ]; then
      [ -n "$dry_run_checked" ] || { check_dry_run; dry_run_checked=1; }
      printf 'approving %s ready gate(s) in one call ... ' "$ready"
      # Count from the REPORT, never the exit code. A batch that answers four and fails on a fifth
      # exits non-zero having answered four; discarding that would make this script disagree with
      # the factory about work that actually happened.
      "$SWF" gates approve --all --dag "$DAG_ID" --yes --json \
        >"$LEG/bulk-$i.json" 2>"$LEG/bulk-$i.err" || true
      got="$(py "$LEG/bulk-$i.json" <<'PY' || echo 0
import json, sys

try:
    rows = json.load(open(sys.argv[1]))
except (OSError, ValueError):
    print(0); raise SystemExit
rows = rows if isinstance(rows, list) else rows.get("results", rows.get("gates", []))
print(len([r for r in rows if r.get("outcome") == "answered"]))
for r in (r for r in rows if r.get("outcome") not in ("answered", "planned")):
    print(f"    {r.get('outcome')}: {r.get('id', '?')} — {r.get('detail', '')}", file=sys.stderr)
PY
)"
      ANSWERED=$((ANSWERED + got)); echo "answered $got (total $ANSWERED)"
    fi
    sleep 3; i=$((i + 3))
  done
  echo "run state: $STATE after ${i}s ($ANSWERED gates answered through swf)"
  # A bare "failed" is not a diagnosis, and this runs BEFORE the gate-count assertion so the
  # diagnosis survives even when the count is what fails.
  [ "$STATE" = success ] || red_tasks
  [ "$ANSWERED" -eq "$EXPECTED_GATES" ] ||
    fail "[$MODE] answered $ANSWERED gates through swf, expected $EXPECTED_GATES"
}

# Which task instances are red, job by job, through `swf` alone. The next person to read this log is
# trying to tell a factory regression from a flake, and cannot do it from one word.
red_tasks() {
  say "[$MODE] what actually failed"
  collect_jobs || true
  "$SWF" jobs list || true
  local job_id
  while read -r job_id; do
    [ -n "${job_id:-}" ] || continue
    "$SWF" jobs inspect "$job_id" --json >"$LEG/red.json" 2>/dev/null || continue
    py "$LEG/red.json" "$job_id" <<'PY' || true
import json, sys

job = json.load(open(sys.argv[1]))
red = [t for t in job.get("tasks") or [] if t.get("state") not in ("success", "skipped")]
if red:
    print(f"  {sys.argv[2]} = {job.get('state')}")
    for t in red:
        print(f"    RED  {t['task_id']}[{t.get('map_index', -1)}] = {t.get('state')}")
PY
  done <"$LEG/job-ids.txt"
}

# ---------------------------------------------------------------- 4. what the operator sees

# The jobs of THIS run only. Both legs' runs live on one Airflow, so an unfiltered `jobs list` would
# let the backend leg re-verify the direct leg's deliveries and call four of them eight.
collect_jobs() {
  "$SWF" jobs list --json >"$LEG/jobs.json"
  "$PY" -c 'import json,sys
rows = json.load(open(sys.argv[1]))
print("\n".join(r["id"] for r in rows if r.get("id") and r.get("run_id") == sys.argv[2]))' \
    "$LEG/jobs.json" "$RUN_ID" >"$LEG/job-ids.txt"
  JOBS="$(grep -c . <"$LEG/job-ids.txt" || true)"
}

operator_surface() {
  say "[$MODE] swf jobs list"
  "$SWF" jobs list | tee "$LEG/screen-jobs.txt"
  [ "$JOBS" -eq "$EXPECTED_JOBS" ] || fail "[$MODE] $JOBS jobs for $RUN_ID, expected $EXPECTED_JOBS"
  say "[$MODE] swf attention"
  "$SWF" attention | tee "$LEG/screen-attention.txt"
  say "[$MODE] swf jobs inspect (every job)"
  local job_id first
  while read -r job_id; do
    [ -n "${job_id:-}" ] || continue
    "$SWF" jobs inspect "$job_id" >"$LEG/job-$(echo "$job_id" | tr '/#:' '___').txt" ||
      fail "[$MODE] swf jobs inspect $job_id"
  done <"$LEG/job-ids.txt"
  echo "inspected $JOBS jobs"
  say "[$MODE] swf logs (one task attempt, no follow)"
  first="$(head -1 "$LEG/job-ids.txt")"
  [ -n "$first" ] || fail "[$MODE] no jobs to read logs for"
  "$SWF" logs "$first" --task setup >"$LEG/logs.txt" || fail "[$MODE] swf logs $first"
  head -5 "$LEG/logs.txt"

  # Render snapshots prove the widgets draw correctly, never that the binary takes a real terminal
  # over, paints THIS factory into it, answers a keystroke and gives the terminal back — so that is
  # done in a pty. `2` is the jobs view; the default `attention` screen is correctly empty on a
  # healthy finished run. tui_smoke.py inherits SWF_CONFIG and SWF_BACKEND_TOKEN, so it connects the
  # way this leg does.
  say "[$MODE] swf tui (a real terminal, this live factory, then q)"
  "$PY" "$REPO/scripts/tui_smoke.py" --bin "$SWF" --key 2 \
    --expect "$DAG_ID" --expect "${ISSUES[0]}" --expect "success" --out "$LEG/tui-frame.txt" ||
    fail "[$MODE] swf tui did not render, quit and restore the terminal"
  sed -n '1,26p' "$LEG/tui-frame.txt"
}

# ---------------------------------------------------------------- 5. the equivalence claim
#
# Same live server, same moment: the snapshot the Rust client renders and the one the Python control
# room renders must describe the same factory. Volatile fields (timestamps, in-flight task states)
# normalise away; identities, states, gates and issues do not. In backend mode Rust reaches Airflow
# only through `/v1/airflow/...`, so this also proves the compatibility mount keeps that shape.
snapshot_equivalence() {
  say "[$MODE] swf snapshot == swfactory herd --once --json"
  "$SWF" snapshot --json >"$LEG/snapshot-rust.json"
  AIRFLOW_URL="$BASE" AIRFLOW_USER=admin AIRFLOW_PASSWORD="$PASSWORD" \
    "$BIN/swfactory" herd --once --json --metrics-root "$WORK" >"$LEG/snapshot-py.json"
  "$PY" "$REPO/scripts/snapshot_diff.py" "$LEG/snapshot-py.json" "$LEG/snapshot-rust.json" ||
    fail "[$MODE] the two control rooms disagree about the same live factory"
}

# ---------------------------------------------------------------- 6. verify the deliveries
#
# `scm = local`, so each job's branch was published into a bare repository in its run directory
# rather than to GitHub. That is a real delivery and gets the real treatment: `swf` clones the branch
# from that remote into a fresh directory, reads the contract out of the CHECKOUT, and re-runs the
# target's own test command there. Nothing is taken from the worker's workdir.
verify_deliveries() {
  say "[$MODE] swf deliveries verify (independent re-run from a clean checkout)"
  local verified=0 job_id idx run_id remote branch
  while read -r job_id; do
    [ -n "${job_id:-}" ] || continue
    idx="${job_id##*#}"
    run_id="$("$PY" -c 'import sys
from swfactory.runtime import run_id_for
print(run_id_for(sys.argv[1], int(sys.argv[2])))' "$RUN_ID" "$idx")"
    remote="$WORK/.factory/$run_id/remote.git"
    [ -d "$remote" ] || fail "[$MODE] job $idx published nothing: no $remote"
    branch="$(git -C "$remote" for-each-ref --format='%(refname:short)' 'refs/heads/factory/*' | head -1)"
    [ -n "$branch" ] || fail "[$MODE] job $idx has no factory/* branch in $remote"
    printf 'verifying job %s: %s\n' "$idx" "$branch"
    # A verdict short of `independently_verified` exits non-zero, and that is the answer, not an
    # error: the report names the evidence row that did not hold. So read the report either way and
    # let it explain — aborting on the exit code alone would hide the diagnosis.
    "$SWF" deliveries verify "$branch" --from "$remote" --branch "$branch" --clone \
      --repo "$E2E_REPO" --json >"$LEG/verify-$idx.json" 2>"$LEG/verify-$idx.err" || true
    "$PY" "$REPO/scripts/verify_report.py" "$LEG/verify-$idx.json" "$idx" || {
      sed 's/^/    /' "$LEG/verify-$idx.err" >&2
      fail "[$MODE] job $idx was not independently verified"
    }
    verified=$((verified + 1))
  done <"$LEG/job-ids.txt"
  [ "$verified" -eq "$EXPECTED_JOBS" ] ||
    fail "[$MODE] independently verified $verified deliveries, expected $EXPECTED_JOBS"
  echo "$verified deliveries independently verified"
}

# ---------------------------------------------------------------- the scenario, once per mode
leg() {
  MODE="$1"; LEG="$WORK/$MODE"; CTX="e2e-$MODE"; mkdir -p "$LEG"
  say "############ leg: $MODE"
  connect; wait_parsed; submit_run; settle_run
  collect_jobs; operator_surface; snapshot_equivalence; verify_deliveries
  [ "$STATE" = success ] || fail "[$MODE] run state=$STATE"
  say "[$MODE] OK: $DAG_ID/$RUN_ID green — ${#ISSUES[@]} issues x 2 targets, $ANSWERED gates \
answered through swf, $EXPECTED_JOBS deliveries independently verified"
}

# ---------------------------------------------------------------- many sessions, one repository
#
# Every leg above is ONE factory instance. This one is two, and it is the only leg that can fail the
# way a real deployment fails: several harness sessions -- each in its own Claude Code / Codex / pi
# loop, each with its own state root -- working one repository at the same time.
#
# What it must prove is not that both sessions succeed. It is that the SECOND one does not quietly
# undo the first: one branch, one pull request, and no commit replaced behind a reviewer's back.
two_sessions() {
  say "############ leg: two sessions, one repository"
  local arena="$WORK/two-sessions" remote="$WORK/two-sessions/origin.git"
  local key branch a_head b_rc
  rm -rf "$arena"; mkdir -p "$arena"

  git init -q --bare -b main "$remote"
  git clone -q "$remote" "$arena/seed"
  printf 'seed\n' >"$arena/seed/README.md"
  git -C "$arena/seed" add -A
  git -C "$arena/seed" -c user.name=e2e -c user.email=e2e@example.com commit -qm seed
  git -C "$arena/seed" push -q origin main

  # Both sessions resolve the SAME publication key from (repo, target, issue) without talking to
  # each other -- that is the whole mechanism, so read it from the runtime rather than restating it.
  key="$("$PY" -c 'from swfactory.publication_identity import publication_key
print(publication_key("o/r", "demo/target", "DEMO-1"))')"
  branch="factory/DEMO-1-$key"

  "$PY" - "$remote" "$arena" "$branch" <<'PYEOF' >"$arena/session-a.log" 2>&1
import sys
from pathlib import Path
from swfactory.publication_identity import PublicationIdentity, publication_key
from swfactory.scm import LocalGitScm

remote, arena, branch = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
patch = (
    "From 0000000000000000000000000000000000000000 Mon Sep 17 00:00:00 2001\n"
    "From: swfactory-bot <bot@example.com>\nDate: Mon, 1 Jan 2026 00:00:00 +0000\n"
    "Subject: [PATCH] session A\n\n---\n demo/target/a.txt | 1 +\n 1 file changed, 1 insertion(+)\n\n"
    "diff --git a/demo/target/a.txt b/demo/target/a.txt\nnew file mode 100644\n"
    "index 0000000..0000001\n--- /dev/null\n+++ b/demo/target/a.txt\n@@ -0,0 +1 @@\n+A\n-- \n2.39.0\n"
).encode()
LocalGitScm(remote, arena / "run-a").publish(
    branch=branch, patch=patch, title="A", body="A", labels=["factory"],
    identity=PublicationIdentity(key=publication_key("o/r", "demo/target", "DEMO-1"), instance="session-a"),
)
PYEOF
  a_head="$(git -C "$remote" rev-parse "refs/heads/$branch")"
  printf 'session A published %s at %s\n' "$branch" "${a_head:0:12}"

  # Session B: a different instance, same work, no knowledge of A. It must be refused rather than
  # replace A's commit -- and `|| true` because the refusal IS the pass condition here.
  b_rc=0
  "$PY" - "$remote" "$arena" "$branch" <<'PYEOF' >"$arena/session-b.log" 2>&1 || b_rc=$?
import sys
from pathlib import Path
from swfactory.publication_identity import PublicationIdentity, publication_key
from swfactory.scm import LocalGitScm

remote, arena, branch = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
patch = (
    "From 0000000000000000000000000000000000000000 Mon Sep 17 00:00:00 2001\n"
    "From: swfactory-bot <bot@example.com>\nDate: Mon, 1 Jan 2026 00:00:00 +0000\n"
    "Subject: [PATCH] session B\n\n---\n demo/target/b.txt | 1 +\n 1 file changed, 1 insertion(+)\n\n"
    "diff --git a/demo/target/b.txt b/demo/target/b.txt\nnew file mode 100644\n"
    "index 0000000..0000001\n--- /dev/null\n+++ b/demo/target/b.txt\n@@ -0,0 +1 @@\n+B\n-- \n2.39.0\n"
).encode()
LocalGitScm(remote, arena / "run-b").publish(
    branch=branch, patch=patch, title="B", body="B", labels=["factory"],
    identity=PublicationIdentity(key=publication_key("o/r", "demo/target", "DEMO-1"), instance="session-b"),
)
PYEOF
  [ "$b_rc" -ne 0 ] || fail "[two-sessions] session B published over session A instead of being refused"
  grep -q "did not publish" "$arena/session-b.log" \
    || fail "[two-sessions] session B failed, but not with the refusal that names the other writer"

  # The three things a reviewer actually cares about.
  [ "$(git -C "$remote" rev-parse "refs/heads/$branch")" = "$a_head" ] \
    || fail "[two-sessions] session A's commit was replaced"
  [ "$(git -C "$remote" for-each-ref --format='%(refname)' 'refs/heads/factory/*' | wc -l | tr -d ' ')" = 1 ] \
    || fail "[two-sessions] two sessions produced two factory branches"
  git -C "$remote" cat-file -e "$a_head:demo/target/a.txt" \
    || fail "[two-sessions] session A's file is gone from the published tree"

  say "[two-sessions] OK: one branch, A's commit intact at ${a_head:0:12}, B refused naming the holder"
}

boot_airflow
mint_admin
start_backend
for mode in $LEGS; do leg "$mode"; done
case " $LEGS " in *" two-sessions "*) : ;; *) two_sessions ;; esac
say "OK: the same factory, proved through ${LEGS// /, }, and two sessions on one repository"
