# shellcheck shell=bash
# The live-Airflow bootstrap shared by scripts/swf_e2e.sh and scripts/stress_airflow.sh: the
# project's interpreter, `airflow standalone` in a throwaway AIRFLOW_HOME on a free port, its admin
# password and its DAG parse. Each harness keeps its own scenario. The deploy entrypoints source it
# for wait_for_airflow_password alone. Sourcing defines functions and does nothing else.
#
# The harness sets REPO, WORK, AIRFLOW_HOME, STANDALONE_LOG, HEALTH_TIMEOUT_S, PARSE_TIMEOUT_S and
# DAG_ID, and BASE before export_airflow_env. resolve_py sets PY and BIN, boot_airflow sets
# STANDALONE_PID, admin_password sets PASSWORD.

say() { printf '\n=== %s\n' "$*"; }
fail() { echo "FAILED: $*" >&2; exit 1; }

# One field of the JSON object on stdin (stdlib only: jq is not assumed to be installed).
field() { "$PY" -c "import json,sys;print(json.load(sys.stdin).get('$1',''))"; }
free_port() { "$PY" -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

# One `uv run` to materialise/locate the venv, then the venv's own binaries: nothing else in the
# harness holds uv's lock, so a long `airflow standalone` cannot block another `uv run` (or be
# blocked by one).
resolve_py() {
  if [ "${SWF_AIRFLOW_NO_SYNC:-}" = "1" ]; then
    PY="$(uv run --no-sync --project "$REPO" python -c 'import sys; print(sys.executable)')"
  else
    PY="$(uv run --project "$REPO" --group airflow python -c 'import sys; print(sys.executable)')"
  fi
  "$PY" -c 'import airflow; print("Live E2E Airflow:", airflow.__version__)'
  BIN="$(dirname "$PY")"
  [ -x "$BIN/airflow" ] || fail "no airflow in $BIN — run: uv sync --group airflow"
  # `airflow standalone` starts its scheduler / api-server / dag-processor / triggerer by running
  # `airflow <subcommand>` off PATH, so the venv has to be on it and not just addressed by path.
  export PATH="$BIN:$PATH"
}

# blueprints/stress.toml's second [[targets]].dir. Materialised rather than committed: the recorded
# patches carry blob hashes, so a "second" target has to BE that copy.
materialize_target_b() {
  mkdir -p "$WORK/demo"
  cp -R "$REPO/demo/target" "$WORK/demo/target-b"
  find "$WORK/demo/target-b" \( -name __pycache__ -o -name .pytest_cache -o -name .venv \) -prune \
    -exec rm -rf {} + 2>/dev/null || true
}

# No keys and no network: scripted agent, local sandbox, local git remote.
export_airflow_env() {
  export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
  export AIRFLOW__CORE__LOAD_EXAMPLES=False
  export AIRFLOW__API__PORT="${BASE##*:}"
  # The execution API url defaults to `{api.base_url}/execution/`, so a non-default port needs both
  # or the task workers dial 8080 and every task hangs.
  export AIRFLOW__API__BASE_URL="$BASE"
  export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="$BASE/execution/"
  export SWF_AGENT=scripted SWF_SANDBOX=local SWF_SCM=local
}

# Each NAME=value reaches the standalone's environment and not this shell's.
boot_airflow() { # boot_airflow [NAME=value ...]
  say "airflow standalone on $BASE (log: $STANDALONE_LOG)"
  set -m   # own process group, so stop_group can signal airflow's children too
  env "$@" "$BIN/airflow" standalone >"$STANDALONE_LOG" 2>&1 &
  STANDALONE_PID=$!
  set +m
  say "waiting for $BASE/api/v2/monitor/health"
  local i=0 health
  while :; do
    kill -0 "$STANDALONE_PID" 2>/dev/null || fail "standalone died during startup"
    health="$(curl -fsS "$BASE/api/v2/monitor/health" 2>/dev/null || true)"
    if [ -n "$health" ] && printf '%s' "$health" | "$PY" -c '
import json
import sys

data = json.load(sys.stdin)
parts = ("metadatabase", "scheduler", "dag_processor", "triggerer")
sys.exit(0 if all(data.get(p, {}).get("status") == "healthy" for p in parts) else 1)
'; then echo "healthy after ${i}s: $health"; break; fi
    [ $i -lt "$HEALTH_TIMEOUT_S" ] ||
      fail "health never went green in ${HEALTH_TIMEOUT_S}s: ${health:-<no response>}"
    sleep 1; i=$((i + 1))
  done
}

# SIGINT to the whole GROUP, then a bounded wait, then SIGKILL. The group, because `airflow
# standalone` stops its scheduler / api-server / dag-processor / triggerer children on
# KeyboardInterrupt and they are its subprocesses. The bound, because a background job started with
# job control OFF inherits SIGINT ignored: `swfactory backend` outlived every INT this trap sent and
# the bare `wait` after it blocked forever — a hung harness behind a green factory, which reads
# exactly like a hung factory. Hence `set -m` on every start, and no unbounded wait.
stop_group() { # stop_group <pid> <what>
  local pid=${1:-} what=$2 i=0
  [ -n "$pid" ] || return 0
  say "shutting down $what (process group $pid)"
  kill -INT -- "-$pid" 2>/dev/null || true
  while kill -0 "$pid" 2>/dev/null && [ $i -lt 60 ]; do sleep 0.5; i=$((i + 1)); done
  kill -KILL -- "-$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# `airflow standalone` writes the admin password on first boot. Gates are answered as that user, so
# the factory records a real HITL respondent instead of "auto".
admin_password() {
  local file="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
  [ -f "$file" ] || fail "no $file — is [core] simple_auth_manager_all_admins on?"
  # shellcheck disable=SC2034 # the harness reads it
  PASSWORD="$("$PY" -c 'import json,sys
u = json.load(open(sys.argv[1])); print(u.get("admin") or next(iter(u.values())))' "$file")"
}

# Until `<probe ...>` succeeds, which it does once the dag-processor has parsed $DAG_ID. New DAGs
# start paused in standalone, so the caller unpauses next, or the run sits queued forever.
wait_dag_parsed() { # wait_dag_parsed <probe ...>
  local i=0
  until "$@" >/dev/null 2>&1; do
    [ $i -lt "$PARSE_TIMEOUT_S" ] || fail "$DAG_ID never appeared in ${PARSE_TIMEOUT_S}s"
    sleep 1; i=$((i + 1))
  done
  echo "parsed after ${i}s"
}

# The deploy entrypoints' Airflow login when none is configured: wait for the generated admin
# password, then export AIRFLOW_USER and AIRFLOW_PASSWORD (read here, never printed). Given the
# standalone's <pid>, a standalone that dies first fails at once instead of after six minutes.
wait_for_airflow_password() { # wait_for_airflow_password [pid]
  local pid=${1:-}
  local file="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated}"
  for _ in $(seq 1 180); do
    if [ -s "$file" ]; then break; fi
    if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
      echo "${0##*/}: Airflow exited before generating its credential file" >&2
      exit 1
    fi
    sleep 2
  done
  if [ ! -s "$file" ]; then
    echo "${0##*/}: Airflow has not generated its credential file" >&2
    exit 1
  fi
  export AIRFLOW_USER="${AIRFLOW_USER:-admin}"
  AIRFLOW_PASSWORD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$file" "$AIRFLOW_USER")"
  export AIRFLOW_PASSWORD
}
