#!/usr/bin/env bash
# Orchestrator entrypoint, run INSIDE the swf-orchestrator islo sandbox (see deploy.sh):
#   airflow db migrate -> airflow standalone (background, API+UI on :8080) -> wait for
#   /api/v2/monitor/health -> swfactory webhook serve --port 8081 (foreground).
# Airflow 3.3.1 simple auth manager: the admin password is generated on first start into
# $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated; the receiver logs in with it.
# Nothing here prints a password or a token.
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

# --- locate the factory checkout (the single --source clone under /workspace) --------------
if [ -z "${SWF_REPO_DIR:-}" ]; then
  for d in /workspace/*/; do
    if [ -f "$d/dags/blueprints.py" ]; then SWF_REPO_DIR="${d%/}"; break; fi
  done
fi
: "${SWF_REPO_DIR:?no factory checkout with dags/blueprints.py under /workspace}"
cd "$SWF_REPO_DIR"

# --- Airflow ---------------------------------------------------------------------------------
export AIRFLOW_HOME="${AIRFLOW_HOME:-/workspace/airflow_home}"
export AIRFLOW__CORE__DAGS_FOLDER="$SWF_REPO_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT="${AIRFLOW__API__PORT:-8080}"
export AIRFLOW__API__HOST="${AIRFLOW__API__HOST:-0.0.0.0}"
AIRFLOW_URL="http://localhost:${AIRFLOW__API__PORT}"

# --- factory knobs read by the DAG tasks (SWF_* env > blueprint) -----------------------------
export SWF_SANDBOX="${SWF_SANDBOX:-islo}"
export SWF_AGENT="${SWF_AGENT:-claude}"
export SWF_SCM="${SWF_SCM:-github}"
export SWF_APPROVE="${SWF_APPROVE:-prompt}"
export SWF_SANDBOX_OWNER="${SWF_SANDBOX_OWNER:-}"   # deploy.sh passes the islo login email
if [ -z "$SWF_SANDBOX_OWNER" ]; then
  echo "start.sh: warning: SWF_SANDBOX_OWNER is empty; the nightly sandbox sweep refuses to run" >&2
fi

mkdir -p "$AIRFLOW_HOME"
uv run --group airflow airflow db migrate

uv run --group airflow airflow standalone >"$AIRFLOW_HOME/standalone.log" 2>&1 &
AIRFLOW_PID=$!
trap 'kill "$AIRFLOW_PID" 2>/dev/null || true' EXIT INT TERM

echo "start.sh: waiting for $AIRFLOW_URL/api/v2/monitor/health ..."
for _ in $(seq 1 180); do
  if curl -fsS "$AIRFLOW_URL/api/v2/monitor/health" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$AIRFLOW_PID" 2>/dev/null; then
    echo "start.sh: airflow standalone exited; tail of $AIRFLOW_HOME/standalone.log:" >&2
    tail -n 50 "$AIRFLOW_HOME/standalone.log" >&2
    exit 1
  fi
  sleep 2
done
curl -fsS "$AIRFLOW_URL/api/v2/monitor/health" >/dev/null || {
  echo "start.sh: airflow did not become healthy" >&2
  exit 1
}
echo "start.sh: airflow healthy"

# --- webhook receiver (foreground) -----------------------------------------------------------
# Credentials for the receiver's /auth/token login: AIRFLOW_TOKEN wins; else AIRFLOW_USER +
# AIRFLOW_PASSWORD; else the generated admin password (read here, exported, never printed).
if [ -z "${AIRFLOW_TOKEN:-}" ] && [ -z "${AIRFLOW_PASSWORD:-}" ]; then
  PW_FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated}"
  export AIRFLOW_USER="${AIRFLOW_USER:-admin}"
  AIRFLOW_PASSWORD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$PW_FILE" "$AIRFLOW_USER")"
  export AIRFLOW_PASSWORD
fi
export AIRFLOW_URL
exec uv run --group airflow swfactory webhook serve --port "${SWF_WEBHOOK_PORT:-8081}" --airflow-url "$AIRFLOW_URL"
