#!/usr/bin/env bash
# Orchestrator entrypoint, run INSIDE the swf-orchestrator islo sandbox (see deploy.sh):
#   airflow db migrate -> airflow standalone (background, API+UI on :8080) -> wait for
#   credentials ready -> swfactory webhook serve --port 8081 (foreground).
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
export SWF_WEBHOOK_INBOX="${SWF_WEBHOOK_INBOX:-$AIRFLOW_HOME/webhooks/inbox.sqlite3}"
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

# --- webhook receiver (foreground) -----------------------------------------------------------
# LEGACY unmanaged intake (#2068): this sandbox runs no factory backend, so the receiver dispatches
# straight to Airflow and those runs carry no `_factory_cells` -- no admission record, no capacity
# accounting, no Factory Cell fencing. Run `swfactory backend` here and pass --backend-url instead
# to get the managed boundary the Docker stack and dispatch.yml already use.
# Credentials for the receiver's /auth/token login: AIRFLOW_TOKEN wins; else AIRFLOW_USER +
# AIRFLOW_PASSWORD; else the generated admin password (read here, exported, never printed).
if [ -z "${AIRFLOW_TOKEN:-}" ] && [ -z "${AIRFLOW_PASSWORD:-}" ]; then
  PW_FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated}"
  for _ in $(seq 1 180); do
    if [ -s "$PW_FILE" ]; then break; fi
    if ! kill -0 "$AIRFLOW_PID" 2>/dev/null; then
      echo "start.sh: Airflow exited before generating its credential file" >&2
      exit 1
    fi
    sleep 2
  done
  if [ ! -s "$PW_FILE" ]; then
    echo "start.sh: Airflow has not generated its credential file" >&2
    exit 1
  fi
  export AIRFLOW_USER="${AIRFLOW_USER:-admin}"
  AIRFLOW_PASSWORD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$PW_FILE" "$AIRFLOW_USER")"
  export AIRFLOW_PASSWORD
fi
export AIRFLOW_URL
exec uv run --group airflow swfactory webhook serve --port "${SWF_WEBHOOK_PORT:-8081}" --airflow-url "$AIRFLOW_URL"
