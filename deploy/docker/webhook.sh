#!/usr/bin/env bash
# Webhook receiver entrypoint of deploy/docker/compose.yml: resolves the
# receiver's Airflow login (AIRFLOW_TOKEN wins; else AIRFLOW_USER + AIRFLOW_PASSWORD; else the
# generated admin password from the shared airflow-home volume, read here, exported, never
# printed) and execs `swfactory webhook serve`.
set -euo pipefail

cd "${SWF_REPO_DIR:-$PWD}"
export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow_home}"
export SWF_WEBHOOK_INBOX="${SWF_WEBHOOK_INBOX:-$AIRFLOW_HOME/webhooks/inbox.sqlite3}"
AIRFLOW_URL="${AIRFLOW_URL:-http://airflow:8080}"

# Existing credentials are enough to accept work while the API is unavailable. On the very
# first boot, wait only for Airflow to generate the password; dispatch retries handle startup.
if [ -z "${AIRFLOW_TOKEN:-}" ] && [ -z "${AIRFLOW_PASSWORD:-}" ]; then
  PW_FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated}"
  for _ in $(seq 1 180); do
    if [ -s "$PW_FILE" ]; then break; fi
    sleep 2
  done
  if [ ! -s "$PW_FILE" ]; then
    echo "webhook.sh: Airflow has not generated its credential file" >&2
    exit 1
  fi
  export AIRFLOW_USER="${AIRFLOW_USER:-admin}"
  AIRFLOW_PASSWORD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$PW_FILE" "$AIRFLOW_USER")"
  export AIRFLOW_PASSWORD
fi
export AIRFLOW_URL
uv sync --group airflow --frozen
exec uv run --group airflow swfactory webhook serve --port "${SWF_WEBHOOK_PORT:-8081}" --airflow-url "$AIRFLOW_URL"
