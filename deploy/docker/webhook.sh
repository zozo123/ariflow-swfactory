#!/usr/bin/env bash
# Webhook receiver entrypoint of deploy/docker/compose.yml: waits for Airflow, resolves the
# receiver's Airflow login (AIRFLOW_TOKEN wins; else AIRFLOW_USER + AIRFLOW_PASSWORD; else the
# generated admin password from the shared airflow-home volume, read here, exported, never
# printed) and execs `swfactory webhook serve`.
set -euo pipefail

cd "${SWF_REPO_DIR:-$PWD}"
export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow_home}"
AIRFLOW_URL="${AIRFLOW_URL:-http://airflow:8080}"

echo "webhook.sh: waiting for $AIRFLOW_URL/api/v2/monitor/health ..."
for _ in $(seq 1 180); do
  if curl -fsS "$AIRFLOW_URL/api/v2/monitor/health" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$AIRFLOW_URL/api/v2/monitor/health" >/dev/null || {
  echo "webhook.sh: airflow did not become healthy" >&2
  exit 1
}

if [ -z "${AIRFLOW_TOKEN:-}" ] && [ -z "${AIRFLOW_PASSWORD:-}" ]; then
  PW_FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated}"
  export AIRFLOW_USER="${AIRFLOW_USER:-admin}"
  AIRFLOW_PASSWORD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$PW_FILE" "$AIRFLOW_USER")"
  export AIRFLOW_PASSWORD
fi
export AIRFLOW_URL
uv sync --group airflow --frozen
exec uv run --group airflow swfactory webhook serve --port "${SWF_WEBHOOK_PORT:-8081}" --airflow-url "$AIRFLOW_URL"
