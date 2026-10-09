#!/usr/bin/env bash
# Webhook receiver entrypoint of deploy/docker/compose.yml: execs `swfactory webhook serve` against
# the backend's managed work-order boundary. It deliberately carries no Airflow credential -- the
# backend owns admission, capacity, Factory Cell bindings and the Airflow write, so a receiver that
# cannot reach Airflow cannot create a run nothing admitted.
set -euo pipefail

cd "${SWF_REPO_DIR:-$PWD}"
export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow_home}"
export SWF_WEBHOOK_INBOX="${SWF_WEBHOOK_INBOX:-$AIRFLOW_HOME/webhooks/inbox.sqlite3}"
SWF_BACKEND_URL="${SWF_BACKEND_URL:-http://backend:8082}"

if [ -z "${SWF_BACKEND_TOKEN:-}" ]; then
  echo "webhook.sh: SWF_BACKEND_TOKEN is required; the receiver submits work orders, not DAG runs" >&2
  exit 1
fi

export SWF_BACKEND_URL
uv sync --group airflow --frozen
# The port mapping needs the container's own interface. compose.yml publishes it on the host's
# loopback only (127.0.0.1:8081), which is the boundary --trust-upstream relies on while
# SWF_WEBHOOK_SECRET is unset; set the secret before putting a tunnel in front of it.
exec uv run --group airflow swfactory webhook serve --port "${SWF_WEBHOOK_PORT:-8081}" \
  --host 0.0.0.0 --trust-upstream --backend-url "$SWF_BACKEND_URL"
