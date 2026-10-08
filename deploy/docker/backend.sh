#!/usr/bin/env bash
# Factory backend entrypoint of deploy/docker/compose.yml: resolves the
# backend's Airflow login (AIRFLOW_TOKEN wins; else AIRFLOW_USER + AIRFLOW_PASSWORD; else the
# generated admin password from the shared airflow-home volume, read here, exported, never
# printed) and execs `swfactory backend`.
set -euo pipefail

cd "${SWF_REPO_DIR:-$PWD}"
# shellcheck source=SCRIPTDIR/../../scripts/lib/live_airflow.sh
. scripts/lib/live_airflow.sh
export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow_home}"
AIRFLOW_URL="${AIRFLOW_URL:-http://airflow:8080}"

# Existing credentials are enough to accept work while the API is unavailable. On the very
# first boot, wait only for Airflow to generate the password; dispatch retries handle startup.
if [ -z "${AIRFLOW_TOKEN:-}" ] && [ -z "${AIRFLOW_PASSWORD:-}" ]; then
  wait_for_airflow_password
fi
export AIRFLOW_URL
uv sync --group airflow --frozen
exec uv run --group airflow swfactory backend --host 0.0.0.0 --port 8082
