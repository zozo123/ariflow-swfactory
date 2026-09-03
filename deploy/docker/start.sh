#!/usr/bin/env bash
# Airflow entrypoint of deploy/docker/compose.yml (mirrors deploy/islo/orchestrator/start.sh):
#   uv sync --group airflow -> airflow db migrate -> airflow standalone (foreground, API+UI :8080).
# The receiver runs in its own service (webhook.sh). Simple auth manager generates the admin
# password on first start into $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated.
# Nothing here prints a password or a token.
set -euo pipefail

: "${SWF_REPO_DIR:=$PWD}"
[ -f "$SWF_REPO_DIR/dags/blueprints.py" ] || {
  echo "start.sh: $SWF_REPO_DIR has no dags/blueprints.py (is the repo mounted at its host path?)" >&2
  exit 1
}
cd "$SWF_REPO_DIR"

export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow_home}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-$SWF_REPO_DIR/dags}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT="${AIRFLOW__API__PORT:-8080}"
export AIRFLOW__API__HOST="${AIRFLOW__API__HOST:-0.0.0.0}"

# factory knobs read by the DAG tasks (SWF_* env > blueprint); compose sets the defaults
export SWF_SANDBOX="${SWF_SANDBOX:-docker}"
export SWF_AGENT="${SWF_AGENT:-scripted}"
export SWF_SCM="${SWF_SCM:-local}"
export SWF_APPROVE="${SWF_APPROVE:-prompt}"
export SWF_DOCKER_IMAGE="${SWF_DOCKER_IMAGE:-swfactory-sandbox:local}"

if [ ! -S /var/run/docker.sock ]; then
  echo "start.sh: warning: /var/run/docker.sock is not mounted; SWF_SANDBOX=docker cannot start sandboxes" >&2
fi
if [ "$SWF_AGENT" = "claude" ] && [ -z "${ANTHROPIC_API_KEY:-}" ] && [ "${SWF_DOCKER_CREDENTIALS:-env}" = "env" ]; then
  echo "start.sh: warning: SWF_AGENT=claude but ANTHROPIC_API_KEY is empty (agent calls will fail)" >&2
fi

mkdir -p "$AIRFLOW_HOME"
uv sync --group airflow --frozen
uv run --group airflow airflow db migrate
exec uv run --group airflow airflow standalone
