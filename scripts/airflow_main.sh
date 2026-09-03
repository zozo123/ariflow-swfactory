#!/usr/bin/env bash
# Put this checkout on Airflow's development head, with Airflow's own sandbox toolset.
#
# Why a script and not a dependency group: locking a git dependency on the Airflow monorepo clones
# ~1 GB and pins a commit that is stale the next day. The default pin stays the latest release
# (see [dependency-groups].airflow); this is the opt-in bleeding edge, and it changes nothing in
# swfactory itself — `--sandbox toolset` already adapts whatever backend the provider exposes.
#
#   ./scripts/airflow_main.sh          # apache/airflow@main + common.ai from apache/airflow#71672
#   ./scripts/airflow_main.sh --pypi   # apache/airflow@main + the released common.ai (sbx only)
#   uv sync --group airflow            # back to the pinned release
set -euo pipefail

AIRFLOW_REF="${AIRFLOW_REF:-main}"                       # apache/airflow, the real head
AI_PROVIDER_REPO="${AI_PROVIDER_REPO:-https://github.com/zozo123/airflow.git}"
AI_PROVIDER_REF="${AI_PROVIDER_REF:-agent/add-islo-sandbox-backend}"  # apache/airflow#71672
APACHE="git+https://github.com/apache/airflow.git@${AIRFLOW_REF}"

# Airflow core, the task SDK and the standard provider always come from apache/airflow HEAD, so
# this really is the development head and not a fork snapshot (the islo PR branch is ~290 commits
# behind main, which an earlier version of this script silently pinned).
uv pip install \
  "apache-airflow-core @ ${APACHE}#subdirectory=airflow-core" \
  "apache-airflow-task-sdk @ ${APACHE}#subdirectory=task-sdk" \
  "apache-airflow-providers-standard @ ${APACHE}#subdirectory=providers/standard"

# The common.ai provider carries the sandbox toolset. The released wheel has `sbx` only; the PR
# branch adds `islo`. It is installed with --no-deps on purpose: the fork's pyproject pins
# apache-airflow to its own git URL, and uv refuses two different URLs for one package.
if [ "${1:-}" = "--pypi" ]; then
  uv pip install "apache-airflow-providers-common-ai"          # sbx only
else
  uv pip install --no-deps \
    "apache-airflow-providers-common-ai @ git+${AI_PROVIDER_REPO}@${AI_PROVIDER_REF}#subdirectory=providers/common/ai"
fi

uv run --no-sync python - <<'PY'
import airflow
from swfactory.sandbox import TOOLSET_BACKENDS, load_toolset_backend
print(f"airflow {airflow.__version__}")
for name in sorted(TOOLSET_BACKENDS):
    try:
        print(f"  {name:12s} {type(load_toolset_backend(name)).__name__}")
    except Exception as e:
        print(f"  {name:12s} unavailable: {str(e).split(':')[-1].strip()[:60]}")
PY
echo
echo "ready: uv run swfactory run --issue <n> --sandbox toolset   (SWF_TOOLSET_BACKEND=islo|sbx)"
