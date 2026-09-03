#!/usr/bin/env bash
# Put this checkout on Airflow's development head, with Airflow's own sandbox toolset.
#
# Why a script and not a dependency group: locking a git dependency on the Airflow monorepo clones
# ~1 GB and pins a commit that is stale the next day. The default pin stays the latest release
# (see [dependency-groups].airflow); this is the opt-in bleeding edge, and it changes nothing in
# swfactory itself — `--sandbox toolset` already adapts whatever backend the provider exposes.
#
#   ./scripts/airflow_main.sh            # airflow main + common.ai from the islo backend PR
#   ./scripts/airflow_main.sh --pypi     # airflow main + the released common.ai (sbx only)
#   uv sync --group airflow              # back to the pinned release
set -euo pipefail

AIRFLOW_REF="${AIRFLOW_REF:-main}"
AI_PROVIDER_REPO="${AI_PROVIDER_REPO:-https://github.com/zozo123/airflow.git}"
AI_PROVIDER_REF="${AI_PROVIDER_REF:-agent/add-islo-sandbox-backend}"  # apache/airflow#71672
# uv rejects two different git URLs for the same package, so every Airflow distribution comes
# from ONE repo. The PR branch is apache/airflow@main plus the islo backend, so it serves both.
if [ "${1:-}" = "--pypi" ]; then
  SRC="git+https://github.com/apache/airflow.git@${AIRFLOW_REF}"
  EXTRA=("apache-airflow-providers-common-ai")            # released provider: sbx only
else
  SRC="git+${AI_PROVIDER_REPO}@${AI_PROVIDER_REF}"
  EXTRA=("apache-airflow-providers-common-ai @ ${SRC}#subdirectory=providers/common/ai")
fi

uv sync --group airflow                       # baseline, then overlay the dev head
pkgs=(
  "apache-airflow-core @ ${SRC}#subdirectory=airflow-core"
  "apache-airflow-task-sdk @ ${SRC}#subdirectory=task-sdk"
  "apache-airflow-providers-standard @ ${SRC}#subdirectory=providers/standard"
  "${EXTRA[@]}"
)
uv pip install "${pkgs[@]}"

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
