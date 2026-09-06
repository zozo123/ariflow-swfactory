#!/usr/bin/env bash
# Install a coherent snapshot of upstream main, including its sandbox provider.
# Use --islo for the experimental provider fork, --pypi for the released provider.
# AIRFLOW_REF may be main, a branch/tag, or an exact commit for reproduction.
set -euo pipefail
REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
MODE="${1:---upstream}"
case "$MODE" in
  --upstream|--islo|--pypi) ;;
  *) echo "usage: $0 [--upstream|--islo|--pypi]" >&2; exit 2 ;;
esac
[ "$#" -le 1 ] || { echo "expected at most one option" >&2; exit 2; }
AIRFLOW_REF="${AIRFLOW_REF:-main}"
UPSTREAM=https://github.com/apache/airflow.git
resolve_commit() {
  local repo="$1" ref="$2" commit
  if [[ "$ref" =~ ^[0-9a-f]{40}$ ]]; then
    commit="$ref"
  else
    commit="$(git ls-remote "$repo" "$ref" "refs/heads/$ref" \
      "refs/tags/$ref" "refs/tags/$ref^{}" | sort -k2 | tail -1 | cut -f1)"
  fi
  [[ "$commit" =~ ^[0-9a-f]{40}$ ]] || {
    echo "cannot resolve $repo ref: $ref" >&2; return 1;
  }
  printf '%s\n' "$commit"
}
AIRFLOW_COMMIT="$(resolve_commit "$UPSTREAM" "$AIRFLOW_REF")"
export AIRFLOW_COMMIT
APACHE="git+$UPSTREAM@$AIRFLOW_COMMIT"
echo "Installing apache/airflow@$AIRFLOW_COMMIT ($MODE)"
# Bootstrap a fresh checkout; subsequent commands must use --no-sync to retain this overlay.
uv sync
uv pip install \
  "apache-airflow @ $APACHE" \
  "apache-airflow-core @ $APACHE#subdirectory=airflow-core" \
  "apache-airflow-task-sdk @ $APACHE#subdirectory=task-sdk" \
  "apache-airflow-providers-standard @ $APACHE#subdirectory=providers/standard" \
  "apache-airflow-providers-common-ai @ $APACHE#subdirectory=providers/common/ai"
if [ "$MODE" = --islo ]; then
  AI_PROVIDER_REPO="${AI_PROVIDER_REPO:-https://github.com/zozo123/airflow.git}"
  AI_PROVIDER_REF="${AI_PROVIDER_REF:-agent/add-islo-sandbox-backend}"
  AI_PROVIDER_COMMIT="$(resolve_commit "$AI_PROVIDER_REPO" "$AI_PROVIDER_REF")"
  export AI_PROVIDER_REPO AI_PROVIDER_COMMIT
  # The fork declares conflicting core URLs. Dependencies were installed from upstream above.
  uv pip install --no-deps \
    "apache-airflow-providers-common-ai @ git+$AI_PROVIDER_REPO@$AI_PROVIDER_COMMIT#subdirectory=providers/common/ai"
elif [ "$MODE" = --pypi ]; then
  uv pip install --reinstall-package apache-airflow-providers-common-ai \
    apache-airflow-providers-common-ai
fi
uv pip check
uv run --no-sync python scripts/verify_airflow_main.py "$MODE"
scripts/build_airflow_ui.sh
echo "ready: uv run --no-sync swfactory run --issue <n> --sandbox toolset"
echo "live E2E: SWF_AIRFLOW_NO_SYNC=1 scripts/stress_airflow.sh"
echo "release: uv sync --group airflow"
