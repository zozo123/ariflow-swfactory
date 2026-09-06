#!/usr/bin/env bash
# Git-built Airflow wheels omit compiled UI assets. Build both UIs from the installed commit.
set -euo pipefail
REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
command -v pnpm >/dev/null || { echo "Airflow UI build needs Node >=22 and pnpm 10.28.1" >&2; exit 1; }
COMMIT="$(uv run --no-sync python -c '
import importlib.metadata as m, json
print(json.loads(m.distribution("apache-airflow-core").read_text("direct_url.json"))["vcs_info"]["commit_id"])
')"
TEMP_SOURCE=""
trap '[ -z "$TEMP_SOURCE" ] || rm -rf "$TEMP_SOURCE"' EXIT
if [ -n "${AIRFLOW_UI_SOURCE:-}" ]; then
  SOURCE="$AIRFLOW_UI_SOURCE"
  [ "$(git -C "$SOURCE" rev-parse HEAD)" = "$COMMIT" ] || {
    echo "UI source must match installed Airflow commit $COMMIT" >&2; exit 1;
  }
else
  TEMP_SOURCE="$(mktemp -d "${TMPDIR:-/tmp}/swf-airflow-ui.XXXXXX")"
  SOURCE="$TEMP_SOURCE"
  git -C "$SOURCE" init -q
  git -C "$SOURCE" remote add origin https://github.com/apache/airflow.git
  git -C "$SOURCE" sparse-checkout set airflow-core
  git -C "$SOURCE" fetch --depth 1 --filter=blob:none origin "$COMMIT"
  git -C "$SOURCE" checkout --detach FETCH_HEAD
fi
DEST="$(uv run --no-sync python -c 'import airflow; from pathlib import Path; print(Path(airflow.__file__).parent)')"
for UI in api_fastapi/auth/managers/simple/ui ui; do
  (
    cd "$SOURCE/airflow-core/src/airflow/$UI"
    pnpm install --frozen-lockfile
    pnpm build
  )
  mkdir -p "$DEST/$UI"
  cp -R "$SOURCE/airflow-core/src/airflow/$UI/dist" "$DEST/$UI/"
done
echo "Airflow UI and login UI built from $COMMIT"
