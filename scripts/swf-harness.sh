#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: scripts/swf-harness.sh [--harness NAME] [--factory-id ID] --issue REF [swf submit args...]

Starts governed factory work from an outer AI coding harness. The harness/session identity is
forwarded through SWF_HARNESS + SWF_FACTORY_ID; Apache Airflow remains the lifecycle scheduler.

Examples:
  scripts/swf-harness.sh --harness codex --factory-id codex-a17 --issue 1201
  scripts/swf-harness.sh --harness claude --factory-id claude-b9 --issue 1202 --issue 1203
  SWF_HARNESS=grok SWF_FACTORY_ID=grok-c4 scripts/swf-harness.sh --issue 1204
EOF
  exit 2
}

harness="${SWF_HARNESS:-}"
factory_id="${SWF_FACTORY_ID:-}"
args=()
while (($#)); do
  case "$1" in
    --harness)
      (($# >= 2)) || usage
      harness="$2"
      shift 2
      ;;
    --factory-id)
      (($# >= 2)) || usage
      factory_id="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

[[ ${#args[@]} -gt 0 ]] || usage

# Explicit is best. Parent-process detection is only a convenience for interactive harnesses and
# never changes factory authority or policy.
if [[ -z "$harness" ]]; then
  parent="$(ps -o comm= -p "$PPID" 2>/dev/null | tr '[:upper:]' '[:lower:]' | xargs || true)"
  case "$parent" in
    *codex*) harness=codex ;;
    *claude*) harness=claude ;;
    *grok*) harness=grok ;;
    *) harness=custom ;;
  esac
fi

# One outer harness process gets one stable default factory id. Callers that need replay across a
# harness restart should set SWF_FACTORY_ID explicitly and reuse it.
if [[ -z "$factory_id" ]]; then
  seed="${SWF_HARNESS_SESSION_SEED:-$harness:$PPID:$PWD}"
  if command -v sha256sum >/dev/null 2>&1; then
    short="$(printf '%s' "$seed" | sha256sum | cut -c1-12)"
  else
    short="$(printf '%s' "$seed" | shasum -a 256 | cut -c1-12)"
  fi
  factory_id="${harness}-${short}"
fi

export SWF_HARNESS="$harness"
export SWF_FACTORY_ID="$factory_id"

swf_bin="${SWF_BIN:-swf}"
printf 'swfactory harness=%s factory_id=%s\n' "$SWF_HARNESS" "$SWF_FACTORY_ID" >&2
exec "$swf_bin" submit "${args[@]}"
