#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: scripts/swf_harness.sh HARNESS FACTORY_ID [swf submit args...]

Examples:
  scripts/swf_harness.sh codex codex-session-17 --issue 1204
  scripts/swf_harness.sh claude claude-worktree-a --issue 1205 --target owner/repo

Set SWF_BIN to use a non-default swf executable.
EOF
  exit 2
}

[[ $# -ge 2 ]] || usage
harness=$1
factory_id=$2
shift 2

valid_component='^[A-Za-z0-9._-]+$'
[[ ${#harness} -ge 1 && ${#harness} -le 48 && $harness =~ $valid_component ]] || {
  echo "invalid harness: use 1-48 ASCII letters, digits, dot, underscore or hyphen" >&2
  exit 2
}
[[ ${#factory_id} -ge 1 && ${#factory_id} -le 64 && $factory_id =~ $valid_component ]] || {
  echo "invalid factory id: use 1-64 ASCII letters, digits, dot, underscore or hyphen" >&2
  exit 2
}

swf_bin=${SWF_BIN:-swf}
exec "$swf_bin" submit --harness "$harness" --factory-id "$factory_id" "$@"
