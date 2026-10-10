#!/usr/bin/env bash
# Validate the deployment inputs that Compose interpolation cannot express. No model calls,
# image builds or pulls happen here; the requested Compose operation owns those effects.
set -euo pipefail

fail() { printf 'external-agent deployment: %s\n' "$1" >&2; exit 2; }

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/../.." && pwd -P)"
cd -- "$repo_root"

[ "$#" -gt 0 ] || fail 'pass a Compose operation, such as config --quiet or up -d'
[[ "${SWF_EXTERNAL_PROFILE_DIR:-}" == /* ]] || fail 'SWF_EXTERNAL_PROFILE_DIR must be an absolute directory'
[ -d "$SWF_EXTERNAL_PROFILE_DIR" ] || fail 'SWF_EXTERNAL_PROFILE_DIR must already exist'
profile_dir="$(cd -- "$SWF_EXTERNAL_PROFILE_DIR" && pwd -P)"
[[ "$profile_dir" != / ]] || fail 'use a dedicated operator profile directory, not /'
[[ "$profile_dir" != "$repo_root/.factory" && "$profile_dir" != "$repo_root/.factory/"* ]] \
  || fail 'the operator profile directory must be outside candidate workdirs under .factory'
[ -O "$profile_dir" ] || fail 'the operator must own the profile directory'
[ -f "$profile_dir/profile.json" ] && [ -r "$profile_dir/profile.json" ] \
  || fail 'the operator directory must contain readable profile.json'
[ -O "$profile_dir/profile.json" ] && [ ! -L "$profile_dir/profile.json" ] \
  || fail 'profile.json must be an operator-owned regular file, not a symlink'
[[ "${SWF_EXTERNAL_IMAGE:-}" =~ ^[^[:space:]]+@sha256:[0-9a-f]{64}$ ]] \
  || fail 'SWF_EXTERNAL_IMAGE must use image@sha256:<64 lowercase hex characters>'

export SWF_EXTERNAL_PROFILE_DIR="$profile_dir"
exec docker compose -f "$script_dir/compose.yml" -f "$script_dir/compose.external-agent.yml" "$@"
