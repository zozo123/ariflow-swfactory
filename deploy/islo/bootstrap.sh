#!/usr/bin/env bash
# One-time (idempotent) bootstrap of the islo side of the factory; see docs/islo.md.
#
#   islo login (+ github, claude integrations) -> gateway profile (deny-by-default + allow rules)
#   -> environment carrying the Anthropic key as a gateway secret -> optional golden snapshot
#   -> `swfactory doctor` -> publish the factory knowledge bundle.
#
# Every step is skipped when its object already exists, so re-running is safe. Secrets are never
# echoed; the only place the key appears is the argv of `islo environment create` (the CLI has no
# stdin/file form for --gateway-secret), which is visible to `ps` on this host for that instant.
#
# Flags verified against islo 0.48.1 (`islo <cmd> --help`, `islo schema <cmd>`) and gh 2.83.
# Usage: [REPO=owner/name] [TARGET_DIR=demo/target] [PROFILE=swfactory] [ENV=swfactory]
#        [SNAPSHOT=1] [ANTHROPIC_API_KEY=...] deploy/islo/bootstrap.sh
set -euo pipefail

REPO="${REPO:-zozo123/ariflow-swfactory}"        # target repo (owner/name); cloned by --source
TARGET_DIR="${TARGET_DIR:-demo/target}"          # subdir the factory operates on ("" = repo root)
PROFILE="${PROFILE:-swfactory}"                  # islo gateway profile ([sandbox] gateway_profile)
ENV="${ENV:-swfactory}"                          # islo environment ([sandbox] environment)
SNAPSHOT="${SNAPSHOT:-0}"                        # 1 = bake swf-golden-<date> (docs/islo.md)
BRANCH="${BRANCH:-main}"
ALLOW_HOSTS=(api.anthropic.com github.com api.github.com pypi.org files.pythonhosted.org astral.sh)

FACTORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$FACTORY_ROOT"   # `islo use` picks up ./islo.yaml from here

log()  { printf '==> %s\n' "$*"; }
note() { printf 'NOTE: %s\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing on PATH: $1" >&2; exit 1; }; }
# Names ("name" field) of a JSON listing on stdin (a bare list or an object holding one).
json_names() {
  python3 -c '
import json, sys
raw = sys.stdin.read().strip()
data = json.loads(raw) if raw else []
if isinstance(data, dict):
    data = next((v for v in data.values() if isinstance(v, list)), [])
for x in data:
    if isinstance(x, dict) and isinstance(x.get("name"), str):
        print(x["name"])
'
}
has_name() { json_names | grep -qx -- "$1"; }

need islo; need gh; need uv; need python3

# ---------------------------------------------------------------- 1. islo login
if islo status --output json | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("auth",{}).get("authenticated") else 1)'; then
  log "islo: already authenticated"
else
  log "islo login (opens a browser)"
  islo login
fi

# ---------------------------------------------------------------- 2. integrations
# `islo status --output json` carries an "integrations" list (strings or objects with a
# tool/name/provider field); when absent, the text view's "Connected Integrations" section is
# parsed (its "No integrations connected (run 'islo login --tool github')" hint is NOT a match).
# `claude` also accepts an `anthropic` integration, mirroring swfactory.doctor.
has_integration() {
  islo status --output json | python3 -c '
import json, sys
want = {sys.argv[1]} | ({"anthropic"} if sys.argv[1] == "claude" else set())
data = json.load(sys.stdin)
names = set()
if "integrations" in data:
    raw = data["integrations"]
    raw = [{"name": k, **(v if isinstance(v, dict) else {})} for k, v in raw.items()] if isinstance(raw, dict) else raw
    for x in raw or []:
        if isinstance(x, str):
            names.add(x.lower())
        elif isinstance(x, dict) and str(x.get("status") or x.get("state") or "").lower() not in {"disconnected", "expired", "error", "revoked", "pending"} and x.get("connected") is not False:
            names |= {str(x[k]).lower() for k in ("tool", "name", "provider", "type", "slug", "id") if isinstance(x.get(k), str)}
else:
    sys.exit(2)
sys.exit(0 if names & want else 1)
' "$1"
  rc=$?
  [[ $rc -ne 2 ]] && return $rc
  local pat="$1"; [[ "$1" == "claude" ]] && pat="(claude|anthropic)"
  islo status | sed -n '/Connected Integrations/,/^$/p' | tail -n +2 \
    | grep -viE '^\s*(no integrations|integrations power)' \
    | grep -qiE -- "^[-* ]*${pat}\b"
}
for tool in github claude; do
  if has_integration "$tool"; then
    log "integration $tool: connected"
  else
    log "islo login --tool $tool"
    islo login --tool "$tool"
  fi
done

# ---------------------------------------------------------------- 3. gateway profile (deny-by-default)
if islo gateway ls --output json | has_name "$PROFILE"; then
  log "gateway profile $PROFILE: exists"
else
  log "islo gateway create --name $PROFILE --default-action deny --internet-access true"
  islo gateway create --name "$PROFILE" --default-action deny --internet-access true
fi
# Allow rules: `islo gateway <profile> add-rule --host <h> --action allow` (verified). Rules already
# on the profile (by host) are not re-added; `rule ls` output shape is unverified live, so a
# host that cannot be found in the listing is (re)added — harmless duplicates at worst.
existing_rules="$(islo gateway "$PROFILE" rule ls --output json 2>/dev/null || true)"
for host in "${ALLOW_HOSTS[@]}"; do
  if printf '%s' "$existing_rules" | grep -qF -- "\"$host\""; then
    log "gateway rule allow $host: exists"
  else
    log "islo gateway $PROFILE add-rule --host $host --action allow"
    islo gateway "$PROFILE" add-rule --host "$host" --action allow
  fi
done
note "review the profile in the console: default_action must stay 'deny'; allow-list = ${ALLOW_HOSTS[*]}"

# ---------------------------------------------------------------- 4. environment (phantom Anthropic key)
if islo environment list --output json | has_name "$ENV"; then
  log "environment $ENV: exists (key not re-read)"
else
  if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
    echo "ANTHROPIC_API_KEY must be set in this shell to create environment $ENV" >&2
    exit 1
  fi
  log "islo environment create --name $ENV --gateway-secret 'ANTHROPIC_API_KEY=<redacted>;host=api.anthropic.com;auth=bearer'"
  islo environment create --name "$ENV" \
    --gateway-secret "ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY};host=api.anthropic.com;auth=bearer"
fi

# ---------------------------------------------------------------- 5. optional golden snapshot
if [[ "$SNAPSHOT" == "1" ]]; then
  snap="swf-golden-$(date +%Y%m%d)"
  if islo snapshot ls --output json | has_name "$snap"; then
    log "snapshot $snap: exists"
  else
    repo_name="${REPO##*/}"
    workdir="/workspace/${repo_name}/${TARGET_DIR}"; workdir="${workdir%/}"
    log "baking $snap from github://${REPO}:${BRANCH} (${workdir})"
    islo use swf-golden --source "github://${REPO}:${BRANCH}" --gateway-profile "$PROFILE" \
      --environment "$ENV" --init minimal --output plain -- \
      bash -lc "cd '${workdir}' && uv sync --group dev && claude --version"
    islo snapshot save swf-golden --name "$snap" --output plain
    islo rm swf-golden --force --output plain
  fi
  note "warm start: export SWF_ISLO_SNAPSHOT=$snap  (or set [sandbox] snapshot in the blueprint)"
fi

# ---------------------------------------------------------------- 6. verify
log "uv run swfactory doctor"
uv run swfactory doctor
log "deploy/islo/knowledge.sh $REPO"
"$FACTORY_ROOT/deploy/islo/knowledge.sh" "$REPO"
