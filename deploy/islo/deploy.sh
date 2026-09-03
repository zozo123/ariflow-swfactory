#!/usr/bin/env bash
# Deploy the factory ORCHESTRATOR on islo and wire GitHub to it. Run from the factory repo root
# on a machine with `islo` (0.48+, logged in: `islo login && islo login --tool github &&
# islo login --tool claude`) and `gh` (authenticated for the target repo's hooks).
#
#   GitHub (issues, issue_comment) --HMAC--> islo incoming webhook (verifies X-Hub-Signature-256,
#   idempotent on X-GitHub-Delivery) --> swf-orchestrator:8081 (swfactory webhook serve)
#   --> POST /api/v2/dags/<blueprint>/dagRuns on the orchestrator's Airflow (:8080, `islo share`d)
#
# One-time prerequisites (console or CLI; not done here because they carry secrets):
#   islo gateway create --name swfactory-orchestrator --default-action deny --internet-access true
#     allow: github.com api.github.com pypi.org files.pythonhosted.org astral.sh islo.dev
#            releases.islo.dev and the islo API host (the orchestrator creates agent sandboxes)
#   islo environment create --name swfactory-orchestrator --secret ISLO_API_KEY=<islo api-key create>
#   the agent-side `swfactory` gateway profile + environment from docs/islo.md (unchanged)
#
# Env: SWF_REPO (owner/name, default zozo123/ariflow-swfactory), SWF_BRANCH (main),
#      GITHUB_WEBHOOK_SECRET (required; reuse the same secret on redeploy), SWF_SANDBOX_OWNER (defaults
#      to the islo login email if `islo status --output json` exposes one), SHARE_TTL (7d).
# Every islo/gh flag below exists in `islo <cmd> --help` (0.48.1) / `gh api --help`.
set -euo pipefail

REPO="${SWF_REPO:-zozo123/ariflow-swfactory}"
BRANCH="${SWF_BRANCH:-main}"
ORCH="${SWF_ORCHESTRATOR:-swf-orchestrator}"
GATEWAY="${SWF_ORCH_GATEWAY_PROFILE:-swfactory-orchestrator}"
ENVIRONMENT="${SWF_ORCH_ENVIRONMENT:-swfactory-orchestrator}"
WEBHOOK_NAME="${SWF_WEBHOOK_NAME:-swf-github}"
SHARE_TTL="${SHARE_TTL:-7d}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$HERE/orchestrator/islo.yaml"
REPO_NAME="${REPO##*/}"
REPO_DIR="/workspace/$REPO_NAME"

if [ -z "${GITHUB_WEBHOOK_SECRET:-}" ]; then
  echo "deploy: GITHUB_WEBHOOK_SECRET is required; generate it once (for example: openssl rand -hex 32) and reuse it on every redeploy" >&2
  exit 1
fi
export GITHUB_WEBHOOK_SECRET

json_field() {  # json_field <key> [<key2> ...]  -- first present key of the object (or list[0]) on stdin
  python3 -c '
import json, sys
doc = json.load(sys.stdin)
if isinstance(doc, list):
    doc = doc[0] if doc else {}
for k in sys.argv[1:]:
    if isinstance(doc, dict) and doc.get(k):
        print(doc[k]); break
' "$@"
}

# --- 1. the orchestrator sandbox (create-if-needed; no --pause-after-idle: it must never pause)
echo "deploy: ensuring sandbox $ORCH from github://$REPO:$BRANCH"
islo use "$ORCH" \
  --config "$CONFIG" \
  --source "github://$REPO:$BRANCH" \
  --gateway-profile "$GATEWAY" \
  --environment "$ENVIRONMENT" \
  --init full \
  --auto-resume on_activity \
  --output plain -- true

# --- 2. start airflow + the receiver, detached from this exec (setsid/nohup; log in the sandbox)
OWNER="${SWF_SANDBOX_OWNER:-$(islo status --output json 2>/dev/null | json_field email user_email 2>/dev/null || true)}"
START_CMD="cd $(printf '%q' "$REPO_DIR") && git pull --ff-only >/dev/null 2>&1 || true; \
if curl -fsS http://localhost:8081/healthz >/dev/null 2>&1; then echo 'orchestrator already running'; \
else SWF_SANDBOX_OWNER=$(printf '%q' "$OWNER") setsid nohup bash deploy/islo/orchestrator/start.sh \
  >/workspace/orchestrator.log 2>&1 </dev/null & echo 'orchestrator starting (log: /workspace/orchestrator.log)'; fi"
islo use "$ORCH" --output plain -- bash -lc "$START_CMD"

echo "deploy: waiting for the receiver on $ORCH:8081 ..."
for _ in $(seq 1 90); do
  if islo use "$ORCH" --output plain -- bash -lc 'curl -fsS http://localhost:8081/healthz >/dev/null 2>&1' \
      >/dev/null 2>&1; then
    break
  fi
  sleep 10
done
islo use "$ORCH" --output plain -- bash -lc 'curl -fsS http://localhost:8081/healthz' \
  || { echo "deploy: receiver not healthy; islo use $ORCH -- tail -n 100 /workspace/orchestrator.log" >&2; exit 1; }
echo

# --- 3. Airflow UI share URL
echo "deploy: Airflow UI:"
islo share "$ORCH" 8080 --ttl "$SHARE_TTL" --output plain

# --- 4. islo incoming webhook (reuse by name; islo verifies HMAC + de-duplicates deliveries)
WEBHOOK_ID="$(islo webhook incoming ls --output json 2>/dev/null | python3 -c '
import json, sys
name = sys.argv[1]
doc = json.load(sys.stdin)
items = doc if isinstance(doc, list) else next((v for v in doc.values() if isinstance(v, list)), [])
for w in items:
    if isinstance(w, dict) and w.get("name") == name:
        print(w.get("id") or w.get("webhook_id") or ""); break
' "$WEBHOOK_NAME" || true)"
if [ -z "$WEBHOOK_ID" ]; then
  echo "deploy: creating incoming webhook $WEBHOOK_NAME -> $ORCH:8081/webhooks/github"
  WEBHOOK_ID="$(islo webhook incoming create \
    --name "$WEBHOOK_NAME" \
    --target-sandbox-name "$ORCH" \
    --deliver-to-port 8081 \
    --path /webhooks/github \
    --hmac-secret-name github-webhook \
    --hmac-secret-value "$GITHUB_WEBHOOK_SECRET" \
    --signature-header X-Hub-Signature-256 \
    --signature-prefix sha256= \
    --idempotency-header-name X-GitHub-Delivery \
    --auto-resume \
    --output json | json_field id webhook_id)"
else
  echo "deploy: incoming webhook $WEBHOOK_NAME exists ($WEBHOOK_ID); not recreated (rotate the secret with 'islo webhook incoming update')"
fi
: "${WEBHOOK_ID:?could not determine the incoming webhook id}"
RECEIVER_URL="$(islo webhook incoming get "$WEBHOOK_ID" --output json | json_field receiver_url url)"
: "${RECEIVER_URL:?islo webhook incoming get returned no receiver_url}"
echo "deploy: receiver URL: $RECEIVER_URL"

# --- 5. GitHub repo hook (issues + issue_comment, JSON, same secret); reuse by URL
EXISTING="$(gh api "repos/$REPO/hooks" --paginate --jq ".[] | select(.config.url == \"$RECEIVER_URL\") | .id" 2>/dev/null | head -n1 || true)"
if [ -n "$EXISTING" ]; then
  echo "deploy: GitHub hook $EXISTING already points at the receiver; updating secret + events"
  METHOD=PATCH; ENDPOINT="repos/$REPO/hooks/$EXISTING"
else
  METHOD=POST; ENDPOINT="repos/$REPO/hooks"
fi
python3 -c '
import json, os, sys
print(json.dumps({
    "name": "web", "active": True, "events": ["issues", "issue_comment"],
    "config": {"url": sys.argv[1], "content_type": "json", "insecure_ssl": "0",
               "secret": os.environ["GITHUB_WEBHOOK_SECRET"]},
}))' "$RECEIVER_URL" \
  | gh api -X "$METHOD" "$ENDPOINT" --input - --jq '"deploy: GitHub hook \(.id) -> \(.config.url) events=\(.events)"'

cat <<EOF

deploy: done.
  label an issue 'factory' / 'factory:<blueprint>' or comment '@factory run [<blueprint>]'
  orchestrator log : islo use $ORCH -- tail -f /workspace/orchestrator.log
  deliveries       : islo webhook incoming deliveries ls $WEBHOOK_ID
  approve gates    : in the shared Airflow UI, or 'swfactory approve <dag_run_id> intent|plan'
EOF
