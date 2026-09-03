#!/usr/bin/env bash
# deploy/islo/knowledge.sh — publish the factory's institutional knowledge as islo knowledge items
# so every sandbox agent (and every teammate's `islo use`) can render the same contract:
#
#   islo knowledge render --repo <owner/repo> --tag swfactory
#
# Items (identifier <- file, type):
#   swfactory-claude-md  <- CLAUDE.md                        rule   (conventions, common mistakes)
#   swfactory-review-md  <- REVIEW.md                        rule   (review contract the agent follows)
#   swfactory-skill      <- .claude/skills/swfactory/SKILL.md skill (spec/plan shape, review passes)
#
# Idempotent: an item that already exists (`islo knowledge get <id>` exits 0) is updated in place
# (a new immutable version; `islo knowledge versions <id>` / `restore` keep history), otherwise it
# is created. Markdown goes through `--body @file` (`--file` is the upload path for image/video/
# audio items only). Links (--tag/--repo) are replaced on update, so the result is the same set
# whether the script ran once or ten times. No secrets, no sandbox names.
#
# Usage:
#   deploy/islo/knowledge.sh [owner/repo]        # default: $SWF_REPO or zozo123/ariflow-swfactory
#   SWF_KNOWLEDGE_TAG=<tag> ...                  # default tag: swfactory
#   ISLO_BIN=/path/to/islo ...                   # default: `islo` on PATH (tests use a stub)
# Needs `islo login` (or ISLO_API_KEY) on the machine that runs it; verified flags: islo 0.48.1.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
REPO=${1:-${SWF_REPO:-zozo123/ariflow-swfactory}}
TAG=${SWF_KNOWLEDGE_TAG:-swfactory}
ISLO=${ISLO_BIN:-islo}

# identifier|type|repo-relative file — identifiers are lowercase-hyphen and can never be renamed.
ITEMS=(
  "swfactory-claude-md|rule|CLAUDE.md"
  "swfactory-review-md|rule|REVIEW.md"
  "swfactory-skill|skill|.claude/skills/swfactory/SKILL.md"
)

exists() {
  # `get` prints the item and exits 0 when it exists; any other outcome means "create".
  "$ISLO" knowledge get "$1" --output json >/dev/null 2>&1
}

publish() {
  local id=$1 type=$2 file=$3 path="$ROOT/$3" verb
  if [ ! -s "$path" ]; then
    echo "knowledge: missing or empty $file (looked at $path)" >&2
    return 1
  fi
  if exists "$id"; then verb=update; else verb=create; fi
  "$ISLO" knowledge "$verb" "$id" --output plain --type "$type" --body "@$path" \
    --tag "$TAG" --repo "$REPO"
  echo "knowledge: ${verb}d $id  <- $file  (type=$type tag=$TAG repo=$REPO)"
}

command -v "$ISLO" >/dev/null 2>&1 || {
  echo "knowledge: '$ISLO' not on PATH (curl -fsSL https://islo.dev/install.sh | bash)" >&2
  exit 2
}

for item in "${ITEMS[@]}"; do
  IFS='|' read -r id type file <<<"$item"
  publish "$id" "$type" "$file"
done

echo
echo "knowledge: rendered view every sandbox agent can pull (islo knowledge render):"
"$ISLO" knowledge render --output plain --repo "$REPO" --tag "$TAG"
