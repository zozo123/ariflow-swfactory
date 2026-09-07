from pathlib import Path

path = Path("scripts/swf_e2e.sh")
text = path.read_text()


def one(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one shell replacement, found {count}: {old[:100]!r}")
    text = text.replace(old, new, 1)


one(
    '''# ---------------------------------------------------------------- 2. submit

say "swf submit: ${#ISSUES[@]} issues x 2 targets"
SUBMIT_ARGS=()
for issue in "${ISSUES[@]}"; do SUBMIT_ARGS+=(--issue "$issue"); done
SUBMIT_ID_ARGS=()
if [ "$BACKEND_MODE" -eq 1 ]; then
  # This proves the normal harness identity crosses Rust -> backend -> Cell admission -> Airflow.
  SUBMIT_ID_ARGS=(--harness codex --factory-id e2e-codex)
fi
"$SWF" submit --blueprint "$DAG_ID" "${SUBMIT_ARGS[@]}" "${SUBMIT_ID_ARGS[@]}" --json >"$WORK/submit.json"
cat "$WORK/submit.json"
RUN_ID="$("$PY" -c "import json;print(json.load(open('$WORK/submit.json'))['run_id'])")"
[ -n "$RUN_ID" ] || fail "swf submit returned no run id"
echo "run $DAG_ID/$RUN_ID"
''',
    '''# ---------------------------------------------------------------- 2. submit

RUN_IDS=()
TOTAL_ISSUES=${#ISSUES[@]}
TUI_EXPECT_ISSUE="${ISSUES[0]}"
if [ "$BACKEND_MODE" -eq 1 ]; then
  say "concurrent harness submit: Codex + Claude Code + Grok + duplicate Cell race"
  "$PY" "$REPO/scripts/multi_harness_submit.py" \\
    --swf "$SWF" --blueprint "$DAG_ID" --backend-url "$BACKEND_BASE" \\
    >"$WORK/multi-harness.json" || fail "live multi-harness admission/race/replay probe"
  cat "$WORK/multi-harness.json"
  mapfile -t RUN_IDS < <("$PY" - "$WORK/multi-harness.json" <<'PYEOF'
import json
import sys
for row in json.load(open(sys.argv[1]))["runs"]:
    print(row["run_id"])
PYEOF
  )
  TOTAL_ISSUES=${#RUN_IDS[@]}
  [ "$TOTAL_ISSUES" -eq 4 ] || fail "multi-harness probe returned $TOTAL_ISSUES successful runs, expected 4"
  TUI_EXPECT_ISSUE="e2e-codex"
  RUN_ID="${RUN_IDS[0]}"
else
  say "swf submit: ${#ISSUES[@]} issues x 2 targets"
  SUBMIT_ARGS=()
  for issue in "${ISSUES[@]}"; do SUBMIT_ARGS+=(--issue "$issue"); done
  "$SWF" submit --blueprint "$DAG_ID" "${SUBMIT_ARGS[@]}" --json >"$WORK/submit.json"
  cat "$WORK/submit.json"
  RUN_ID="$("$PY" -c "import json;print(json.load(open('$WORK/submit.json'))['run_id'])")"
  [ -n "$RUN_ID" ] || fail "swf submit returned no run id"
  RUN_IDS=("$RUN_ID")
fi
printf 'runs:'
printf ' %s' "${RUN_IDS[@]}"
printf '\n'
''',
)

one(
    '''while [ $i -lt "$RUN_TIMEOUT_S" ]; do
  STATE="$("$SWF" runs inspect "$DAG_ID/$RUN_ID" --json | field state)"
  case "$STATE" in success | failed) break ;; esac

  ready="$("$SWF" gates list --dag "$DAG_ID" --ready --json | "$PY" -c 'import json,sys; print(len(json.load(sys.stdin)))')"
''',
    '''while [ $i -lt "$RUN_TIMEOUT_S" ]; do
  all_terminal=1
  any_failed=0
  run_states=()
  for current_run in "${RUN_IDS[@]}"; do
    current_state="$("$SWF" runs inspect "$DAG_ID/$current_run" --json | field state)"
    run_states+=("$current_run=$current_state")
    case "$current_state" in
      success) ;;
      failed) any_failed=1 ;;
      *) all_terminal=0 ;;
    esac
  done
  if [ "$all_terminal" -eq 1 ]; then
    if [ "$any_failed" -eq 1 ]; then STATE="failed"; else STATE="success"; fi
    break
  fi
  STATE="running"

  ready="$("$SWF" gates list --dag "$DAG_ID" --ready --json | "$PY" -c 'import json,sys; print(len(json.load(sys.stdin)))')"
''',
)

one(
    '''EXPECTED_GATES=$(( ${#ISSUES[@]} * 2 * 2 ))   # issues x targets x (intent, plan)
''',
    '''EXPECTED_GATES=$(( TOTAL_ISSUES * 2 * 2 ))   # successful one-issue runs x targets x (intent, plan)
''',
)

one(
    '''"$PY" "$REPO/scripts/tui_smoke.py" --bin "$SWF" --key 2 \\
  --expect "$DAG_ID" --expect "demo/issue.md" --expect "success" \\
''',
    '''"$PY" "$REPO/scripts/tui_smoke.py" --bin "$SWF" --key 2 \\
  --expect "$DAG_ID" --expect "$TUI_EXPECT_ISSUE" --expect "success" \\
''',
)

one(
    '''VERIFIED=0
while read -r job_id; do
  [ -n "${job_id:-}" ] || continue
  idx="${job_id##*#}"
  run_id="$("$PY" -c "
from swfactory.runtime import run_id_for
print(run_id_for('$RUN_ID', int('$idx')))
")"
  remote="$WORK/.factory/$run_id/remote.git"
''',
    '''VERIFIED=0
: >"$WORK/delivery-keys.txt"
while read -r job_id; do
  [ -n "${job_id:-}" ] || continue
  idx="${job_id##*#}"
  job_ref="${job_id%#*}"
  job_airflow_run="${job_ref#*/}"
  runtime_id="$("$PY" -c "
from swfactory.runtime import run_id_for
print(run_id_for('$job_airflow_run', int('$idx')))
")"
  remote="$WORK/.factory/$runtime_id/remote.git"
''',
)

one(
    '''  branch="$(git -C "$remote" for-each-ref --format='%(refname:short)' 'refs/heads/factory/*' | head -1)"
  [ -n "$branch" ] || fail "job $idx has no factory/* branch in $remote"
  printf 'verifying job %s: %s\n' "$idx" "$branch"
''',
    '''  branch="$(git -C "$remote" for-each-ref --format='%(refname:short)' 'refs/heads/factory/*' | head -1)"
  [ -n "$branch" ] || fail "job $idx has no factory/* branch in $remote"
  printf '%s|%s\n' "$remote" "$branch" >>"$WORK/delivery-keys.txt"
  printf 'verifying job %s: %s\n' "$idx" "$branch"
''',
)

one(
    '''EXPECTED_DELIVERIES=$(( ${#ISSUES[@]} * 2 ))
[ "$VERIFIED" -eq "$EXPECTED_DELIVERIES" ] ||
  fail "independently verified $VERIFIED deliveries, expected $EXPECTED_DELIVERIES"
echo "$VERIFIED deliveries independently verified"

if [ "$STATE" != "success" ]; then
''',
    '''EXPECTED_DELIVERIES=$(( TOTAL_ISSUES * 2 ))
[ "$VERIFIED" -eq "$EXPECTED_DELIVERIES" ] ||
  fail "independently verified $VERIFIED deliveries, expected $EXPECTED_DELIVERIES"
unique_remotes="$(cut -d'|' -f1 "$WORK/delivery-keys.txt" | sort -u | wc -l | tr -d ' ')"
unique_branches="$(cut -d'|' -f2- "$WORK/delivery-keys.txt" | sort -u | wc -l | tr -d ' ')"
[ "$unique_remotes" -eq "$EXPECTED_DELIVERIES" ] || fail "delivery run directories collided"
[ "$unique_branches" -eq "$EXPECTED_DELIVERIES" ] || fail "delivery branches collided"
echo "$VERIFIED deliveries independently verified; workspaces/remotes/branches are distinct"

if [ "$STATE" != "success" ]; then
''',
)

one(
    '''say "OK: $DAG_ID green — ${#ISSUES[@]} issues x 2 targets, $answered gates answered through swf, \\
$(( ${#ISSUES[@]} * 2 )) deliveries independently verified"
''',
    '''say "OK: $DAG_ID green — $TOTAL_ISSUES successful one-issue sessions x 2 targets, $answered gates answered through swf, \\
$(( TOTAL_ISSUES * 2 )) deliveries independently verified"
''',
)

path.write_text(text)
