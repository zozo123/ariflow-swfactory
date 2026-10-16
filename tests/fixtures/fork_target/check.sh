#!/bin/sh
# Passes only on the fanned-in candidate: every sibling's bytes, then the index built from them.
mkdir -p .factory
failures=0
for node in alpha beta gamma; do
  grep -qx "written by $node in a forked cell" "src/$node.txt" 2>/dev/null || failures=$((failures + 1))
done
grep -qx "alpha beta gamma" src/index.txt 2>/dev/null || failures=$((failures + 1))
printf '<testsuite tests="4" failures="%s" errors="0" skipped="0"/>\n' "$failures" > .factory/junit.xml
[ "$failures" -eq 0 ]
