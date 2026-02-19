#!/bin/bash
# Monitor CI checks for a PR until all checks complete.
# Usage: PR=<number> .claude/scripts/monitor_pr.sh
# Exits with code 0 if all pass, 1 if any fail.
PR=${PR:-}
if [ -z "$PR" ]; then
  echo "Usage: PR=<number> $0"
  exit 1
fi

while true; do
  OUTPUT=$(gh pr checks $PR --repo weaviate/weaviate 2>&1)
  PASS=$(echo "$OUTPUT" | grep -c $'\tpass\t')
  FAIL=$(echo "$OUTPUT" | grep -c $'\tfail\t')
  PENDING=$(echo "$OUTPUT" | grep -c $'\tpending\t')
  FAILED_CHECKS=$(echo "$OUTPUT" | grep $'\tfail\t' | awk -F'\t' '{print $1}')

  echo "[$(date '+%H:%M:%S')] PASS: $PASS | FAIL: $FAIL | PENDING: $PENDING"

  if [ "$PENDING" -eq 0 ]; then
    echo ""
    echo "=== ALL CHECKS COMPLETE ==="
    echo "PASSED: $PASS | FAILED: $FAIL"
    if [ -n "$FAILED_CHECKS" ]; then
      echo ""
      echo "Failed checks:"
      echo "$FAILED_CHECKS" | while read line; do echo "  - $line"; done
      exit 1
    fi
    exit 0
  fi

  sleep 60
done
