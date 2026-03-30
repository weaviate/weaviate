#!/bin/bash

set -euo pipefail

RUN_ID=${1:-}
if [ -z "$RUN_ID" ]; then
  echo "Usage: $0 <run_id>"
  exit 1
fi

echo "Monitoring generate-docker-report for run $RUN_ID..."

while true; do
  RESULT=$(gh run view "$RUN_ID" --repo weaviate/weaviate --json jobs \
    --jq '.jobs[] | select(.name | test("generate-docker-report|docker report")) | {status: .status, conclusion: .conclusion}' 2>&1)

  STATUS=$(echo "$RESULT" | jq -r '.status' 2>/dev/null || echo "unknown")
  CONCLUSION=$(echo "$RESULT" | jq -r '.conclusion' 2>/dev/null || echo "unknown")

  echo "[$(date '+%H:%M:%S')] status=$STATUS conclusion=$CONCLUSION"

  if [ "$STATUS" = "completed" ]; then
    if [ "$CONCLUSION" = "success" ]; then
      echo "Docker image is ready!"
      JOB_ID=$(gh run view "$RUN_ID" --repo weaviate/weaviate --json jobs \
        --jq '.jobs[] | select(.name | test("generate-docker-report|docker report")) | .databaseId')
      echo "Docker image tags:"
      gh api repos/weaviate/weaviate/actions/jobs/"$JOB_ID"/logs 2>&1 \
        | sed 's/\x1b\[[0-9;]*m//g' \
        | grep -oE 'semitechnologies/weaviate:[a-zA-Z0-9.:_-]+' \
        | sort -u \
        | while read -r tag; do echo "  $tag"; done
      exit 0
    else
      echo "generate-docker-report ended with: $CONCLUSION"
      exit 1
    fi
  fi

  sleep 30
done
