#!/usr/bin/env bash
#
# Stop the job-level acceptance-test Prometheus. No-op if it was never started.
set -euo pipefail

PROM_CONTAINER="weaviate-acceptance-prometheus"
docker rm -f "$PROM_CONTAINER" >/dev/null 2>&1 || true
echo "Stopped Prometheus '$PROM_CONTAINER' (if it was running)"
