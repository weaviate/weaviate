#!/usr/bin/env bash
#
# Start a job-level Prometheus that scrapes every ephemeral Weaviate started by
# the acceptance-test harness. No-op unless ACCEPTANCE_METRICS=true.
#
# Requires (exported by test/run.sh):
#   METRICS_SD_DIR  directory the harness writes file_sd target JSON into
# Optional:
#   PROM_PORT       host port for Prometheus (default 9090)
#   PROM_IMAGE      prometheus image (default prom/prometheus:v2.53.0)
#   GIT_SHA, GIT_BRANCH  external labels stamped onto every sample
set -euo pipefail

if [ "${ACCEPTANCE_METRICS:-}" != "true" ]; then
  echo "ACCEPTANCE_METRICS != true; skipping Prometheus startup"
  exit 0
fi

: "${METRICS_SD_DIR:?METRICS_SD_DIR must be set}"
PROM_PORT="${PROM_PORT:-9090}"
PROM_IMAGE="${PROM_IMAGE:-prom/prometheus:v2.53.0}"
PROM_CONTAINER="weaviate-acceptance-prometheus"
export GIT_SHA="${GIT_SHA:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
export GIT_BRANCH="${GIT_BRANCH:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)}"
export METRICS_SD_DIR

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$METRICS_SD_DIR"

# Render the config from the template (substitute env vars only).
cfg="$METRICS_SD_DIR/prometheus.yml"
envsubst '${METRICS_SD_DIR} ${GIT_SHA} ${GIT_BRANCH}' \
  < "$script_dir/prometheus.tpl.yml" > "$cfg"

# Remove any leftover from a previous run on the same runner.
docker rm -f "$PROM_CONTAINER" >/dev/null 2>&1 || true

# Host networking so 127.0.0.1:<mappedPort> in the file_sd targets resolves to
# the testcontainers-published ports. Mount the SD dir at the same path the
# config globs, so the in-container glob matches the host paths.
docker run -d --name "$PROM_CONTAINER" \
  --network host \
  -v "$cfg:/etc/prometheus/prometheus.yml:ro" \
  -v "$METRICS_SD_DIR:$METRICS_SD_DIR:ro" \
  "$PROM_IMAGE" \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --storage.tsdb.retention.time=2h \
  --web.listen-address="0.0.0.0:${PROM_PORT}" \
  --web.enable-admin-api >/dev/null

echo "Started Prometheus '$PROM_CONTAINER' on :${PROM_PORT} (sd=$METRICS_SD_DIR)"
