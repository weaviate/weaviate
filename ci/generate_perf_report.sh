#!/usr/bin/env bash
#
# Render the acceptance-test performance dashboard and append it to the GitHub
# Actions run summary. Safe to call with `if: always()` — it never fails the job.
#
# Inputs (env):
#   ACCEPTANCE_METRICS   must be "true" or this is a no-op
#   PROM_URL             Prometheus base URL (default http://localhost:9090)
#   PERF_METRICS_OUT     where to write this run's metrics JSON (artifact source)
#   PERF_BASELINE        baseline metrics JSON to compare against (optional)
#   PERF_TITLE           report title (optional)
set -uo pipefail

if [ "${ACCEPTANCE_METRICS:-}" != "true" ]; then
  echo "ACCEPTANCE_METRICS != true; skipping perf report"
  exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PERF_METRICS_OUT="${PERF_METRICS_OUT:-perf-metrics.json}"
PERF_TITLE="${PERF_TITLE:-Acceptance performance metrics}"

# Diagnostics: the report runs after all test containers are torn down, so live
# targets are gone — but the `up` series persists in TSDB. Its history tells us
# whether targets were ever registered (H1) vs registered-but-unreachable (H2).
echo "=== perf diag: prometheus container ==="
docker ps -a --filter "name=weaviate-acceptance-prometheus" --format '{{.Names}} {{.Status}}' 2>&1 || true
echo "=== perf diag: prometheus logs (tail) ==="
docker logs weaviate-acceptance-prometheus 2>&1 | tail -30 || true
echo "=== perf diag: SD dir contents ==="
ls -la "${METRICS_SD_DIR:-/nonexistent}" 2>&1 || true
echo "=== perf diag: registrations.log (harness wrote a target here when it registered) ==="
cat "${METRICS_SD_DIR:-/nonexistent}/registrations.log" 2>&1 | tail -20 || echo "(none — harness never registered a target)"
echo "=== perf diag: up series history (empty => targets never registered; max=0 => unreachable; max=1 => scraped) ==="
curl -fsS "$PROM_URL/api/v1/query?query=max_over_time(up%5B6h%5D)" 2>&1 || echo "(curl failed — prometheus unreachable)"
echo
echo "=== perf diag: any weaviate samples ever scraped ==="
curl -fsS "$PROM_URL/api/v1/query?query=count_over_time(scrape_samples_scraped%5B6h%5D)" 2>&1 || echo "(curl failed)"
echo

args=(-prom "$PROM_URL" -out "$PERF_METRICS_OUT" -title "$PERF_TITLE")
if [ -n "${PERF_BASELINE:-}" ] && [ -s "${PERF_BASELINE}" ]; then
  args+=(-baseline "$PERF_BASELINE")
fi

report="$(mktemp)"
if ! go run "$repo_root/test/perf-report" "${args[@]}" > "$report" 2>/tmp/perf-report.err; then
  echo "perf-report failed (non-fatal):"
  cat /tmp/perf-report.err || true
  rm -f "$report"
  exit 0
fi

cat "$report"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  cat "$report" >> "$GITHUB_STEP_SUMMARY"
fi
rm -f "$report"
