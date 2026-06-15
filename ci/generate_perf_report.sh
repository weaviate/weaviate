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

# Log target health so a zero-data report is debuggable from the build log.
if command -v curl >/dev/null 2>&1; then
  echo "Prometheus targets health:"
  curl -fsS "$PROM_URL/api/v1/targets" 2>/dev/null \
    | grep -o '"health":"[a-z]*"' | sort | uniq -c || echo "  (could not query targets)"
fi

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
