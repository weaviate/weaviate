#!/usr/bin/env bash
# Captures system + Go profiles WHILE a benchmark run is hitting Weaviate
# (Phase 0 of docs/hfresh-search-optimization.md). Start it right before the
# benchmark, stop with Ctrl-C (or it stops itself after DURATION seconds).
#
# Collects into one directory:
#   - iostat 1s samples (disk IOPS/throughput/latency during the run)
#   - pprof CPU profile (30s) and goroutine/heap snapshots
#   - fgprof-style off-CPU: pprof "block" is not enabled by default, so we
#     grab /debug/pprof/profile plus mutex; wall-clock attribution comes from
#     the HFresh search profiler logs (HFRESH_SEARCH_PROFILE=1)
#
# Usage:
#   WEAVIATE_PPROF=localhost:6060 ./tools/dev/hfresh_bench_capture.sh [duration_s] [out_dir]
#
# Weaviate must run with pprof enabled (GO_PROFILING_PORT=6060, on by
# default unless GO_PROFILING_DISABLE is set) and, for the search breakdown,
# HFRESH_SEARCH_PROFILE=1.
set -euo pipefail

DURATION=${1:-120}
OUT_DIR=${2:-"hfresh_capture_$(date +%Y%m%d_%H%M%S)"}
PPROF=${WEAVIATE_PPROF:-localhost:6060}
mkdir -p "$OUT_DIR"

echo "==> Capturing for ${DURATION}s into $OUT_DIR (pprof at $PPROF)"

# disk activity for the whole window
iostat -xmt 1 "$DURATION" > "$OUT_DIR/iostat.txt" &
IOSTAT_PID=$!

# CPU profile for 30s in the middle of the window
(
  sleep 5
  curl -sf "http://$PPROF/debug/pprof/profile?seconds=30" -o "$OUT_DIR/cpu.pprof" \
    && echo "==> cpu.pprof done" || echo "!! cpu profile failed"
  curl -sf "http://$PPROF/debug/pprof/goroutine?debug=0" -o "$OUT_DIR/goroutine.pprof" || true
  curl -sf "http://$PPROF/debug/pprof/heap?debug=0" -o "$OUT_DIR/heap.pprof" || true
  curl -sf "http://$PPROF/debug/pprof/mutex?debug=0" -o "$OUT_DIR/mutex.pprof" || true
) &
PPROF_PID=$!

trap 'kill $IOSTAT_PID $PPROF_PID 2>/dev/null || true' INT TERM

wait "$IOSTAT_PID" 2>/dev/null || true
wait "$PPROF_PID" 2>/dev/null || true

echo "==> Done. Analyze with:"
echo "    go tool pprof -http :8080 $OUT_DIR/cpu.pprof"
echo "    grep hfresh_search_profile <weaviate logs>   # phase/IO breakdown"
