#!/usr/bin/env bash
# searchProbe sweep for HFresh QPS/latency on a rig (Phase 0/1 of
# docs/hfresh-search-optimization.md). For each probe value: updates the
# collection config (searchProbe is mutable, applied atomically without
# restart), optionally drops the page cache, runs the benchmarker's
# random-vectors mode, and tags the output.
#
# Usage:
#   CLASS=MyClass BENCHMARKER=/path/to/weaviate-benchmarking/benchmarker \
#     ./tools/dev/hfresh_rig_sweep.sh [probe values...]
#
# Env:
#   CLASS        (required) collection name
#   BENCHMARKER  (required) path to the weaviate-benchmarking/benchmarker dir
#   HTTP         http origin (default localhost:8080)
#   GRPC         grpc origin (default localhost:50051)
#   PARALLEL     client parallelism (default 16)
#   DURATION     seconds per probe setting (default 60)
#   DROP_CACHES  1 = drop page cache before each setting (default 1; needs root)
#   OUT_DIR      output directory (default hfresh_sweep_<ts>)
set -euo pipefail

CLASS=${CLASS:?"set CLASS to the collection name"}
BENCHMARKER=${BENCHMARKER:?"set BENCHMARKER to the weaviate-benchmarking/benchmarker directory"}
HTTP=${HTTP:-localhost:8080}
GRPC=${GRPC:-localhost:50051}
PARALLEL=${PARALLEL:-16}
DURATION=${DURATION:-60}
DROP_CACHES=${DROP_CACHES:-1}
OUT_DIR=${OUT_DIR:-"hfresh_sweep_$(date +%Y%m%d_%H%M%S)"}

PROBES=("$@")
if [ ${#PROBES[@]} -eq 0 ]; then
  PROBES=(16 32 64 128 256 512)
fi

command -v jq >/dev/null || { echo "jq is required"; exit 1; }
mkdir -p "$OUT_DIR"

set_probe() {
  local probe=$1
  local schema
  schema=$(curl -sf "http://$HTTP/v1/schema/$CLASS")
  echo "$schema" \
    | jq ".vectorIndexConfig.searchProbe = $probe" \
    | curl -sf -X PUT "http://$HTTP/v1/schema/$CLASS" \
        -H 'Content-Type: application/json' -d @- > /dev/null
  # verify
  local got
  got=$(curl -sf "http://$HTTP/v1/schema/$CLASS" | jq '.vectorIndexConfig.searchProbe')
  if [ "$got" != "$probe" ]; then
    echo "!! failed to set searchProbe=$probe (got $got)"
    exit 1
  fi
}

for probe in "${PROBES[@]}"; do
  echo "==> searchProbe=$probe"
  set_probe "$probe"

  if [ "$DROP_CACHES" = "1" ]; then
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null || \
      echo "!! could not drop caches (no root?); results will be warm"
  fi

  (
    cd "$BENCHMARKER"
    go run main.go random-vectors \
      -c "$CLASS" \
      -p "$PARALLEL" \
      --queryDuration "$DURATION" \
      -a grpc \
      -u "$GRPC" --httpOrigin "$HTTP"
  ) 2>&1 | tee "$OUT_DIR/probe_${probe}.log"
done

echo "==> Sweep done: $OUT_DIR"
echo "    Server-side phase/IO breakdown: grep hfresh_search_profile <weaviate logs>"
echo "    Match profiler windows to settings via timestamps."
