#!/bin/bash
# v2.0, cell 6 (pre-registered 2026-11-16): inverse edges with sibling repair,
# on dbpedia-openai 1M, queued behind cell 5 (bench-report.md, "Celda 6:
# aristas inversas más reparación por hermanos"). Untimed and alone, with the
# threshold of the calibration.
#
# Behind it, also alone, H1 again with this binary and one checkpoint at
# 100 %: the binary keeps the inverse-edge writes of every insert, which the
# H1 run of the chain did not, and the two runs of H1 give the noise between
# runs at 100 % (bench-report.md, "Escrituras de aristas inversas por insert,
# para H1" and "Suelo de ruido entre corridas").
#
# At the end, the verdict of every cell into stream-verdict.md.
#
# usage: WORK=<dir> caffeinate -s -i run_v20_cell6.sh
#   WORK holds postingedge-v20d.test, built before the run, with cell 6:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v20d.test ./adapters/repos/db/vector/postingedge/
#
# Nothing is committed to the repository while it runs.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$(cd "$HERE/.." && pwd)" # testdata/bench/v2.0
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v20d.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=40GiB

date "+== %Y-%m-%d %H:%M queued behind run_v20_stream.sh and run_v20_siblings.sh"
while pgrep -f 'run_v20_(stream|siblings)\.sh' > /dev/null || others ||
  pgrep -f 'postingedge-(p2|v20[a-z]?)\.test' > /dev/null; do
  sleep 60
done
START="$(date '+%Y-%m-%d %H:%M:%S')"
power_state
T=$(python3 -I - "$OUT/dbpedia-1m-stream-calibration.json" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))
inside = [r for r in rows if abs(r["sizeMean"] / 236 - 1) <= 0.15]
print(min(inside, key=lambda r: abs(r["sizeMean"] - 236))["threshold"] if inside else "")
PY
)
[ -n "$T" ] || { echo "== no calibrated threshold"; exit 1; }
echo "== threshold $T"

stream() { # stream <label> <flags...>
  local label=$1
  shift
  date "+== %H:%M start $label"
  "$PE" -test.run 'TestStream$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000 \
    -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.workthreads 8 \
    -bench.streamthreshold "$T" "$@" -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
  echo "== $label: $(tail -1 "$WORK/$label.log")"
  grep -E "checkpoint|stop and report|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-420
}

stream dbpedia-1m-stream-inverse-siblings -bench.stream inverse-siblings
stream dbpedia-1m-stream-h1-writes -bench.stream h1 -bench.streamcheckpoints 100

python3 -I "$HERE/stream_verdict.py" "$OUT" > "$OUT/stream-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/stream-verdict.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
