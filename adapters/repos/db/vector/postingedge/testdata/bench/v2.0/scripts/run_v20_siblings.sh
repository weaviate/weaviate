#!/bin/bash
# v2.0, cell 5 (pre-registered 2026-11-16): sibling repair at a split, on
# dbpedia-openai 1M, queued behind the chain of run_v20_stream.sh
# (bench-report.md, "Celda 5: reparación por hermanos en el split"). Untimed,
# like the controls, with the threshold of the calibration that chain read.
# At the end, the verdict of every cell into stream-verdict.md.
#
# usage: WORK=<dir> caffeinate -s -i run_v20_siblings.sh
#   WORK holds postingedge-v20c.test, built before the run, with cell 5:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v20c.test ./adapters/repos/db/vector/postingedge/
#
# Nothing is committed to the repository while it runs.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$(cd "$HERE/.." && pwd)" # testdata/bench/v2.0
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v20c.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=40GiB

date "+== %Y-%m-%d %H:%M queued behind run_v20_stream.sh"
while pgrep -f 'run_v20_stream\.sh' > /dev/null || others || pgrep -f 'postingedge-(p2|v20b?)\.test' > /dev/null; do
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

label=dbpedia-1m-stream-siblings
date "+== %H:%M start $label"
"$PE" -test.run 'TestStream$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000 \
  -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.workthreads 8 \
  -bench.stream siblings -bench.streamthreshold "$T" -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
echo "== $label: $(tail -1 "$WORK/$label.log")"
grep -E "checkpoint|stop and report|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400
python3 -I "$HERE/stream_verdict.py" "$OUT" > "$OUT/stream-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/stream-verdict.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
