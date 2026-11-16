#!/bin/bash
# v2.0 (2026-11-16): the streaming experiment in memory on dbpedia-openai 1M
# (bench-report.md, "v2.0: streaming en memoria").
#
#  1. Calibration of the split threshold: assignment and split alone, for
#     every threshold, and the threshold whose final mean posting size is
#     nearest 236, within +-15 %. None within: stop. A calibration already
#     done for every threshold is not repeated (the amendment of the
#     pre-registration, 2026-11-16).
#  2. The principal cell, with the batch reference and, at the end, the timed
#     frontiers of the reference, the reference keyed by doc id and the cell.
#     It runs alone.
#  3. The controls H1 and H2 and the principal cell in natural order, untimed,
#     side by side.
#  4. Both mechanisms together, if the H2 cell's inserts took at most three
#     times the principal's.
#
# usage: WORK=<dir> caffeinate -s -i run_v20_stream.sh
#   WORK holds postingedge-v20b.test, built before the run, with the guard of
#   the amendment:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v20b.test ./adapters/repos/db/vector/postingedge/
#
# Nothing is committed to the repository while it runs.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$(cd "$HERE/.." && pwd)" # testdata/bench/v2.0
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v20b.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=40GiB
START="$(date '+%Y-%m-%d %H:%M:%S')"
COMMON=(-test.run 'TestStream$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000
  -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.workthreads 8)

stream() { # stream <label> <flags...>
  local label=$1
  shift
  date "+== %H:%M start $label"
  "$PE" "${COMMON[@]}" "$@" -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
  echo "== $label: $(tail -1 "$WORK/$label.log")"
  grep -E "calibration:|checkpoint|stop and report|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400
}

date "+== %Y-%m-%d %H:%M waiting for no other benchmark"
while others || pgrep -f 'postingedge-(p2|v20)\.test' > /dev/null; do sleep 60; done
power_state

THRESHOLDS=280,320,360,400,472
DONE=$(python3 -I -c 'import json, sys
try:
    print(len(json.load(open(sys.argv[1]))))
except (OSError, ValueError):
    print(0)' "$OUT/dbpedia-1m-stream-calibration.json")
if [ "$DONE" = "$(echo $THRESHOLDS | tr ',' '\n' | wc -l | tr -d ' ')" ]; then
  echo "== the calibration is done for every threshold: not repeated"
else
  stream dbpedia-1m-stream-calibration -bench.stream calibrate -bench.streamthresholds "$THRESHOLDS"
fi
T=$(python3 -I - "$OUT/dbpedia-1m-stream-calibration.json" <<'EOF'
import json, sys
rows = json.load(open(sys.argv[1]))
inside = [r for r in rows if abs(r["sizeMean"] / 236 - 1) <= 0.15]
print(min(inside, key=lambda r: abs(r["sizeMean"] - 236))["threshold"] if inside else "")
EOF
)
if [ -z "$T" ]; then
  echo "== no threshold keeps the mean posting size within 15 % of 236: no cell runs"
  power_log_since "$START"
  exit 1
fi
echo "== threshold $T"

stream dbpedia-1m-stream-principal -bench.stream principal -bench.streamthreshold "$T" -bench.streamreference \
  -bench.runs 3 -bench.repeat -bench.sweep "ef=10,20,40,80,160,320,640,1024 threads=1,8 rescore=64 batch=4 margin=0.06"

stream dbpedia-1m-stream-h1 -bench.stream h1 -bench.streamthreshold "$T" &
stream dbpedia-1m-stream-h2 -bench.stream h2 -bench.streamthreshold "$T" &
stream dbpedia-1m-stream-natural -bench.stream principal -bench.streamorder natural -bench.streamthreshold "$T" &
wait

CHEAP=$(python3 -I - "$OUT" <<'EOF'
import json, os, sys
def secs(name):
    doc = json.load(open(os.path.join(sys.argv[1], "dbpedia-1m-stream-%s.json" % name)))
    return doc["checkpoints"][-1]["insertSec"] if doc["checkpoints"] and doc["checkpoints"][-1]["percent"] == 100 else None
p, h2 = secs("principal"), secs("h2")
print("yes" if p and h2 and h2 <= 3 * p else "no (principal %s s, h2 %s s)" % (p, h2))
EOF
)
echo "== both mechanisms together: $CHEAP"
if [ "$CHEAP" = yes ]; then
  stream dbpedia-1m-stream-both -bench.stream both -bench.streamthreshold "$T"
fi

date "+== %H:%M end"
power_state
power_log_since "$START"
