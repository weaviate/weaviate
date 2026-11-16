#!/bin/bash
# v2.1 (pre-registered 2026-11-16): the insert ef sweep over H1, on
# dbpedia-openai 1M (bench-report.md, "v2.1: el ef de inserción, sobre H1").
# Four cells, insert ef 160, 80, 40 and 20, with the protocol of the H1 cell
# of v2.0: inverse edges, the threshold of its calibration, one checkpoint at
# 100 % and the guard of the edge builder at 60, 70, 80 and 90 %. They count
# and time nothing, so they run at once, each in its own process: a cell the
# guard stops ends alone and the others go on.
#
# At the end, the verdict into insertef-verdict.md, against the batch
# reference and the two runs of H1 at ef 320 of v2.0.
#
# usage: WORK=<dir> caffeinate -s -i run_v21_insertef.sh
#   WORK holds postingedge-v21.test, built before the run:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v21.test ./adapters/repos/db/vector/postingedge/
#   For a smoke: OUT=<dir> EXTRA="-bench.limit 30000 -bench.queries 500 ..." EFS="80 20"
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
V20="$(cd "$HERE/../../v2.0" && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}" # testdata/bench/v2.1
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v21.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
EFS="${EFS:-160 80 40 20}"
T=360 # the calibration of v2.0
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=14GiB # four processes on 64 GB

date "+== %Y-%m-%d %H:%M waiting for no other benchmark"
while others || pgrep -f 'postingedge-(p2|v2[0-9][a-z]?)\.test' > /dev/null; do sleep 60; done
START="$(date '+%Y-%m-%d %H:%M:%S')"
power_state
echo "== threshold $T, insert ef $EFS"

stream() { # stream <label> <flags...>
  local label=$1
  shift
  date "+== %H:%M start $label"
  # shellcheck disable=SC2086
  "$PE" -test.run 'TestStream$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000 \
    -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.workthreads 8 \
    -bench.streamthreshold "$T" "$@" ${EXTRA:-} -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
  date "+== %H:%M $label: $(tail -1 "$WORK/$label.log")"
  grep -E "checkpoint|guard|stop and report|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-420
}

for EF in $EFS; do
  stream "dbpedia-1m-stream-h1-ef$EF" -bench.stream h1 -bench.streaminsertef "$EF" \
    -bench.streamcheckpoints 100 -bench.streamguards 60,70,80,90 &
done
wait

python3 -I -B "$HERE/insertef_verdict.py" "$OUT" "$V20" > "$OUT/insertef-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/insertef-verdict.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
