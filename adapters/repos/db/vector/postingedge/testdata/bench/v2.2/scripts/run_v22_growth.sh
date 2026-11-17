#!/bin/bash
# v2.2 (pre-registered 2026-11-17): growing from 10 % over H1, on
# dbpedia-openai 1M (bench-report.md, "v2.2: crecer desde el 10 %, sobre H1").
# Two cells, insert ef 160 and 80: a batch build over the first 10 % of the
# train set and the other 90 % inserted with inverse edges, with checkpoints
# at 20, 40, 60, 80 and 100 %. With them, a batch build over the whole train
# set that only counts its work. They count and time nothing, so the three
# run at once, each in its own process: a cell the guard stops ends alone and
# the others go on.
#
# At the end, the verdict into growth-verdict.md, against the batch reference
# of v2.0 and the cells of v2.1 with the same insert ef.
#
# usage: WORK=<dir> caffeinate -s -i run_v22_growth.sh
#   WORK holds postingedge-v22.test, built before the run:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v22.test ./adapters/repos/db/vector/postingedge/
#   For a recalibrated run: T=<threshold> BUILDWORK=0, which writes the cells
#   as ...-ef<ef>-t<threshold>.json.
#   For a smoke: OUT=<dir> EXTRA="-bench.limit 30000 -bench.queries 500 ..."
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
V20="$(cd "$HERE/../../v2.0" && pwd)"
V21="$(cd "$HERE/../../v2.1" && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}" # testdata/bench/v2.2
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v22.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
EFS="${EFS:-160 80}"
T="${T:-360}" # the calibration of v2.0, where the rule starts
SUFFIX=""
[ "$T" = 360 ] || SUFFIX="-t$T"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=18GiB # three processes on 64 GB

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
    "$@" ${EXTRA:-} -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
  date "+== %H:%M $label: $(tail -1 "$WORK/$label.log")"
  grep -E "checkpoint|guard|build work|stop and report|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-420
}

if [ "${BUILDWORK:-1}" = 1 ]; then
  stream "dbpedia-1m-batch-buildwork" -bench.stream buildwork &
fi
for EF in $EFS; do
  stream "dbpedia-1m-grow10-h1-ef$EF$SUFFIX" -bench.stream h1 -bench.streamstart 10 -bench.streamthreshold "$T" \
    -bench.streaminsertef "$EF" -bench.streamcheckpoints 20,40,60,80,100 &
done
wait

python3 -I -B "$HERE/growth_verdict.py" "$OUT" "$V20" "$V21" "$SUFFIX" > "$OUT/growth-verdict$SUFFIX.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/growth-verdict$SUFFIX.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
