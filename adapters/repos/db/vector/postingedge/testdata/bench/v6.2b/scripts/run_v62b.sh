#!/bin/bash
# v6.2b (bench-report.md, "v6.2b: churn largo y borrado por regiones"): the
# cells B and C of v6.2 over a churn of thirty rounds of 3 %, measured after
# rounds 10, 20 and 30 against the batch index of the vectors live then, and
# over 30 % of dbpedia 1M deleted by regions. Counts: nothing is timed. It
# runs by day, and never next to a timed session.
#
# usage: run_v62b.sh [stage ...]     (default: bin run verdict)
#
#   bin      builds the benchmark binary into $WORK.
#   run      runs the stages of $POINTS, in order, and writes
#            $OUT/dbpedia-1m-long.json after every measure.
#   verdict  writes the tables to $OUT/long-verdict.md.
#
# environment:
#   DATASET  the dataset directory.
#   POINTS   the stages. Default: memorylong,memoryclustered.
#   ROUNDS   the rounds of the long churn. Default: 30.
#   MEASURE  the rounds after which it is measured. Default: 10,20,30.
#   BASE     the vectors of its base, of every 990 of the train set. Default: 520.
#   CELLS    the cells of v6.2 it runs. Default: B,C.
#   UNTIL    the time of day, as 15:04, from which no stage starts. Default: none.
#   OUT      where the results and the verdict go. Default: testdata/bench/v6.2b.
#   WORK     where the binary and the log go. Default: $OUT/work.
#   QUERIES  the queries. Default: 5000.
#   EXTRA    more flags for the binary. A smoke: EXTRA="-bench.limit 30000" QUERIES=500
#
# It must not be edited while it runs.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
POINTS="${POINTS:-memorylong,memoryclustered}"
ROUNDS="${ROUNDS:-30}"
MEASURE="${MEASURE:-10,20,30}"
BASE="${BASE:-520}"
CELLS="${CELLS:-B,C}"
UNTIL="${UNTIL:-}"
# Not the name of a timed binary: the timed sessions wait for those.
PE="$WORK/pe-long.test"
LABEL="dbpedia-1m-long"
mkdir -p "$WORK" "$OUT" || exit 1

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
}

stage_run() {
  date "+== %Y-%m-%d %H:%M the run starts: $POINTS, cells $CELLS, $ROUNDS rounds measured after $MEASURE${UNTIL:+, no point starts from $UNTIL}"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestStreamDeletes$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.streamthreshold 360 -bench.streaminsertef 160 -bench.deletes "$POINTS" -bench.memoryrounds "$ROUNDS" \
    -bench.memorymeasure "$MEASURE" -bench.memorybase "$BASE" -bench.memorycells "$CELLS" \
    ${UNTIL:+-bench.deleteuntil "$UNTIL"} -bench.out "$OUT/$LABEL.json" ${EXTRA:-} > "$WORK/$LABEL.log" 2>&1)
  grep -E "bench_delete_test|STOP" "$WORK/$LABEL.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-520
  [ "$(tail -1 "$WORK/$LABEL.log")" = PASS ] || { echo "== the run did not pass: see $WORK/$LABEL.log"; exit 1; }
  date "+== %Y-%m-%d %H:%M the run ends"
}

stage_verdict() {
  python3 -I -B "$HERE/long_verdict.py" "$OUT" > "$OUT/long-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/long-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin run verdict
date "+== %Y-%m-%d %H:%M start: $*"
for stage in "$@"; do
  case "$stage" in
    bin|run|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
