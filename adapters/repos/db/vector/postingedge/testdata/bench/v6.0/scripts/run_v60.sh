#!/bin/bash
# v6.0 (pre-registered 2026-11-19): deletes in memory, in counts
# (bench-report.md, "v6.0: deletes en memoria"). One run of the deletes
# experiment over the streaming index of v2.x: a batch index of the whole
# train set; for every point of deletes, a copy of it with its vectors
# marked, searched as each of the four cells next to a batch index of the
# vectors that are left; and a churn of deletes and inserts with the best
# cell. Nothing is timed.
#
# The stages, in this order when none is named:
#   bin      builds the benchmark binary into $WORK.
#   run      runs the points of $POINTS, in order, and writes the results to
#            $OUT/dbpedia-1m-deletes.json after every cell.
#   verdict  writes the tables to $OUT/deletes-verdict.md.
#
# usage: [VAR=value ...] run_v60.sh [stage ...]
#   DATASET  the dataset directory. Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   POINTS   the points to run. Default: random30,random10,random50,clustered30,churn
#   UNTIL    the time of day, as 15:04, from which no point starts. Default: none.
#   OUT      where the results and the verdict go. Default: testdata/bench/v6.0.
#   WORK     where the binary and the log go. Default: $OUT/work.
#   QUERIES  the queries of the search grid. Default 5000.
#   EXTRA    more flags for the binary. A smoke: EXTRA="-bench.limit 30000" QUERIES=500
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
POINTS="${POINTS:-random30,random10,random50,clustered30,churn}"
UNTIL="${UNTIL:-}"
PE="$WORK/postingedge-v60.test"
LABEL="dbpedia-1m-deletes"
mkdir -p "$WORK" "$OUT" || exit 1

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
}

stage_run() {
  date "+== %Y-%m-%d %H:%M the run starts: $POINTS${UNTIL:+, no point starts from $UNTIL}"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestStreamDeletes$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.streamthreshold 360 -bench.streaminsertef 160 -bench.deletes "$POINTS" ${UNTIL:+-bench.deleteuntil "$UNTIL"} \
    -bench.out "$OUT/$LABEL.json" ${EXTRA:-} > "$WORK/$LABEL.log" 2>&1)
  grep -E "bench_delete_test|STOP" "$WORK/$LABEL.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-420
  [ "$(tail -1 "$WORK/$LABEL.log")" = PASS ] || { echo "== the run did not pass: see $WORK/$LABEL.log"; exit 1; }
  date "+== %Y-%m-%d %H:%M the run ends"
}

stage_verdict() {
  python3 -I -B "$HERE/deletes_verdict.py" "$OUT" > "$OUT/deletes-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/deletes-verdict.md"
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
