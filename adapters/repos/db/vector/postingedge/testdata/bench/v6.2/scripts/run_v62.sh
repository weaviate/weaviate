#!/bin/bash
# v6.2 (bench-report.md, "v6.2: reparación en el churn"): the maintenance
# pass of v6.1 over dbpedia 1M with three repairs, A, B and C, with 30 %
# deleted at once and in the churn, each cell over a copy of its own. Counts:
# nothing is timed, and it runs by day.
#
# usage: run_v62.sh [stage ...]     (default: bin run verdict)
#
#   bin      builds the benchmark binary into $WORK.
#   run      runs the stages of $POINTS, in order, and writes
#            $OUT/dbpedia-1m-memory.json after every cell.
#   verdict  writes the tables to $OUT/memory-verdict.md.
#
# environment:
#   DATASET  the dataset directory.
#   POINTS   the stages. Default: memory30,memorychurn.
#   UNTIL    the time of day, as 15:04, from which no stage starts. Default: none.
#   OUT      where the results and the verdict go. Default: testdata/bench/v6.2.
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
POINTS="${POINTS:-memory30,memorychurn}"
UNTIL="${UNTIL:-}"
# Not the name of a timed binary: the timed sessions wait for those.
PE="$WORK/pe-memory.test"
LABEL="dbpedia-1m-memory"
mkdir -p "$WORK" "$OUT" || exit 1

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
}

stage_run() {
  date "+== %Y-%m-%d %H:%M the run starts: $POINTS${UNTIL:+, no point starts from $UNTIL}"
  # -bench.deletefill: every stage builds besides the batch indexes with the
  # posting sizes its cells leave.
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestStreamDeletes$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.streamthreshold 360 -bench.streaminsertef 160 -bench.deletes "$POINTS" -bench.deletefill \
    ${UNTIL:+-bench.deleteuntil "$UNTIL"} -bench.out "$OUT/$LABEL.json" ${EXTRA:-} > "$WORK/$LABEL.log" 2>&1)
  grep -E "bench_delete_test|STOP" "$WORK/$LABEL.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-520
  [ "$(tail -1 "$WORK/$LABEL.log")" = PASS ] || { echo "== the run did not pass: see $WORK/$LABEL.log"; exit 1; }
  date "+== %Y-%m-%d %H:%M the run ends"
}

stage_verdict() {
  python3 -I -B "$HERE/memory_verdict.py" "$OUT" > "$OUT/memory-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/memory-verdict.md"
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
