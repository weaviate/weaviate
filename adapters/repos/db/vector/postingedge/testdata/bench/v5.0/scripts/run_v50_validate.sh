#!/bin/bash
# v5.0 (pre-registered 2026-11-18): the validation of the inserts on disk
# (bench-report.md, "v5.0: inserts en disco"), untimed, on macOS and on Linux.
#
# An index is built in batch over the first tenth of dbpedia-openai 1M,
# shuffled as in v2.x, written in the format that grows, and grown to the
# whole train set by inserts that read and write the store. It grows next to
# its twin in memory. At every checkpoint the coverage is checked from the
# store and the index is compared with the twin: either failing stops the
# run. The result has what an insert reads and writes, the size of the store,
# and what a search reads from it before and after it is compacted. The
# store compacts by levels while the index grows, its default, so that what
# was replaced piles up as it would without the forced compaction of HFresh's
# postings bucket (the second amendment); FORCE=1 gives it that one.
#
# usage: [VAR=value ...] run_v50_validate.sh
#   DATASET  the dataset directory (meta.txt, train.f32, test.f32,
#            neighbors.i32). Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     the directory the index grows in, on the disk under test, with
#            some 8 GB free. It must not exist, or be empty.
#   OUT      where the result goes. Default: testdata/bench/v5.0.
#   WORK     where the binary and the log go. Default: $OUT/work.
#   EXTRA    more flags for the binary. A smoke:
#            EXTRA="-bench.limit 30000" QUERIES=500
#   QUERIES  the queries the searches are measured over. Default 5000.
#   FORCE    1 compacts the bucket of postings as HFresh's. Default 0.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
FORCE="${FORCE:-0}"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the index grows in"; exit 1; }
PE="$WORK/postingedge-v50.test"
LABEL="dbpedia-1m-grow"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-40GiB}"

date "+== %Y-%m-%d %H:%M building the binary"
(cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1

date "+== %Y-%m-%d %H:%M the validation starts: $DISK"
# shellcheck disable=SC2086
(cd "$PKG" && "$PE" -test.run 'TestGrowValidate$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
  -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
  -bench.growdir "$DISK" -bench.growforce="$([ "$FORCE" = 1 ] && echo true || echo false)" \
  -bench.out "$OUT/$LABEL-validate.json" ${EXTRA:-} > "$WORK/$LABEL-validate.log" 2>&1)
grep -E "bench_grow_test|STOP" "$WORK/$LABEL-validate.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-600
if [ "$(tail -1 "$WORK/$LABEL-validate.log")" = PASS ]; then
  date "+== %Y-%m-%d %H:%M the validation passed"
else
  date "+== %Y-%m-%d %H:%M the validation did NOT pass: see $WORK/$LABEL-validate.log"
  exit 1
fi
