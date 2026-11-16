#!/bin/bash
# The closing check of the centroid ef (2026-11-16), untimed: centroid efs 8
# and 4 against the default 64 at 1M on dbpedia-openai (1536d) and wiki-dpr e5
# (768d), each with its default build and rescoreLimit, at the first efs of its
# ef curve that reach 0.95 and 0.99. If neither dataset loses more than 0.0005
# of recall at any of those points with ef 8, the default goes to 8
# (bench-report.md, v1.6, "El ef de centroides: cierre a 1M").
#
# usage: WORK=<dir> caffeinate -i run_v16_centroidef_1m.sh
#   WORK holds postingedge-v16c.test, a binary of the branch head:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v16c.test ./adapters/repos/db/vector/postingedge/
#
# Nothing here is timed: the plan of each run is timed=0, and the centroid-ef
# walk measures its points untimed, so the walk's "timed plan" of the last ef
# that keeps recall is the same untimed points again.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/v16_lib.sh"
PE="$WORK/postingedge-v16c.test"
DBP="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }

# dbpedia 1M, the v1.2 default: S=236, P=256, R=64, full code, rescoreLimit 64,
# 5000 queries. Its curve reaches 0.95 at ef=20 (0.9502) and 0.99 at ef=320
# (0.9908) (v1.2/dbpedia-1m-posting-236-batches.json).
label=dbpedia-1m-centroidef
date "+== %Y-%m-%d %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$DBP" -bench.arm postingedge -bench.queries 5000 \
  -bench.posting 236 -bench.probe 256 -bench.codebits 0 -bench.rescue 64 -bench.workthreads 8 \
  -bench.sweep "ef=20,320 threads=1 rescore=64 batch=4 margin=0.06 timed=0" \
  -bench.centroidefs 8,4 -bench.centroidpoints 20,320 -bench.centroidmaxdrop 0.0005 \
  -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label

# wiki-dpr 1M, the v1.4 default at 1M: S=236, P=256 over the cached
# assignment, R=64, full code, rescoreLimit 200, 1000 queries. Its curve
# reaches 0.95 at ef=40 (0.9664) and 0.99 at ef=160 (0.9904)
# (v1.4/wikidpr-1m-full-timed.json).
label=wikidpr-1m-centroidef
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D1M" -bench.arm postingedge -bench.queries 1000 \
  -bench.posting 236 -bench.probe 256 -bench.codebits 0 -bench.rescue 64 -bench.workthreads 8 -bench.buildcache "$CACHE" \
  -bench.sweep "ef=40,160 threads=1 rescore=200 batch=4 margin=0.06 timed=0" \
  -bench.centroidefs 8,4 -bench.centroidpoints 40,160 -bench.centroidmaxdrop 0.0005 \
  -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label
date "+== %H:%M end"
