#!/bin/bash
# Day run of 2026-11-13, nothing timed: the redundant edges each search meets
# on the 10M build (bench-report.md, v1.6, "Redundancia por query"), at ef 20
# and 160, the first points of the rescoreLimit 200 curve at recall 0.95 and
# 0.99.
#
# usage: WORK=<dir> caffeinate -s -i run_v16_edgetrace.sh
#   WORK holds postingedge-v16t.test, a binary with -bench.edgetrace:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v16t.test ./adapters/repos/db/vector/postingedge/
#   The night binary, postingedge-v16.test, is not rebuilt for it. The night
#   chain waits for this one, whose name it matches, to end.
set -u
source "$(cd "$(dirname "$0")" && pwd)/v16_lib.sh"
PE="$WORK/postingedge-v16t.test"

date "+== %Y-%m-%d %H:%M start"

label=wikidpr-10m-edge-trace
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D10M" -bench.arm postingedge -bench.queries 1000 "${PE10M[@]}" -bench.edgetrace \
  -bench.sweep "ef=20,160 threads=1 rescore=200 batch=4 margin=0.06 timed=0" \
  -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label

date "+== %H:%M end"
