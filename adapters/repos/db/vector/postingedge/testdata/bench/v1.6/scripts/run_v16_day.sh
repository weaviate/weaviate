#!/bin/bash
# Day runs of 2026-11-13, nothing timed:
#
#  1. Edge redundancy on the 10M index (the v1.4 default): per vector, its
#     edges against the distinct postings they lead to, and the edges that
#     share their target posting with a nearer edge of the same vector.
#  2. The same on the 1M index, for the change with N.
#  3. The distances of HNSW at 10M (TestSearchWork), which the kernel estimate
#     of 2026-11-12 took from 1M.
#
# usage: WORK=<dir> caffeinate -s -i run_v16_day.sh
set -u
source "$(cd "$(dirname "$0")" && pwd)/v16_lib.sh"
UNTIMED="ef=20 threads=1 rescore=200 batch=4 margin=0.06 timed=0"

date "+== %Y-%m-%d %H:%M start"

label=wikidpr-10m-edge-redundancy
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D10M" -bench.arm postingedge -bench.queries 1000 "${PE10M[@]}" \
  -bench.sweep "$UNTIMED" -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label

label=wikidpr-1m-edge-redundancy
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D1M" -bench.arm postingedge -bench.queries 1000 \
  -bench.posting 236 -bench.probe 256 -bench.codebits 0 -bench.rescue 64 -bench.workthreads 8 -bench.buildcache "$CACHE" \
  -bench.sweep "$UNTIMED" -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label

label=wikidpr-10m-hnsw-work
date "+== %H:%M start $label"
"$HW" -test.run 'TestSearchWork$' -test.timeout 0 -test.v \
  -work.dataset "$D10M" -work.queries 1000 -work.rescores 512 \
  -work.efs 10,16,24,32,48,64,96,128,192,256,384,512,768,1024 \
  -work.out "$KERNEL/$label.json" > "$WORK/$label.log" 2>&1
summary $label

date "+== %H:%M end"
