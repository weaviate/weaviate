#!/bin/bash
# The night of 10M grown by inserts (bench-report.md of feat/posting-edge-index,
# "10M crecido por inserts, una noche"): grow, then the heap of what it left in
# another process, then the power log. Run under caffeinate -i -s. The binary
# is built before, once ($N/night.test); this script never builds.
set -u
S=<scratch>
N=$S/night10m
B=$N/night.test
D=$HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin
cd $S/night/adapters/repos/db/vector/postingedge || exit 2
date "+== %Y-%m-%d %H:%M:%S grow" | tee $N/start.txt
PE_DATASET=$D PE_DIR=$N/index PE_OUT=$N/grow.json PE_CALLERS=8 PE_STRETCH=300 PE_MINFREE=8 \
  $B -test.run '^TestNightGrow$' -test.v -test.timeout 0 > $N/grow.log 2>&1
echo "grow exit $?"
date "+== %H:%M:%S heap routing"
PE_DIR=$N/index PE_CACHE=routing PE_HEAP=$N/heap-routing.heap PE_OUT=$N/heap-routing.json \
  $B -test.run '^TestNightHeap$' -test.v -test.timeout 0 -test.memprofilerate=1 > $N/heap-routing.log 2>&1
echo "heap exit $?"
go tool pprof -raw $B $N/heap-routing.heap 2>/dev/null > $N/heap-routing.raw
echo "raw exit $?"
date "+== %Y-%m-%d %H:%M:%S done" | tee $N/end.txt
pmset -g log > $N/pmset.log 2>&1
echo "pmset exit $?"
