#!/bin/bash
# The small test of the night's harness, with the same binary: 30 000 vectors
# in a directory of its own, and the heap of what it leaves. Not reported.
set -u
S=<scratch>
N=$S/night10m
B=$N/night.test
D=$HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin
cd $S/night/adapters/repos/db/vector/postingedge || exit 2
date "+== %H:%M:%S smoke grow"
PE_DATASET=$D PE_DIR=$N/smoke/index PE_OUT=$N/smoke/grow.json PE_N=30000 PE_CALLERS=8 PE_STRETCH=10 PE_MINFREE=8 \
  $B -test.run '^TestNightGrow$' -test.v -test.timeout 30m > $N/smoke/grow.log 2>&1
echo "grow exit $?"
PE_DIR=$N/smoke/index PE_CACHE=routing PE_HEAP=$N/smoke/heap.heap PE_OUT=$N/smoke/heap.json \
  $B -test.run '^TestNightHeap$' -test.v -test.timeout 30m -test.memprofilerate=1 > $N/smoke/heap.log 2>&1
echo "heap exit $?"
go tool pprof -raw $B $N/smoke/heap.heap 2>/dev/null > $N/smoke/heap.raw
echo "raw exit $?"
date "+== %H:%M:%S smoke done"
