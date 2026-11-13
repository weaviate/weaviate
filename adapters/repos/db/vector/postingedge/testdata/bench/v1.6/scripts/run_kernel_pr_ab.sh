#!/bin/bash
# Interleaved rounds of the kernel microbenchmarks, before and after the fused
# kernel of weaviate/weaviate#13880. "current_distance" is the distance of
# compressionhelpers, the one the index runs: five Hamming calls before, the
# kernel of the PR after. Every round times both binaries, in alternating
# order, so that a slow stretch of the machine lands on both.
#
# usage: WORK=<dir> run_kernel_pr_ab.sh <rounds> <out>
#   WORK holds kernelproto-before.test (built at e94ace811c) and
#   kernelproto-after.test (built at d763bb23fc or later).
set -u
WORK="${WORK:-$(cd "$(dirname "$0")" && pwd)}"
BENCH='BenchmarkKernel|BenchmarkPostingScan|BenchmarkScattered'
: > "$2"
for round in $(seq 1 "$1"); do
  order="before after"
  [ $((round % 2)) = 0 ] && order="after before"
  for side in $order; do
    date "+== round $round $side %H:%M:%S" >> "$2"
    "$WORK/kernelproto-$side.test" -test.run XXX -test.bench "$BENCH" \
      -test.benchtime 300ms -test.count 1 >> "$2" 2>&1
  done
done
date "+== end %H:%M:%S" >> "$2"
