#!/bin/bash
# Interleaved rounds of the kernel microbenchmarks: every round times every
# variant once, so that a slow stretch of the machine lands on all of them.
# usage: run_kernel_ab.sh <rounds> <out>
set -u
S="$(cd "$(dirname "$0")" && pwd)"
: > "$2"
for round in $(seq 1 "$1"); do
  date "+== round $round %H:%M:%S" >> "$2"
  "$S/kernelproto.test" -test.run XXX -test.bench 'BenchmarkKernel|BenchmarkPostingScan|BenchmarkScattered|BenchmarkQueryLayout' \
    -test.benchtime 300ms -test.count 1 >> "$2" 2>&1
done
date "+== end %H:%M:%S" >> "$2"
