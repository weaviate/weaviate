#!/bin/bash
# Phase 2 (2026-11-16): what the endpoint scoring costs with the endpoint side
# keyed by doc id instead of by slot, at 10M on wiki-dpr e5 (disk-design.md,
# "Aristas por docID: el coste en memoria").
#
# One build of the v1.4 default at 10M, untimed; then the searches of 1000
# queries at ef=20 and ef=160 (the first efs at 0.95 and 0.99) are recorded,
# untimed, and their endpoint scoring is replayed and timed over three
# keyings: by slot, by doc id and by shuffled doc id (-bench.endpointkeys,
# endpoint_keys_test.go). Only the replays are timed, one thread.
#
# usage: WORK=<dir> caffeinate -s -i run_endpoint_keys.sh
#   WORK holds postingedge-p2.test, built before the run:
#   go test -tags postingedgebench -c -o $WORK/postingedge-p2.test ./adapters/repos/db/vector/postingedge/
#
# It waits for no other benchmark binary to be running, not for an idle
# machine: it runs in the day. The keyings are timed in interleaved rounds
# whose order rotates, so what a busy desktop adds lands on all of them, and
# the output keeps the keyboard idle time and the power log of the run.
# Nothing is committed to the repository while it runs.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$(cd "$HERE/.." && pwd)" # testdata/bench/phase2
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-p2.test"
CACHE="$HOME/Documents/datasets/postingedge-buildcache"
D10M="$HOME/Documents/datasets/wikidpr-10m-e5.bin"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=54GiB

date "+== %Y-%m-%d %H:%M waiting for no other benchmark"
while others; do sleep 60; done
START="$(date '+%Y-%m-%d %H:%M:%S')"
power_state

label=wikidpr-10m-endpointkeys
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D10M" -bench.arm postingedge -bench.queries 1000 \
  -bench.posting 236 -bench.probe 320 -bench.cacheprobe 384 -bench.codebits 0 -bench.rescue 256 \
  -bench.minedgerecall 0.94 -bench.workthreads 8 -bench.streamtrain -bench.buildcache "$CACHE" \
  -bench.sweep "ef=20,160 threads=1 rescore=200 batch=4 margin=0.06 timed=0" \
  -bench.endpointkeys -bench.endpointkeyrounds 9 \
  -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
echo "== $label: $(tail -1 "$WORK/$label.log")"
grep -E "build:|endpoint keys|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400
date "+== %H:%M end"
power_state
power_log_since "$START"
