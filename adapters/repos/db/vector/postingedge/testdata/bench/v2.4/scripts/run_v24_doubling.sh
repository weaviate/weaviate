#!/bin/bash
# v2.4 (pre-registered 2026-11-17): the edge refresh on doubling, during the
# growth, on dbpedia-openai 1M (bench-report.md, "v2.4: refresco al doblar, en
# línea"). The growth cell of v2.2 and v2.3 with insert ef 80: a batch build
# over the first 10 % of the train set and the other 90 % inserted with
# inverse edges. When the index reaches 200 000, 400 000 and 800 000 vectors,
# every vector whose edges were last built with half that many vectors or
# fewer has them refreshed with the search of an insert, the oldest first:
# 700 000 refreshes, and none at the end. The cell is measured before and
# after each round and at 100 %, and then with its edges rebuilt in batch
# over the same partition, the reference inside the run.
#
# Informative, and after all of that is written: the edges of the cell at
# 100 % are put back, every vector is refreshed as in v2.3, and the cell is
# measured as that leaves it and with every list as its own refresh wrote it;
# a sample of vectors is followed through the refresh and the batch rule. It
# counts and times nothing.
#
# At the end, the verdict into doubling-verdict.md, against the batch
# reference of v2.0 and the counts of v2.3. Nothing at 0.99 is compared with
# another run.
#
# usage: WORK=<dir> caffeinate -s -i run_v24_doubling.sh
#   WORK holds postingedge-v24.test, built before the run:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v24.test ./adapters/repos/db/vector/postingedge/
#   For a smoke: OUT=<dir> ROUNDS=6000,12000,24000 WHY=1000 EXTRA="-bench.limit 30000 -bench.queries 500"
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
V20="$(cd "$HERE/../../v2.0" && pwd)"
V23="$(cd "$HERE/../../v2.3" && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}" # testdata/bench/v2.4
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v24.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
EF="${EF:-80}"
ROUNDS="${ROUNDS:-200000,400000,800000}"
WHY="${WHY:-10000}"
LABEL="dbpedia-1m-grow10-h1-ef$EF-doubling"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=24GiB # one process on 64 GB

date "+== %Y-%m-%d %H:%M waiting for no other benchmark"
while others || pgrep -f 'postingedge-(p2|v2[0-9][a-z]?)\.test' > /dev/null; do sleep 60; done
START="$(date '+%Y-%m-%d %H:%M:%S')"
power_state
echo "== threshold 360, insert ef $EF, a refresh round at $ROUNDS vectors, the edges rebuilt in batch, and $WHY vectors sampled"

date "+== %H:%M start $LABEL"
# shellcheck disable=SC2086
"$PE" -test.run 'TestStream$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000 \
  -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.workthreads 8 \
  -bench.stream h1 -bench.streamstart 10 -bench.streamthreshold 360 -bench.streaminsertef "$EF" \
  -bench.streamcheckpoints 100 -bench.streamdoubling "$ROUNDS" \
  -bench.streamrebuild -bench.streamrebuildgraph=false -bench.streamwhy "$WHY" \
  ${EXTRA:-} -bench.out "$OUT/$LABEL.json" > "$WORK/$LABEL.log" 2>&1
date "+== %H:%M $LABEL: $(tail -1 "$WORK/$LABEL.log")"
grep -E "checkpoint|corpus [0-9]+|rebuilt|why:|guard|stop and report|FAIL|panic" "$WORK/$LABEL.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-420

python3 -I -B "$HERE/doubling_verdict.py" "$OUT" "$V20" "$V23" "$EF" > "$OUT/doubling-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/doubling-verdict.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
