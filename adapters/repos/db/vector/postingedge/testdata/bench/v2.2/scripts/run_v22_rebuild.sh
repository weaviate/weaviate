#!/bin/bash
# v2.2, the diagnostic (pre-registered 2026-11-17): the edges or the
# partition, on dbpedia-openai 1M (bench-report.md, "Diagnóstico: aristas o
# partición"). The cell of insert ef 80 of v2.2 once more, as it ran: a batch
# build over the first 10 % of the train set and the other 90 % inserted with
# inverse edges, with checkpoints at 20, 40, 60, 80 and 100 %. At the end the
# edges of every vector are rebuilt in batch over the partition as it is and
# the cell is measured again, and then the centroid graph is rebuilt too and
# it is measured a third time. It counts and times nothing.
#
# At the end, the verdict into rebuild-verdict.md, against the batch
# reference of v2.0 and the first run of the cell.
#
# usage: WORK=<dir> caffeinate -s -i run_v22_rebuild.sh
#   WORK holds postingedge-v22d.test, built before the run:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v22d.test ./adapters/repos/db/vector/postingedge/
#   For a smoke: OUT=<dir> EXTRA="-bench.limit 30000 -bench.queries 500 ..."
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
V20="$(cd "$HERE/../../v2.0" && pwd)"
V22="$(cd "$HERE/.." && pwd)"
OUT="${OUT:-$V22}" # testdata/bench/v2.2
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v22d.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
EF="${EF:-80}"
LABEL="dbpedia-1m-grow10-h1-ef$EF-run2"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=24GiB # one process on 64 GB

date "+== %Y-%m-%d %H:%M waiting for no other benchmark"
while others || pgrep -f 'postingedge-(p2|v2[0-9][a-z]?)\.test' > /dev/null; do sleep 60; done
START="$(date '+%Y-%m-%d %H:%M:%S')"
power_state
echo "== threshold 360, insert ef $EF, edges and centroid graph rebuilt at the end"

date "+== %H:%M start $LABEL"
# shellcheck disable=SC2086
"$PE" -test.run 'TestStream$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000 \
  -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.workthreads 8 \
  -bench.stream h1 -bench.streamstart 10 -bench.streamthreshold 360 -bench.streaminsertef "$EF" \
  -bench.streamcheckpoints 20,40,60,80,100 -bench.streamrebuild \
  ${EXTRA:-} -bench.out "$OUT/$LABEL.json" > "$WORK/$LABEL.log" 2>&1
date "+== %H:%M $LABEL: $(tail -1 "$WORK/$LABEL.log")"
grep -E "checkpoint|rebuilt|guard|stop and report|FAIL|panic" "$WORK/$LABEL.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-420

python3 -I -B "$HERE/rebuild_verdict.py" "$OUT" "$V20" "$V22" "$EF" > "$OUT/rebuild-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/rebuild-verdict.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
