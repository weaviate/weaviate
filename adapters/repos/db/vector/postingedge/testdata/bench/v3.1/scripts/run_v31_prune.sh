#!/bin/bash
# v3.1 (pre-registered 2026-11-17): pruning the halo, on dbpedia-openai 1M
# built in batch (bench-report.md, "v3.1: podar el halo"). One build, that of
# v3.0 with S = 236: probe 256, rescue 64 and the full code. Over it, the
# reference of v3.0, which ranks the endpoints of the edges with the full
# code as the resident sidecar does, and, for the sketches of 384 and 512
# bits, a grid of twelve cells: four sets of edges (all of them, one per
# target posting, capped at 8, and both) searched with three source margins
# (0.04, 0.06 and 0.08). The cell with all the edges and margin 0.06 is the
# base, the cell of v3.0, measured again here. The scan keeps the full code
# in every cell. Each is searched over ef 10 to 2048, untimed, and what every
# search loads is counted block by block: members, edges and halo entries. A
# cell that does not reach recall 0.99 with the rescore limit of the index is
# swept again with 200.
#
# The ef grid is that of v3.0 with the midpoints up to 640, so that what is
# read at recall 0.95 and 0.99 is interpolated over half the step.
#
# It counts and times nothing, and does not wait for other benchmarks.
#
# At the end, the verdict into prune-verdict.md. Every cell is compared with
# the base of its width, inside the run.
#
# usage: WORK=<dir> caffeinate -s -i run_v31_prune.sh
#   WORK holds postingedge-v31.test, built before the run:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v31.test ./adapters/repos/db/vector/postingedge/
#   For a smoke: OUT=<dir> EXTRA="-bench.limit 30000 -bench.queries 500"
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}" # testdata/bench/v3.1
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v31.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
WIDTHS="${WIDTHS:-384,512}"
MARGINS="${MARGINS:-0.04,0.06,0.08}"
EFS="${EFS:-10,14,20,28,40,56,80,113,160,226,320,452,640,1024,2048}"
V30="${V30:-$HERE/../../v3.0}" # the run of v3.0 the build is checked against
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=20GiB

START="$(date '+%Y-%m-%d %H:%M:%S')"
date "+== %Y-%m-%d %H:%M start"
power_state

label="dbpedia-1m-haloprune-s236"
date "+== %H:%M start $label, sketches of $WIDTHS bits, margins $MARGINS, cap 8"
# shellcheck disable=SC2086
"$PE" -test.run 'TestHaloPrune$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000 \
  -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.haloprune "$WIDTHS" -bench.halomargins "$MARGINS" \
  -bench.halocap 8 -bench.haloefs "$EFS" \
  ${EXTRA:-} -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
date "+== %H:%M $label: $(tail -1 "$WORK/$label.log")"
grep -E "built|edges [A-Za-z0-9]+: |sketch of [0-9]+ bits: |gate|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-300

python3 -I -B "$HERE/prune_verdict.py" "$OUT" "$V30" > "$OUT/prune-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/prune-verdict.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
