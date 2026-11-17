#!/bin/bash
# v3.0 (pre-registered 2026-11-17): the operating point of the halo, on
# dbpedia-openai 1M built in batch (bench-report.md, "v3.0: punto de operación
# del halo"). One build per posting size, S = 236 with probe 256 and S = 1024
# with probe 112, with the full code. Over each, the reference, which ranks
# the endpoints of the edges with the full code as the resident sidecar does,
# and three cells that rank them with a sketch of 256, 384 and 512 bits, which
# is what a posting would carry for every distinct endpoint of its edges if
# there were no sidecar. The scan keeps the full code in every cell. Each is
# searched over ef 10 to 2048, untimed, and what every search loads is counted
# block by block: members, edges and halo entries. A cell that does not reach
# recall 0.99 with the rescore limit of the index is swept again with 200.
#
# It counts and times nothing, and does not wait for other benchmarks: it may
# run next to a chain that only counts.
#
# At the end, the verdict into halo-verdict.md. Every cell is compared with
# the reference of its own build, inside the run.
#
# usage: WORK=<dir> caffeinate -s -i run_v30_halo.sh
#   WORK holds postingedge-v30.test, built before the run:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v30.test ./adapters/repos/db/vector/postingedge/
#   For a smoke: OUT=<dir> EXTRA="-bench.limit 30000 -bench.queries 500"
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}" # testdata/bench/v3.0
source "$HERE/../../v1.6/scripts/v16_queue.sh"
WORK="${WORK:-$HERE}"
PE="$WORK/postingedge-v30.test"
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
WIDTHS="${WIDTHS:-256,384,512}"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }
cd "$HERE/../../../.." || exit 1 # the package directory
export GOMEMLIMIT=20GiB

START="$(date '+%Y-%m-%d %H:%M:%S')"
date "+== %Y-%m-%d %H:%M start"
power_state

# cell <posting size> <build probe>
cell() {
  local label="dbpedia-1m-halo-s$1"
  date "+== %H:%M start $label, probe $2, sketches of $WIDTHS bits"
  # shellcheck disable=SC2086
  "$PE" -test.run 'TestHalo$' -test.timeout 0 -test.v -bench.dataset "$D" -bench.queries 5000 \
    -bench.posting "$1" -bench.probe "$2" -bench.rescue 64 -bench.halo "$WIDTHS" \
    ${EXTRA:-} -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
  date "+== %H:%M $label: $(tail -1 "$WORK/$label.log")"
  grep -E "built|layout:|sketch of|reference|gate|FAIL|panic" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-300
}
cell 236 256
cell 1024 112

python3 -I -B "$HERE/halo_verdict.py" "$OUT" > "$OUT/halo-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/halo-verdict.md"
date "+== %H:%M end"
power_state
power_log_since "$START"
