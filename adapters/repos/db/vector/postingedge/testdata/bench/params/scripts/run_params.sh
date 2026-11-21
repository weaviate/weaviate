#!/bin/bash
# The two measures of the parameters (bench-report.md, "Parámetros: el `ef`
# dinámico y la regla del rescore"): TestHalo, the harness of v3.0, over one
# batch build, untimed, with the reference cell (the full code in RAM) and
# the cell that routes with codes of 512 bits.
#
# The chains, one or more by name; none named runs both, one after the other:
#   ef       dbpedia 1M, 5 000 queries: k = 1, k = 10 and k = 100 with the
#            default rescore limit, and k = 100 again with 200 and with 400.
#   rescore  wiki-dpr 1M at 768d, 1 000 queries, k = 10: the default rescore
#            limit, 64, next to 100, to 150 and to 200.
#
# usage: [VAR=value ...] run_params.sh [chain ...]
#   PE       the benchmark binary, built with the tag postingedgebench.
#   OUT      where the results go. Default: testdata/bench/params.
#   WORK     where the logs go. Default: $OUT/work.
#   LOCK     a file that, while it exists, keeps a run from starting: the
#            marker of a timed session.
#   EXTRA    more flags. A smoke: EXTRA="-bench.limit 30000"
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DBPEDIA="${DBPEDIA:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
WIKIDPR="${WIKIDPR:-$HOME/Documents/datasets/wikidpr-1m-e5-gt.bin}"
[ -n "${PE:-}" ] || { echo "== PE is not set: the benchmark binary"; exit 1; }
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"
MESH="10,12,16,20,24,32,48,64,96,128,192,256,384,512,768,1024"

# run <label> <dataset> <queries> <k> <efs> [flag ...]
run() {
  local label="$1" dataset="$2" queries="$3" k="$4" efs="$5" log
  shift 5
  log="$WORK/$label.log"
  while [ -n "${LOCK:-}" ] && [ -e "$LOCK" ]; do sleep 60; done
  date "+== %Y-%m-%d %H:%M $label"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestHalo$' -test.timeout 0 -test.v -bench.dataset "$dataset" -bench.queries "$queries" \
    -bench.k "$k" -bench.posting 236 -bench.probe 256 -bench.rescue 64 -bench.halo 512 -bench.haloefs "$efs" \
    -bench.out "$OUT/$label.json" "$@" ${EXTRA:-} > "$log" 2>&1)
  grep -E 'bench_halo_test' "$log" | sed -E 's/.*_test.go:[0-9]+: //' | grep -E 'layout|best recall|gate|sketch of' | cut -c1-300
  [ "$(tail -1 "$log")" = PASS ] || { echo "== $label did not pass: see $log"; return 1; }
}

chain_ef() {
  run dbpedia-1m-k1 "$DBPEDIA" 5000 1 "1,2,3,4,6,8,$MESH" -bench.halogate 0 || return 1
  run dbpedia-1m-k10 "$DBPEDIA" 5000 10 "$MESH" -bench.halogate 0 || return 1
  # A gate no cell reaches: the mesh is swept with the default and again with
  # the rescore limit given.
  run dbpedia-1m-k100-r200 "$DBPEDIA" 5000 100 "100,128,160,192,256,384,512,768,1024,1536,2048" \
    -bench.halogate 2 -bench.halorescore 200 || return 1
  run dbpedia-1m-k100-r400 "$DBPEDIA" 5000 100 "100,128,160,192,256,384,512,768,1024,1536,2048" \
    -bench.halogate 2 -bench.halorescore 400 || return 1
}

chain_rescore() {
  local r
  for r in 100 150 200; do
    run "wikidpr-1m-r$r" "$WIKIDPR" 1000 10 "$MESH,1536,2048" -bench.halogate 2 -bench.halorescore "$r" || return 1
  done
}

[ "$#" -gt 0 ] || set -- ef rescore
date "+== %Y-%m-%d %H:%M start: $*"
for chain in "$@"; do
  case "$chain" in
    ef|rescore) "chain_$chain" || exit 1 ;;
    *) echo "== unknown chain $chain"; exit 1 ;;
  esac
done
date "+== %Y-%m-%d %H:%M end"
