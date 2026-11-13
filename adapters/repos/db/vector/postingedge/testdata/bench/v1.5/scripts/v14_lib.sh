# Sourced by the v1.4 chains: the posting-edge index and HNSW on wiki-dpr e5
# (768 dimensions), at 1M and at 10M vectors.
#
# Every run uses one prebuilt test binary, built with go test -c next to these
# scripts, so that nothing compiled later reaches a chain that is still running,
# and keeps the partition and the assignment of a posting-edge build in $CACHE
# for the runs that share them.
S="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$S/postingedge-v14d.test"
OUT="$(cd "$S/../../v1.4" && pwd)"  # testdata/bench/v1.4, from testdata/bench/v1.5/scripts
CACHE="$HOME/Documents/datasets/postingedge-buildcache"
D1M="$HOME/Documents/datasets/wikidpr-1M-e5.bin"
D10M="$HOME/Documents/datasets/wikidpr-10m-e5.bin"
GRID="ef=10,20,40,80,160,320,640,1024"
mkdir -p "$OUT" "$CACHE"
cd "$(dirname "$OUT")/../.." || exit 1
# A soft limit well above what a 10M build holds (about 45 GB), so that the
# collector steps in before the machine swaps if garbage piles up.
export GOMEMLIMIT=54GiB

# usage: pe_cell <label> <dataset> <codebits> <rescue> <minedgerecall> <sweep>...
# PROBE is the build probe (256 unless set), CACHEPROBE the build probe of the
# cached assignment to take where that is a wider one.
pe_cell() {
  local label=$1 dataset=$2 codebits=$3 rescue=$4 minedge=$5
  shift 5
  local sweeps=()
  for sw in "$@"; do sweeps+=(-bench.sweep "$sw"); done
  date "+== %H:%M start $label"
  "$BIN" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
    -bench.dataset "$dataset" -bench.arm postingedge -bench.queries 1000 -bench.runs 3 -bench.repeat \
    -bench.posting 236 -bench.probe "${PROBE:-256}" -bench.cacheprobe "${CACHEPROBE:-0}" \
    -bench.codebits "$codebits" -bench.rescue "$rescue" \
    -bench.minedgerecall "$minedge" -bench.workthreads 8 -bench.ceiling 0.99 \
    -bench.streamtrain -bench.buildcache "$CACHE" \
    "${sweeps[@]}" -bench.out "$OUT/$label.json" > "$S/$label.log" 2>&1
  echo "== $label: $(tail -1 "$S/$label.log")"
  grep -E "bench_test.go.*(build:|memory:|coverage|diagnostics|partition and|train set|calibration: .*auto|ceiling|ef=)|FAIL|panic|fatal" "$S/$label.log" \
    | sed -E 's/.*bench_test.go:[0-9]+: //; s/ p50=[^|]*//; s/\| rounds ([0-9.]+) \(p95 ([0-9]+)\) loads ([0-9.\/]+).*rescored ([0-9]+) \(([^)]*)\).*/| rounds \1 p95 \2 loads \3 rescored \4 (\5)/' | cut -c1-600
}

# usage: hnsw_cell <label> <dataset>
hnsw_cell() {
  date "+== %H:%M start $1"
  "$BIN" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
    -bench.dataset "$2" -bench.arm hnsw -bench.queries 1000 -bench.runs 3 -bench.repeat \
    -bench.efs 10,16,24,32,48,64,96,128,192,256,384,512,768,1024 \
    -bench.sweep "rescore=512 threads=1,8" \
    -bench.sweep "rescore=64 threads=1,8 ef=96,128,192,256,384,512,768,1024" \
    -bench.out "$OUT/$1.json" > "$S/$1.log" 2>&1
  echo "== $1: $(tail -1 "$S/$1.log")"
  grep -E 'bench_test.go.*(build|memory|ef=)|FAIL|panic|fatal' "$S/$1.log" | sed -E 's/.*bench_test.go:[0-9]+: //' | cut -c1-200
}

# usage: edge_grid <label> <dataset> <codebits list> <rescues list>
# PROBES are the build probes of the grid (64,128,256 unless set): the widest
# is the one the assignment is computed and cached for.
edge_grid() {
  date "+== %H:%M start $1"
  "$BIN" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
    -bench.dataset "$2" -bench.arm postingedge -bench.mode edgerecall -bench.queries 1000 \
    -bench.posting 236 -bench.codebitslist "$3" -bench.probes "${PROBES:-64,128,256}" -bench.rescues "$4" \
    -bench.buildcache "$CACHE" -bench.out "$OUT/$1.json" > "$S/$1.log" 2>&1
  echo "== $1: $(tail -1 "$S/$1.log")"
  grep -E 'bench_test.go.*(code bits|postings of|partition and)|FAIL|panic|fatal' "$S/$1.log" | sed -E 's/.*bench_test.go:[0-9]+: //' | cut -c1-200
}
