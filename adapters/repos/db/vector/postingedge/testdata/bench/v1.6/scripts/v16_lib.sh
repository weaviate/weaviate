# Sourced by the v1.6 chains (2026-11-13): wiki-dpr e5, 768 dimensions.
#
# WORK is where the prebuilt test binaries are and where the logs go (the
# directory of these scripts unless set). Every run uses a binary built before
# the chain, so that nothing compiled later reaches a chain that is running:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v16.test ./adapters/repos/db/vector/postingedge/
#   go test -tags postingedgebench -c -o $WORK/hnsw-work.test ./adapters/repos/db/vector/hnsw/
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK:-$HERE}"
OUT="$(cd "$HERE/.." && pwd)"              # testdata/bench/v1.6
KERNEL="$(cd "$HERE/../../kernel" && pwd)" # testdata/bench/kernel
PE="$WORK/postingedge-v16.test"
HW="$WORK/hnsw-work.test"
CACHE="$HOME/Documents/datasets/postingedge-buildcache"
D1M="$HOME/Documents/datasets/wikidpr-1m-e5-gt.bin"
D10M="$HOME/Documents/datasets/wikidpr-10m-e5.bin"
GRID="ef=10,20,40,80,160,320,640,1024"
# The posting-edge default at 10M (v1.4): S=236, P=320 over the cached
# assignment of P=384, R=256, full code.
PE10M=(-bench.posting 236 -bench.probe 320 -bench.cacheprobe 384 -bench.codebits 0 -bench.rescue 256
  -bench.minedgerecall 0.94 -bench.workthreads 8 -bench.streamtrain -bench.buildcache "$CACHE")
cd "$HERE/../../../.." || exit 1 # the package directory
# A soft limit well above what a 10M build holds, so that the collector steps
# in before the machine swaps if garbage piles up.
export GOMEMLIMIT=54GiB

# usage: summary <label>: the lines of the log worth reading in the chain's output
summary() {
  echo "== $1: $(tail -1 "$WORK/$1.log")"
  grep -E "(bench_test|search_work_bench_test).go.*(build|memory:|coverage|diagnostics|edge redundancy|centroid ef|ceiling|ef=)|FAIL|panic|fatal" "$WORK/$1.log" \
    | sed -E 's/.*_test.go:[0-9]+: //; s/ p50=[^|]*//; s/\| rounds ([0-9.]+) \(p95 ([0-9]+)\) loads ([0-9.\/]+).*endpoints ([0-9]+).*/| rounds \1 p95 \2 loads \3 endpoints \4/' \
    | cut -c1-400
}
