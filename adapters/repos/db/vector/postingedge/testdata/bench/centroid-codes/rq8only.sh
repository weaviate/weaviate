#!/bin/bash
# RQ8 under the same conditions as crq.sh (bench-report.md, "CRQ4 y CRQ1 para los
# centroides", e4feb462af, which opens each cell in RQ8, CRQ4 and CRQ1): fresh APFS
# clones of the float32 cells, opened with codes alone in a fresh process, 8 bits,
# the same binary as crq.sh.
set -u
S=/private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad
P=/Users/abdel/Documents/Projects/hnsw+pq/wt-posting-edge-index/adapters/repos/db/vector/postingedge
B=$S/heap/pe-crq.test
mkdir -p /Volumes/v40bench/codes/rq8b
[ -e /Volumes/v40bench/codes/rq8b/v40 ] || cp -c -R /Volumes/v40bench/v40 /Volumes/v40bench/codes/rq8b/v40
[ -e /Volumes/v40bench/codes/rq8b/tenm ] || cp -c -R /Volumes/v40bench/tenm /Volumes/v40bench/codes/rq8b/tenm
run() { # name dataset queries diskdir manifest cell
  date "+== %H:%M:%S $1"
  (cd $S/integ/wt/adapters/repos/db/vector/postingedge && $B -test.run 'TestDiskCentroidCodes$' -test.v -test.timeout 0 \
    -bench.codes.only \
    -bench.dataset "$2" -bench.queries "$3" -bench.diskdir "$4" -bench.diskmanifest "$5" -bench.diskcell "$6" \
    -bench.out $S/codes/$1.json > $S/codes/$1.log 2>&1)
  echo "$1 exit $?"; grep -E 'cell .*(recall|heap in use)|FAIL|PASS|panic|Error' $S/codes/$1.log | sed 's/^ *//' | cut -c1-220
}
D1=$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin; M1=$P/testdata/bench/v4.0/dbpedia-1m-disk-manifest.json
D10=$HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin; M10=$P/testdata/bench/v4.0/10m/wikidpr-10m-manifest.json
run rq8-v40-r512 $D1 5000 /Volumes/v40bench/codes/rq8b/v40/pe $M1 r512
run rq8-v40-halo512 $D1 5000 /Volumes/v40bench/codes/rq8b/v40/pe $M1 halo512
run rq8-tenm-r512 $D10 1000 /Volumes/v40bench/codes/rq8b/tenm/pe $M10 r512
run rq8-tenm-halo512 $D10 1000 /Volumes/v40bench/codes/rq8b/tenm/pe $M10 halo512
date "+== %H:%M:%S done"
