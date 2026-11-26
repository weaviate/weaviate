#!/bin/bash
# CRQ4 and CRQ1 for the centroids (bench-report.md, "CRQ4 y CRQ1 para los
# centroides", e4feb462af), untimed: the same four cells from fresh APFS clones,
# opened with codes alone, centered, at 4 and at 1 bit, rescore 0.
set -u
S=/private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad
P=/Users/abdel/Documents/Projects/hnsw+pq/wt-posting-edge-index/adapters/repos/db/vector/postingedge
B=$S/heap/pe-crq.test
run() { # name bits dataset queries diskdir manifest cell
  date "+== %H:%M:%S $1"
  (cd $S/integ/wt/adapters/repos/db/vector/postingedge && $B -test.run 'TestDiskCentroidCodes$' -test.v -test.timeout 0 \
    -bench.codes.only -bench.codes.bits "$2" -bench.codes.centering \
    -bench.dataset "$3" -bench.queries "$4" -bench.diskdir "$5" -bench.diskmanifest "$6" -bench.diskcell "$7" \
    -bench.out $S/codes/$1.json > $S/codes/$1.log 2>&1)
  echo "$1 exit $?"; grep -E 'cell .*(recall|heap in use)|FAIL|PASS|panic|Error' $S/codes/$1.log | sed 's/^ *//' | cut -c1-220
}
D1=$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin; M1=$P/testdata/bench/v4.0/dbpedia-1m-disk-manifest.json
D10=$HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin; M10=$P/testdata/bench/v4.0/10m/wikidpr-10m-manifest.json
for b in 4 1; do
  run crq$b-v40-r512 $b $D1 5000 /Volumes/v40bench/codes/crq$b/v40 $M1 r512
  run crq$b-v40-halo512 $b $D1 5000 /Volumes/v40bench/codes/crq$b/v40 $M1 halo512
  run crq$b-tenm-r512 $b $D10 1000 /Volumes/v40bench/codes/crq$b/tenm $M10 r512
  run crq$b-tenm-halo512 $b $D10 1000 /Volumes/v40bench/codes/crq$b/tenm $M10 halo512
done
date "+== %H:%M:%S done"
