#!/bin/bash
# The pre-registered measure of the centroids as 8-bit RQ codes (bench-report.md,
# "Centroides en RQ8", 5f6be2c3e8), untimed: the cells r512 and halo512 at 1M
# dbpedia and 10M wiki-dpr, from APFS clones, in float32 and with codes.
set -u
S=/private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad
P=/Users/abdel/Documents/Projects/hnsw+pq/wt-posting-edge-index/adapters/repos/db/vector/postingedge
B=$S/heap/pe-codes.test
run() { # name dataset queries diskdir manifest cell
  date "+== %H:%M:%S $1"
  (cd $S/m2/adapters/repos/db/vector/postingedge && $B -test.run 'TestDiskCentroidCodes$' -test.v -test.timeout 0 \
    -bench.dataset "$2" -bench.queries "$3" -bench.diskdir "$4" -bench.diskmanifest "$5" -bench.diskcell "$6" \
    -bench.out $S/codes/$1.json > $S/codes/$1.log 2>&1)
  echo "$1 exit $?"; grep -E 'cell .*(recall|heap in use)|FAIL|PASS|panic|Error' $S/codes/$1.log | sed 's/^ *//' | cut -c1-220
}
run v40-r512 $HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin 5000 /Volumes/v40bench/codes/v40 $P/testdata/bench/v4.0/dbpedia-1m-disk-manifest.json r512
run v40-halo512 $HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin 5000 /Volumes/v40bench/codes/v40 $P/testdata/bench/v4.0/dbpedia-1m-disk-manifest.json halo512
run tenm-r512 $HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin 1000 /Volumes/v40bench/codes/tenm $P/testdata/bench/v4.0/10m/wikidpr-10m-manifest.json r512
run tenm-halo512 $HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin 1000 /Volumes/v40bench/codes/tenm $P/testdata/bench/v4.0/10m/wikidpr-10m-manifest.json halo512
date "+== %H:%M:%S done"
