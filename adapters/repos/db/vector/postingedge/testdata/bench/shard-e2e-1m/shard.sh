#!/bin/bash
# The inserts end to end through a shard, at 1M (bench-report.md of
# feat/posting-edge-index, "Medidas definitivas a 1M, segunda parte: de punta
# a punta por un shard", pre-registered on 2026-11-28 by the user's order):
# dbpedia 1M of 1536 dimensions, 8 callers that put batches of 100 objects
# with their vectors through DB.BatchPutObjects, the indexing in line as a
# node has it by default; HFresh first, then the posting-edge index with its
# routing codes in memory. Three clocks each: the last batch back, the
# background work of the index done, the store flushed; then recall and
# latency of 1 000 queries through the shard. The binary is built before
# (shard/shard-e2e.test, integration 20e89f0873 with the harness); nothing
# here builds. Each database is removed once it is read. Timed: marker, idle
# wait, caffeinate.
# Usage: shard.sh [smoke]: with smoke, 20 000 rows and 100 queries, untimed.
set -u
S=/private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad
smoke=${1:-}
W=$S/shard/run
V=/Volumes/v40bench/shard1m
ROWS=0; Q=1000; PROGRESS=30
if [ "$smoke" = smoke ]; then W=$S/shard/smoke; V=/Volumes/v40bench/shardsmoke; ROWS=20000; Q=100; PROGRESS=5; fi
D=$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin
T=$S/prop/integ/adapters/repos/db
mkdir -p $W $V

hid_idle() { ioreg -c IOHIDSystem | awk '/HIDIdleTime/ {print int($NF/1000000000); exit}'; }
if [ "$smoke" != smoke ]; then
  printf 'the inserts end to end through a shard at 1M (shard/shard.sh), timed, since %s for about an hour; taken down by the experiments session\n' "$(date '+%Y-%m-%d %H:%M')" > $S/timed-session.lock
  date "+== %H:%M:%S waiting for 900 s of idle (now $(hid_idle) s)"
  while [ "$(hid_idle)" -lt 900 ]; do sleep 30; done
  date "+== %Y-%m-%d %H:%M:%S idle for $(hid_idle) s"
fi

cell() { # name index
  local C=$W/$1
  local free=$(df -k $V | tail -n 1 | awk '{print int($4/1048576)}')
  if [ "$free" -lt 30 ]; then
    date "+== %H:%M:%S $1 NOT RUN: $free GiB free, under 30"
    return
  fi
  mkdir -p $C $V/$1
  date "+== %Y-%m-%d %H:%M:%S $1: $2 (idle $(hid_idle) s)"
  (cd $T && exec env PE_DATASET=$D PE_DIR=$V/$1/db PE_OUT=$C/result.json PE_INDEX=$2 PE_CACHE=routing PE_CALLERS=8 PE_BATCH=100 \
    PE_ROWS=$ROWS PE_QUERIES=$Q PE_PROGRESS=$PROGRESS \
    $S/shard/shard-e2e.test -test.run '^TestShardE2E$' -test.v -test.timeout 0 > $C/run.log 2>&1) &
  local pid=$!
  python3 -I -B $S/def1m/monitor.py $pid $C/monitor.jsonl 10 > $C/monitor.err 2>&1 &
  wait $pid
  echo "$1 exit $?"
  wait
  du -sk $V/$1/db 2>/dev/null | awk '{printf "database: %.1f GB\n", $1/976562.5}'
  find /Volumes/v40bench/shard1m /Volumes/v40bench/shardsmoke -type d -name db -prune -exec rm -rf {} + 2>/dev/null
}

cell hfresh hfresh
cell postingedge postingedge

if [ "$smoke" != smoke ]; then
  date "+== %Y-%m-%d %H:%M:%S done (idle $(hid_idle) s)" | tee $W/end.txt
  pmset -g log 2>/dev/null | grep -E '^[0-9-]+ [0-9:]+ \+[0-9]+ (Sleep|Wake|DarkWake)' | tail -n 5 > $W/pmset-sleep-wake.txt
  rm -f /private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad/timed-session.lock
fi
