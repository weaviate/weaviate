#!/bin/bash
# What held the lock of the index 355 ms at 10M (bench-report.md of
# feat/posting-edge-index, "El cerrojo del índice retenido 355 ms a 10M",
# pre-registered on 2026-11-28): every hold of the index for writing, with
# its parts, next to one thread of queries and 8 callers, at 10M (180 s) and
# at 100k (60 s); and what giving the tables by doc id more room takes, at
# 100k, 1M and 10M. Over clones, removed after each pass; no index is opened
# itself. No maintenance of the index in the background, and at 10M the clock
# starts once the segments of the store have stood still for 15 s. The binary
# is built before (lock/holds.test); nothing here builds. Timed: marker, idle
# wait, caffeinate.
set -u
S=/private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad
X=$S/diag
L=$S/lock
V=/Volumes/v40bench/grow10m
D=$HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin
P=adapters/repos/db/vector/postingedge
mkdir -p $L/clones $V/clones /Volumes/v40bench/sim1m/clone

printf 'the holds of the lock of the index (lock/holds.sh), timed, since %s for about half an hour; taken down by the experiments session\n' "$(date '+%Y-%m-%d %H:%M')" > $S/timed-session.lock

hid_idle() { ioreg -c IOHIDSystem | awk '/HIDIdleTime/ {print int($NF/1000000000); exit}'; }
date "+== %H:%M:%S waiting for 900 s of idle (now $(hid_idle) s)"
while [ "$(hid_idle)" -lt 900 ]; do sleep 30; done
date "+== %Y-%m-%d %H:%M:%S idle for $(hid_idle) s"

clear_clones() {
  find /Volumes/v40bench/grow10m/clones -type d -name index -prune -exec rm -rf {} +
  find /Volumes/v40bench/sim1m/clone -type d -name index -prune -exec rm -rf {} +
  find /private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad/lock/clones -type d -name index -prune -exec rm -rf {} +
}

# run <size> <name> <test> [NAME=value ...]
run() {
  local size=$1 name=$2 test=$3
  shift 3
  local C=$L/$name
  local dir
  mkdir -p $C
  case $size in
  100k)
    dir=$L/clones/$name/index
    mkdir -p $L/clones/$name
    cp -c -R $X/i100k/index $dir || exit 3
    find $dir -type f -exec cat {} + > /dev/null
    set -- PE_ROWS=100000 "$@"
    ;;
  1m)
    dir=/Volumes/v40bench/sim1m/clone/index
    cp -c -R /Volumes/v40bench/sim1m/index $dir || exit 3
    find $dir/lsm -type f -exec cat {} + > /dev/null
    ;;
  10m)
    dir=$V/clones/$name/index
    mkdir -p $V/clones/$name
    cp -c -R $V/index $dir || exit 3
    # The store read once (7 GB); the vectors (30.7 GB) stay as cold as a
    # new clone is.
    find $dir/lsm -type f -exec cat {} + > /dev/null
    ;;
  esac
  date "+== %H:%M:%S $name: $test $* (idle $(hid_idle) s)"
  (cd $X/src/$P && exec env PE_DATASET=$D PE_DIR=$dir PE_OUT=$C/result.json "$@" \
    $L/holds.test -test.run "^$test\$" -test.v -test.timeout 0 > $C/run.log 2>&1) &
  local pid=$!
  python3 -I -B $X/monitor.py $pid $C/monitor.jsonl 2 > $C/monitor.err 2>&1 &
  wait $pid
  echo "$name exit $?"
  wait
  clear_clones
}

run 10m h10m TestDiagQueries PE_NOMAINTENANCE=1 PE_SETTLE=15 PE_EF=24 PE_QTHREADS=1 PE_CALLERS=8 PE_SECONDS=180 PE_NVECTORS=60000
run 100k h100k TestDiagQueries PE_NOMAINTENANCE=1 PE_EF=17 PE_QTHREADS=1 PE_CALLERS=8 PE_SECONDS=60 PE_NVECTORS=90000
run 100k x100k TestDiagExtend PE_EXTEND=1
run 1m x1m TestDiagExtend PE_EXTEND=1
run 10m x10m TestDiagExtend PE_EXTEND=1

date "+== %Y-%m-%d %H:%M:%S done (idle $(hid_idle) s)" | tee $L/end.txt
pmset -g log 2>/dev/null | grep -E '^[0-9-]+ [0-9:]+ \+[0-9]+ (Sleep|Wake|DarkWake)' | tail -n 5 > $L/pmset-sleep-wake.txt
rm -f /private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad/timed-session.lock
