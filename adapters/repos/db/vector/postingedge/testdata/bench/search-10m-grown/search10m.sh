#!/bin/bash
# The layout of the 10M index and its search cold and warm (bench-report.md
# of feat/posting-edge-index, "Reorganizar el índice de 10M y medir su
# búsqueda en frío y en caliente", pre-registered on 2026-11-28 by the user's
# order): over a clone of the grown index, which stays as it is: its points
# over the 1 000 queries of the dataset, the timed search as the growth left
# it, the layout, its points again and the timed search laid out. The clone
# laid out is kept. The binary is the one of the second part of the measures
# at 1M (def1m/def2.test, m2 8ed921ed59 with the night harness); nothing here
# builds. Timed: marker, idle wait, caffeinate.
# Usage: search10m.sh [smoke]: with smoke, 50 queries, 20 of them cold, one
# pass, no layout, and the clone is removed.
set -u
S=/private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad
smoke=${1:-}
C=$S/search10m/run
V=/Volumes/v40bench/grow10m/laid
Q=1000; COLD=500; PASSES=3; THREADS=1,8
if [ "$smoke" = smoke ]; then
  C=$S/search10m/smoke; V=/Volumes/v40bench/grow10m/laidsmoke; Q=50; COLD=20; PASSES=1; THREADS=1
fi
D=$HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin
T=$S/diag/m2/adapters/repos/db/vector/postingedge
DROP="ok=0; for try in 1 2 3 4 5; do diskutil unmount '/Volumes/v40bench' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ \$ok = 1 ] && diskutil mount 'disk3s7' > /dev/null"
mkdir -p $C

hid_idle() { ioreg -c IOHIDSystem | awk '/HIDIdleTime/ {print int($NF/1000000000); exit}'; }
if [ "$smoke" != smoke ]; then
  printf 'the layout of the 10M index and its search cold and warm (search10m/search10m.sh), timed, since %s for about three hours; taken down by the experiments session\n' "$(date '+%Y-%m-%d %H:%M')" > $S/timed-session.lock
  date "+== %H:%M:%S waiting for 900 s of idle (now $(hid_idle) s)"
  while [ "$(hid_idle)" -lt 900 ]; do sleep 30; done
  date "+== %Y-%m-%d %H:%M:%S idle for $(hid_idle) s"
fi

# A pass of the layout writes the vectors again (31 GB) next to the old file.
free=$(df -k /Volumes/v40bench | tail -n 1 | awk '{print int($4/1048576)}')
if [ "$free" -lt 50 ]; then
  date "+== %H:%M:%S NOT RUN: $free GiB free, under 50"
  rm -f /private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad/timed-session.lock
  exit 4
fi
mkdir -p $V
if [ ! -d $V/index ]; then
  cp -c -R /Volumes/v40bench/grow10m/index $V/index || exit 3
fi

points() { # state
  date "+== %H:%M:%S points, $1 (idle $(hid_idle) s)"
  (cd $T && PE_DATASET=$D PE_DIR=$V/index PE_CACHE=routing PE_POINTS=$C/points-$1.json PE_THREADS=8 PE_QUERIES=$Q \
    $S/def1m/def2.test -test.run '^TestNightPoints$' -test.v -test.timeout 0 > $C/points-$1.log 2>&1)
  echo "points $1 exit $?"
}
timed() { # state
  date "+== %Y-%m-%d %H:%M:%S timed, $1 (idle $(hid_idle) s)"
  (cd $T && PE_DATASET=$D PE_DIR=$V/index PE_CACHE=routing PE_POINTS=$C/points-$1.json PE_OUT=$C/timed-$1.json \
    PE_QUERIES=$Q PE_COLD=$COLD PE_PASSES=$PASSES PE_TIMEDTHREADS=$THREADS PE_DROP="$DROP" \
    $S/def1m/def2.test -test.run '^TestNightTimed$' -test.v -test.timeout 0 > $C/timed-$1.log 2>&1)
  echo "timed $1 exit $?"
}
layout() {
  date "+== %Y-%m-%d %H:%M:%S layout (idle $(hid_idle) s)"
  (cd $T && PE_DIR=$V/index PE_CACHE=routing PE_LAYOUT=1 PE_OUT=$C/layout.json PE_MAXWAIT=3600 \
    $S/def1m/def2.test -test.run '^TestNightLayout$' -test.v -test.timeout 0 > $C/layout.log 2>&1)
  echo "layout exit $?"
}

points asleft
timed asleft
if [ "$smoke" = smoke ]; then
  find /Volumes/v40bench/grow10m/laidsmoke -type d -name index -prune -exec rm -rf {} +
  date "+== %H:%M:%S smoke done"
  exit 0
fi
layout
points laidout
timed laidout
du -sk $V/index | awk '{printf "the clone laid out: %.1f GB\n", $1/976562.5}'
date "+== %Y-%m-%d %H:%M:%S done (idle $(hid_idle) s)" | tee $C/end.txt
pmset -g log 2>/dev/null | grep -E '^[0-9-]+ [0-9:]+ \+[0-9]+ (Sleep|Wake|DarkWake)' | tail -n 5 > $C/pmset-sleep-wake.txt
rm -f /private/tmp/claude-501/-Users-abdel-Documents-Projects-hnsw-pq-wt-posting-edge-index/e94130d1-0af4-421d-89c2-936de2c15159/scratchpad/timed-session.lock
