#!/bin/bash
# v5.4 (bench-report.md, "v5.4: la disposición de v5.3 sobre un bucket Map"):
# the index that grows on disk with the row of edges of a posting as one
# value next to its members, as v5.3, with the bucket of postings a Map: a
# member is a pair, and the row of edges a pair that a new one replaces.
#
# The stages, in this order when none is named:
#   bin       builds the benchmark binary into $WORK, and a copy of it under
#             another name for the untimed stages.
#   gate      untimed: writes $DISK/gate, the index of the curves of v5.3
#             ($V53_GROWN) laid out again over a Map, which must return what
#             that index returns; counts what the store reads for a load; and
#             searches it warm next to the index of v5.3 laid out again
#             ($V53_ORDERED), one process each, three times in turns. It
#             stops the run if reading through the Map costs more (the gate
#             of the pre-registration, told by map_verdict.py --gate).
#   validate  untimed: an index built in batch over the first tenth of the
#             train set grows to the whole of it next to its twin in memory,
#             the store compacting by levels and counting what it reads.
#             Every insert and every checkpoint is checked against the twin:
#             a difference stops it.
#   grow      untimed here: the index of the curves, grown the same way with
#             the forced compaction of HFresh's postings bucket, into
#             $DISK/grown, with its coverage, its compaction for good and its
#             operating points.
#   reorg     untimed: writes $DISK/ordered, the index laid out again, checks
#             that it returns what the index returns as it is, which stops it
#             if not, and counts what the store reads for a load.
#   reads     untimed: what a load reads in the cold queries, row by row, in
#             the index as it is and laid out again: what the store counts,
#             and what the system reads from disk for the process.
#   search    times the searches, warm and cold, one process each, all the
#             curves once and then all of them again: mapo (v5.4 laid out
#             again), rowso (v5.3 laid out again), batch and gate (the index
#             of v5.3 laid out again over a Map).
#   verdict   writes map-verdict.md from what there is in $OUT.
#
# usage: [VAR=value ...] run_v54.sh [stage ...]
#   DATASET  the dataset directory.
#   DISK     the directory the indexes go under, on the disk under test, with
#            some 25 GB free: gate, validate, grown and ordered. None may
#            exist.
#   K        the changes a row of edges is written after. Default 64.
#   V53_GROWN, V53_ORDERED, V53_MANIFEST  the index of the curves of v5.3, as
#            it is and laid out again, and its manifest.
#   BATCH, BATCH_MANIFEST  the directory of the cells of v4.0 and the
#            manifest of its write stage.
#   V53, V51 the results of v5.3 and of v5.1, whose counts the verdict puts
#            next to these. Default: ../v5.3 and ../v5.1.
#   OUT      where the results go. Default: testdata/bench/v5.4.
#   WORK     where the binary and the logs go. Default: $OUT/work.
#   COLD     how the system's cache is dropped: volume (with VOLUME_DEV and
#            VOLUME_MOUNT), or cmd (with DROP). The stages reads and search
#            need one. The gate drops nothing.
#   IDLE     the timed stage waits until the keyboard and the trackpad have
#            been idle for this many seconds, with no other benchmark and no
#            compilation running. Default 0.
#   NIGHT    1 makes it wait for the window of 23:00 to 03:00 besides, and
#            IDLE default to 900.
#   ROUNDS   the rounds of the search stage. Default "r1 r2".
#   CURVES   the curves of the search stage. Default "mapo rowso batch gate".
#   READS    the indexes of the stage reads. Default "map mapo".
#   EXTRA    more flags for validate and grow. A smoke:
#            EXTRA="-bench.limit 30000" QUERIES=500
#   QUERIES (5000) and COLDQ (500).
#
# It must not be edited while it runs.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
COLDQ="${COLDQ:-500}"
NIGHT="${NIGHT:-0}"
if [ "$NIGHT" = 1 ]; then IDLE="${IDLE:-900}"; else IDLE="${IDLE:-0}"; fi
ROUNDS="${ROUNDS:-r1 r2}"
CURVES="${CURVES:-mapo rowso batch gate}"
READS="${READS:-map mapo}"
COLD="${COLD:-none}"
OS="$(uname)"
K="${K:-64}"
V51="${V51:-$(cd "$HERE/../../v5.1" && pwd)}"
V53="${V53:-$(cd "$HERE/../../v5.3" && pwd)}"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the indexes go under"; exit 1; }
GROWN_DIR="$DISK/grown"
ORDERED="$DISK/ordered"
GATE="$DISK/gate"
PE="$WORK/postingedge-v54.test"
# The same binary under a name the timed sessions do not wait for.
PEU="$WORK/pe-map.test"
LABEL="dbpedia-1m-map"
STRETCHES="$OUT/$LABEL-stretches.tsv"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"

case "$COLD" in
  volume)  [ -n "${VOLUME_DEV:-}" ] && [ -n "${VOLUME_MOUNT:-}" ] || { echo "== COLD=volume needs VOLUME_DEV and VOLUME_MOUNT"; exit 1; }
           # The unmount is tried a few times: the system may be reading the
           # volume. If it never goes, the command fails, and so does the run.
           DROP="ok=0; for try in 1 2 3 4 5; do diskutil unmount '$VOLUME_MOUNT' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ \$ok = 1 ] && diskutil mount '$VOLUME_DEV' > /dev/null" ;;
  cmd)     [ -n "${DROP:-}" ] || { echo "== COLD=cmd needs DROP"; exit 1; } ;;
  none)    DROP="" ;;
  *)       echo "== unknown COLD=$COLD"; exit 1 ;;
esac

if [ "$OS" = Darwin ]; then
  # hid_idle, awake, in_window, power_state and power_log_since
  # shellcheck source=/dev/null
  source "$HERE/../../v1.6/scripts/v16_queue.sh"
fi

# passed <log>: the test binary ended with PASS
passed() { [ "$(tail -1 "$1")" = PASS ]; }
# lines <log> <pattern>: what a test logged, without the file and line
lines() { grep -E "$2" "$1" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-600; }

# wait_quiet: the gate of the timed stage. Nothing else of these benchmarks
# may be running, and nothing may be compiling or testing on the machine.
IN_SESSION=0
wait_quiet() {
  [ "$OS" = Darwin ] || return 0
  if [ "$NIGHT" = 1 ] || [ "$IDLE" -gt 0 ]; then
    if [ "$NIGHT" = 1 ]; then
      date "+== %Y-%m-%d %H:%M waiting for the window of 23:00 to 03:00 and $IDLE s without keyboard or trackpad"
    else
      date "+== %Y-%m-%d %H:%M waiting for $IDLE s without keyboard or trackpad"
    fi
    until { [ "$NIGHT" != 1 ] || [ "$IN_SESSION" = 1 ] || in_window 2300 0300; } \
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh|run_v50_timed\.sh|pe-[a-z0-9]*\.test' > /dev/null \
      && ! pgrep -f 'pkg/tool/[a-z0-9_]*/(compile|link|asm)|golangci-lint|go-build[0-9]*/b[0-9]*/[^ ]*\.test' > /dev/null \
      && [ "$(hid_idle)" -ge "$IDLE" ]; do
      sleep 60
    done
    if ! awake; then
      caffeinate -u -t 5
      sleep 10
    fi
    awake || { date "+== %H:%M not fully awake, nothing timed"; power_state; exit 1; }
  fi
  IN_SESSION=1
  power_state
}

# stretch <round> <curve> <command...>: runs one timed process and leaves a
# line about its stretch in $STRETCHES, as run_v51.sh does.
stretch() {
  local round="$1" curve="$2" start begin secs ok=1 idle0=-1 idle1=-1 events=0 logged
  shift 2
  [ -e "$STRETCHES" ] || printf 'round\tcurve\tstart\tend\tseconds\tidle at start\tidle at end\tevents\tpassed\n' > "$STRETCHES"
  [ "$OS" != Darwin ] || idle0=$(hid_idle)
  start="$(date '+%Y-%m-%d %H:%M:%S')"
  begin=$(date +%s)
  date "+== %H:%M $round, $curve"
  "$@" || ok=0
  secs=$(( $(date +%s) - begin ))
  if [ "$OS" = Darwin ]; then
    idle1=$(hid_idle)
    logged="$(power_log_since "$start")"
    [ -z "$logged" ] || events=$(printf '%s\n' "$logged" | wc -l | tr -d ' ')
    { echo "== $round, $curve: from $start, $secs s"; [ -z "$logged" ] || printf '%s\n' "$logged"; } >> "${STRETCHES%.tsv}-events.txt"
    [ "$idle1" -ge "$secs" ] || echo "-- keyboard or trackpad used during $round, $curve: idle for $idle1 s at its end, and it took $secs s"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$round" "$curve" "$start" "$(date '+%Y-%m-%d %H:%M:%S')" \
    "$secs" "$idle0" "$idle1" "$events" "$ok" >> "$STRETCHES"
  [ "$ok" = 1 ] || echo "== $round, $curve did not pass: see the log of $round, $curve in $WORK"
  [ "$ok" = 1 ]
}

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
  cp "$PE" "$PEU" || exit 1
}

# need_v53: the gate and two curves read the index of the curves of v5.3
need_v53() {
  [ -n "${V53_GROWN:-}" ] && [ -n "${V53_ORDERED:-}" ] && [ -n "${V53_MANIFEST:-}" ] \
    || { echo "== $1 needs V53_GROWN, V53_ORDERED and V53_MANIFEST"; exit 1; }
}

stage_gate() {
  local log="$WORK/$LABEL-gate-reorg.log" pass index dir
  need_v53 "the stage gate"
  date "+== %Y-%m-%d %H:%M the gate: the index of v5.3 laid out again over a Map, $V53_GROWN into $GATE, untimed"
  (cd "$PKG" && "$PEU" -test.run 'TestGrowReorganize$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.growdir "$V53_GROWN" -bench.growmanifest "$V53_MANIFEST" -bench.growordered "$GATE" -bench.growmap \
    -bench.out "$OUT/$LABEL-gate-reorg.json" > "$log" 2>&1)
  lines "$log" "bench_grow_test|STOP"
  passed "$log" || { echo "== the gate: the index over a Map did not pass: see $log"; exit 1; }
  date "+== %Y-%m-%d %H:%M the gate: warm searches, the Set and the Map in turns, untimed and indicative"
  echo "-- running besides, by CPU: $(ps -Ao pcpu,comm -r | sed -n '2,6p' | awk '{printf "%s%% %s; ", $1, $2}')"
  for pass in 1 2 3; do
    for index in set map; do
      if [ "$index" = set ]; then dir="$V53_ORDERED"; else dir="$GATE"; fi
      log="$WORK/$LABEL-gate-warm-$index-$pass.log"
      # No -bench.diskdrop: nothing is dropped, and only the warm passes run.
      (cd "$PKG" && "$PEU" -test.run 'TestGrowTimedSearch$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
        -bench.growdir "$dir" -bench.growmanifest "$V53_MANIFEST" \
        -bench.out "$OUT/$LABEL-gate-warm-$index-$pass.json" > "$log" 2>&1)
      lines "$log" "cell .*recall 0.95.*1 threads, warm"
      passed "$log" || { echo "== the gate: the warm searches of $index, pass $pass, did not pass: see $log"; exit 1; }
    done
  done
  stage_verdict
  python3 -I -B "$HERE/map_verdict.py" "$OUT" "$V53" "$V51" --gate
  case "$?" in
    0) date "+== %Y-%m-%d %H:%M the gate lets the run go on" ;;
    3) date "+== %Y-%m-%d %H:%M the gate STOPS the run: reading through the Map costs more"; exit 3 ;;
    *) echo "== the gate could not be read"; exit 1 ;;
  esac
}

stage_validate() {
  local log="$WORK/$LABEL-validate.log"
  date "+== %Y-%m-%d %H:%M the validation starts, untimed: the index next to its twin, under $DISK/validate"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PEU" -test.run 'TestGrowValidate$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.growdir "$DISK/validate" -bench.growforce=false -bench.growk "$K" -bench.growrows -bench.growmap \
    -bench.out "$OUT/$LABEL-validate.json" ${EXTRA:-} > "$log" 2>&1)
  lines "$log" "bench_grow_test|STOP"
  passed "$log" || { date "+== %Y-%m-%d %H:%M the validation did NOT pass: see $log"; exit 1; }
  date "+== %Y-%m-%d %H:%M the validation passed"
  stage_verdict
}

stage_grow() {
  local log="$WORK/$LABEL-grow.log"
  date "+== %Y-%m-%d %H:%M the index of the curves grows, untimed here: $GROWN_DIR"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PEU" -test.run 'TestGrowTimedInserts$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.growdir "$GROWN_DIR" -bench.growk "$K" -bench.growrows -bench.growmap \
    -bench.growmanifest "$OUT/$LABEL-manifest.json" \
    ${EXTRA:-} > "$log" 2>&1)
  lines "$log" "bench_grow_test|STOP"
  passed "$log" || { echo "== the index of the curves did not pass: see $log"; exit 1; }
  stage_verdict
}

stage_reorg() {
  local log="$WORK/$LABEL-reorg.log"
  [ -e "$OUT/$LABEL-manifest.json" ] || { echo "== the stage reorg needs the stage grow first"; exit 1; }
  date "+== %Y-%m-%d %H:%M the index laid out again: $GROWN_DIR into $ORDERED, untimed"
  (cd "$PKG" && "$PEU" -test.run 'TestGrowReorganize$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.growdir "$GROWN_DIR" -bench.growmanifest "$OUT/$LABEL-manifest.json" -bench.growordered "$ORDERED" \
    -bench.out "$OUT/$LABEL-reorg.json" > "$log" 2>&1)
  lines "$log" "bench_grow_test|STOP"
  passed "$log" || { echo "== the index laid out again did not pass: see $log"; exit 1; }
  stage_verdict
}

# dir_of <index>: the directory of one of the indexes in the format that grows
dir_of() {
  case "$1" in
    map)   echo "$GROWN_DIR" ;;
    mapo)  echo "$ORDERED" ;;
    rowso) echo "${V53_ORDERED:-}" ;;
    gate)  echo "$GATE" ;;
    *)     return 1 ;;
  esac
}
# manifest_of <index>
manifest_of() {
  case "$1" in
    map|mapo)   echo "$OUT/$LABEL-manifest.json" ;;
    rowso|gate) echo "${V53_MANIFEST:-}" ;;
  esac
}

stage_reads() {
  local name log
  [ -n "$DROP" ] || { echo "== the stage reads needs COLD"; exit 1; }
  for name in $READS; do
    dir_of "$name" > /dev/null || { echo "== unknown index $name"; exit 1; }
    log="$WORK/$LABEL-reads-$name.log"
    date "+== %Y-%m-%d %H:%M what a load reads cold, row by row: $name, untimed"
    (cd "$PKG" && "$PEU" -test.run 'TestGrowColdReads$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
      -bench.growdir "$(dir_of "$name")" -bench.growmanifest "$(manifest_of "$name")" -bench.diskdrop "$DROP" \
      -bench.diskcold "$COLDQ" -bench.out "$OUT/$LABEL-reads-$name.json" > "$log" 2>&1)
    lines "$log" "bench_grow_cold_test|STOP"
    passed "$log" || { echo "== the count of $name did not pass: see $log"; exit 1; }
  done
  stage_verdict
}

# run_grown_in <dir> <manifest> <round> <curve>: the searches over an index
# in the format that grows
run_grown_in() {
  local log="$WORK/$LABEL-$3-$4.log"
  (cd "$PKG" && "$PE" -test.run 'TestGrowTimedSearch$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.growdir "$1" -bench.growmanifest "$2" -bench.diskdrop "$DROP" -bench.diskcold "$COLDQ" \
    -bench.out "$OUT/$LABEL-$3-$4.json" > "$log" 2>&1)
  lines "$log" "cell "
  passed "$log"
}

# run_batch <round>
run_batch() {
  local log="$WORK/$LABEL-$1-batch.log"
  (cd "$PKG" && "$PE" -test.run 'TestDiskTimed$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.diskdir "$BATCH" -bench.diskmanifest "$BATCH_MANIFEST" \
    -bench.diskcell r512 -bench.diskdrop "$DROP" -bench.diskcold "$COLDQ" \
    -bench.out "$OUT/$LABEL-$1-batch.json" > "$log" 2>&1)
  lines "$log" "cell "
  passed "$log"
}

# run_curve <round> <curve>
run_curve() {
  case "$2" in
    batch) run_batch "$1" ;;
    *)     run_grown_in "$(dir_of "$2")" "$(manifest_of "$2")" "$1" "$2" ;;
  esac
}

stage_search() {
  local curve round
  [ -n "$DROP" ] || { echo "== the stage search needs COLD"; exit 1; }
  for curve in $CURVES; do
    case "$curve" in
      batch) [ -n "${BATCH:-}" ] && [ -n "${BATCH_MANIFEST:-}" ] || { echo "== the curve batch needs BATCH and BATCH_MANIFEST"; exit 1; } ;;
      mapo)  [ -e "$OUT/$LABEL-reorg.json" ] || { echo "== the curve mapo needs the stage reorg first"; exit 1; } ;;
      gate)  [ -e "$OUT/$LABEL-gate-reorg.json" ] || { echo "== the curve gate needs the stage gate first"; exit 1; } ;;
      *)     dir_of "$curve" > /dev/null || { echo "== unknown curve $curve"; exit 1; }
             [ -n "$(dir_of "$curve")" ] && [ -n "$(manifest_of "$curve")" ] || { echo "== the curve $curve needs its directory and its manifest"; exit 1; } ;;
    esac
  done
  wait_quiet
  START="$(date '+%Y-%m-%d %H:%M:%S')"
  date "+== %Y-%m-%d %H:%M the timed searches start, cold: $COLD, rounds: $ROUNDS, curves: $CURVES"
  for round in $ROUNDS; do
    date "+== %Y-%m-%d %H:%M round $round"
    for curve in $CURVES; do
      stretch "$round" "$curve" run_curve "$round" "$curve"
    done
    # what there is so far: the session may be read before it ends
    stage_verdict
  done
  date "+== %Y-%m-%d %H:%M the timed searches end"
  if [ "$OS" = Darwin ]; then
    power_state
    power_log_since "$START"
  fi
}

stage_verdict() {
  python3 -I -B "$HERE/map_verdict.py" "$OUT" "$V53" "$V51" > "$OUT/map-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/map-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin gate validate grow reorg reads search verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD"
for stage in "$@"; do
  case "$stage" in
    bin|gate|validate|grow|reorg|reads|search|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
