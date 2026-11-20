#!/bin/bash
# The combined control of v5.1 (bench-report.md, "control combinado"): the
# index v5.1 grew with K = 64, laid out again as cell 2 of the control of
# v5.0, with its buckets in one segment each and its exact vectors in posting
# order, timed next to that cell of v5.0, to the cell written in batch by
# v4.0 and to itself as it is. And what a load reads cold, row by row, in the
# two grown indexes as they are and in their two cells.
#
# The stages, in this order when none is named:
#   bin      builds the benchmark binary into $WORK, and a copy of it under
#            another name for the untimed stages.
#   reorg    untimed: writes the new cell from $K64 into $ORDERED, checks
#            that it returns what $K64 returns for every query at both
#            operating points, which stops it if not, and counts what the
#            store reads for a load.
#   reads    untimed: for each of the four indexes in the format that grows,
#            what a load reads in the cold queries, row by row: what the
#            store counts, and what the system reads from disk for the
#            process, dropping the cache before every batch.
#   search   times the searches, warm and cold, one process each, all the
#            curves once and then all of them again: k64o (the new cell),
#            grown2 (cell 2 of v5.0), batch and k64 (as it is).
#   verdict  writes combined-verdict.md from what there is in $OUT.
#
# usage: [VAR=value ...] run_v51_combined.sh [stage ...]
#   DATASET  the dataset directory.
#   K64, K64_MANIFEST  the index v5.1 grew with K = 64 and its manifest.
#   ORDERED  the directory of the new cell. It must not exist, or be empty.
#   GROWN, GROWN2, GROWN_MANIFEST  the index v5.0 grew, its cell 2 and their
#            manifest.
#   BATCH, BATCH_MANIFEST  the directory of the cells of v4.0 and the
#            manifest of its write stage.
#   OUT      where the results go. Default: testdata/bench/v5.1.
#   WORK     where the binary and the logs go. Default: $OUT/work.
#   COLD     how the system's cache is dropped: volume (with VOLUME_DEV and
#            VOLUME_MOUNT), or cmd (with DROP). The stages reads and search
#            need one.
#   IDLE     the timed stage waits until the keyboard and the trackpad have
#            been idle for this many seconds, with no other benchmark and no
#            compilation running. Default 0.
#   NIGHT    1 makes it wait for the window of 23:00 to 03:00 besides, and
#            IDLE default to 900.
#   ROUNDS   the rounds of the search stage. Default "r1 r2".
#   CURVES   the curves of the search stage. Default "k64o grown2 batch k64".
#   READS    the indexes of the stage reads. Default "k64 grown k64o grown2".
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
CURVES="${CURVES:-k64o grown2 batch k64}"
READS="${READS:-k64 grown k64o grown2}"
COLD="${COLD:-none}"
OS="$(uname)"
for var in K64 K64_MANIFEST ORDERED GROWN GROWN2 GROWN_MANIFEST BATCH BATCH_MANIFEST; do
  [ -n "${!var:-}" ] || { echo "== $var is not set"; exit 1; }
done
PE="$WORK/postingedge-v51c.test"
# The same binary under a name the timed sessions do not wait for.
PEU="$WORK/pe-combined.test"
LABEL="dbpedia-1m-comb"
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
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh|run_v50_timed\.sh|pe-[a-z]*\.test' > /dev/null \
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

stage_reorg() {
  local log="$WORK/$LABEL-reorg.log"
  date "+== %Y-%m-%d %H:%M the new cell: $K64 laid out again into $ORDERED, untimed"
  (cd "$PKG" && "$PEU" -test.run 'TestGrowReorganize$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.growdir "$K64" -bench.growmanifest "$K64_MANIFEST" -bench.growordered "$ORDERED" \
    -bench.out "$OUT/$LABEL-reorg.json" > "$log" 2>&1)
  lines "$log" "bench_grow_test|STOP"
  passed "$log" || { echo "== the new cell did not pass: see $log"; exit 1; }
}

# dir_of <index>: the directory of one of the four indexes that grow
dir_of() {
  case "$1" in
    k64)    echo "$K64" ;;
    k64o)   echo "$ORDERED" ;;
    grown)  echo "$GROWN" ;;
    grown2) echo "$GROWN2" ;;
    *)      return 1 ;;
  esac
}
# manifest_of <index>
manifest_of() {
  case "$1" in
    k64|k64o) echo "$K64_MANIFEST" ;;
    *)        echo "$GROWN_MANIFEST" ;;
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
      batch) ;;
      k64o)  [ -e "$OUT/$LABEL-reorg.json" ] || { echo "== the curve k64o needs the stage reorg first"; exit 1; } ;;
      *)     dir_of "$curve" > /dev/null || { echo "== unknown curve $curve"; exit 1; } ;;
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
  python3 -I -B "$HERE/combined_verdict.py" "$OUT" > "$OUT/combined-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/combined-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin reorg reads search verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD"
for stage in "$@"; do
  case "$stage" in
    bin|reorg|reads|search|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
