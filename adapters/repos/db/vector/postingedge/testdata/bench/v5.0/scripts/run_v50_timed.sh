#!/bin/bash
# v5.0 (pre-registered 2026-11-18): the timed session of the inserts on disk
# (bench-report.md, "v5.0: inserts en disco"), on macOS and on Linux.
#
# The stages, in this order when none is named:
#   bin      builds the benchmark binary into $WORK.
#   inserts  builds the base in batch over the first tenth of the train set
#            and writes it under $DISK, untimed, and then times the inserts
#            that grow it to the whole train set, alone, one after another.
#            It then checks the coverage of what it grew, compacts the store
#            and finds the operating points of the index, untimed.
#   search   times the searches over the index that grew (curve "grown") and
#            over the cell with codes of 512 bits written in batch by v4.0
#            (curve "batch"), warm and, unless COLD=none, cold: one process
#            each, all the curves once and then all of them again. The first
#            round is the measure and the second tells drift, as in the
#            fifth amendment of v4.0.
#   verdict  writes grow-verdict.md from what there is in $OUT.
#
# usage: [VAR=value ...] run_v50_timed.sh [stage ...]
#   DATASET  the dataset directory. Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     the directory the index grows in, on the disk under test, with
#            some 8 GB free. It must not exist, or be empty.
#   BATCH    the directory of the cells of v4.0 (its $DISK/pe), and
#   BATCH_MANIFEST the manifest of its write stage. Without BATCH the curve
#            "batch" is left out.
#   OUT      where the results go. Default: testdata/bench/v5.0.
#   WORK     where the binary and the logs go. Default: $OUT/work.
#   COLD     how the system's cache of the index files is dropped before each
#            cold batch, as in run_v40_disk.sh: fadvise (default on Linux),
#            volume (with VOLUME_DEV and VOLUME_MOUNT; $DISK and $BATCH must
#            be on that volume), cmd (with DROP) or none.
#   IDLE     the session waits until the keyboard and the trackpad have been
#            idle for this many seconds, with no other benchmark running, and
#            then wants the machine fully awake (macOS). Default 0.
#   NIGHT    1 makes it wait for the window of 23:00 to 03:00 besides, and
#            IDLE default to 900 (macOS). Default 0.
#   ROUNDS   the rounds of the search stage. Default "r1 r2".
#   CURVES   the curves of the search stage. Default "grown batch".
#   EXTRA    more flags for the inserts. A smoke: EXTRA="-bench.limit 30000"
#            QUERIES=500 COLDQ=40, without BATCH.
#   QUERIES (5000) and COLDQ (500): the queries of a warm pass and the cold
#            queries of every point.
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
OS="$(uname)"
if [ "$OS" = Darwin ]; then COLD="${COLD:-none}"; else COLD="${COLD:-fadvise}"; fi
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the index grows in"; exit 1; }
BATCH="${BATCH:-}"
BATCH_MANIFEST="${BATCH_MANIFEST:-}"
if [ -n "$BATCH" ]; then CURVES="${CURVES:-grown batch}"; else CURVES="${CURVES:-grown}"; fi
PE="$WORK/postingedge-v50t.test"
LABEL="dbpedia-1m-grow"
MANIFEST="$OUT/$LABEL-manifest.json"
STRETCHES="$OUT/$LABEL-stretches.tsv"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"

case "$COLD" in
  volume)  [ -n "${VOLUME_DEV:-}" ] && [ -n "${VOLUME_MOUNT:-}" ] || { echo "== COLD=volume needs VOLUME_DEV and VOLUME_MOUNT"; exit 1; }
           case "$DISK/" in "$VOLUME_MOUNT"/*) ;; *) echo "== DISK must be under $VOLUME_MOUNT"; exit 1 ;; esac
           # The unmount is tried a few times: the system may be reading the
           # volume. If it never goes, the command fails, and so does the run.
           DROP="ok=0; for try in 1 2 3 4 5; do diskutil unmount '$VOLUME_MOUNT' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ \$ok = 1 ] && diskutil mount '$VOLUME_DEV' > /dev/null" ;;
  fadvise) DROP="python3 -I -B '$HERE/../../v4.0/scripts/drop_cache.py' '$DISK' ${BATCH:+'$BATCH'}" ;;
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
lines() { grep -E "$2" "$1" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-500; }

# wait_quiet: the gate of a timed stage. Nothing else of these benchmarks may
# be running, an earlier session included. The window is asked for once: a
# stage that follows another of the same run starts when the first is done,
# whatever the hour.
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
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh' > /dev/null \
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
# line about its stretch in $STRETCHES: when it ran, how long the keyboard
# and the trackpad had been idle at its start and at its end, and how many
# events of power or of user activity the system logged meanwhile, which go
# to the .txt next to it. The keyboard or the trackpad were used during the
# stretch when at its end they have been idle for less than it lasted.
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
  [ "$ok" = 1 ] || echo "== $round, $curve did not pass: see $WORK/$LABEL-$round-$curve.log"
  [ "$ok" = 1 ]
}

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
}

run_inserts() {
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestGrowTimedInserts$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.growdir "$DISK" -bench.growmanifest "$MANIFEST" ${EXTRA:-} > "$WORK/$LABEL-inserts-grow.log" 2>&1)
  lines "$WORK/$LABEL-inserts-grow.log" "bench_grow_test|STOP"
  passed "$WORK/$LABEL-inserts-grow.log"
}

stage_inserts() {
  wait_quiet
  date "+== %Y-%m-%d %H:%M the timed inserts start: $DISK"
  stretch inserts grow run_inserts || { echo "== the inserts did not pass: see $WORK/$LABEL-inserts-grow.log"; exit 1; }
  date "+== %Y-%m-%d %H:%M the inserts stage ends"
}

run_grown() {
  (cd "$PKG" && "$PE" -test.run 'TestGrowTimedSearch$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.growdir "$DISK" -bench.growmanifest "$MANIFEST" -bench.diskdrop "$DROP" -bench.diskcold "$COLDQ" \
    -bench.out "$OUT/$LABEL-$1-grown.json" > "$WORK/$LABEL-$1-grown.log" 2>&1)
  lines "$WORK/$LABEL-$1-grown.log" "cell "
  passed "$WORK/$LABEL-$1-grown.log"
}

run_batch() {
  (cd "$PKG" && "$PE" -test.run 'TestDiskTimed$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.diskdir "$BATCH" -bench.diskmanifest "$BATCH_MANIFEST" \
    -bench.diskcell r512 -bench.diskdrop "$DROP" -bench.diskcold "$COLDQ" \
    -bench.out "$OUT/$LABEL-$1-batch.json" > "$WORK/$LABEL-$1-batch.log" 2>&1)
  lines "$WORK/$LABEL-$1-batch.log" "cell "
  passed "$WORK/$LABEL-$1-batch.log"
}

stage_search() {
  for curve in $CURVES; do
    case "$curve" in
      grown) ;;
      batch) [ -n "$BATCH" ] && [ -n "$BATCH_MANIFEST" ] || { echo "== the curve batch needs BATCH and BATCH_MANIFEST"; exit 1; } ;;
      *) echo "== unknown curve $curve"; exit 1 ;;
    esac
  done
  wait_quiet
  START="$(date '+%Y-%m-%d %H:%M:%S')"
  date "+== %Y-%m-%d %H:%M the timed searches start, cold: $COLD, rounds: $ROUNDS, curves: $CURVES"
  for round in $ROUNDS; do
    date "+== %Y-%m-%d %H:%M round $round"
    for curve in $CURVES; do
      stretch "$round" "$curve" "run_$curve" "$round"
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
  python3 -I -B "$HERE/grow_verdict.py" "$OUT" > "$OUT/grow-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/grow-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin inserts search verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD, disk: $DISK"
for stage in "$@"; do
  case "$stage" in
    bin|inserts|search|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
