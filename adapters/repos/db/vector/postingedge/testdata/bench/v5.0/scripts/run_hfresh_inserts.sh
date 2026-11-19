#!/bin/bash
# v5.0, the fourth amendment (pre-registered 2026-11-19): the inserts of the
# real HFresh at 1M with one writer (bench-report.md, "los inserts de HFresh
# a 1M con un escritor"), next to the inserts on disk of the posting-edge
# index. The index of this branch with its default configuration, built from
# empty as the build of v4.0 builds it, by $WRITERS goroutines: timed to its
# last insert, and to its background queues empty.
#
# The stages, in this order when none is named:
#   bin      builds the benchmark binary of HFresh into $WORK.
#   inserts  writes the exact vectors, untimed, and times the inserts, alone.
#            Then, untimed, the recall of what they left.
#
# usage: [VAR=value ...] run_hfresh_inserts.sh [stage ...]
#   DATASET  the dataset directory. Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     the directory of the index, on the disk under test, with some
#            20 GB free. It must not exist, or be empty: this does not write
#            over an index.
#   WRITERS  the goroutines that insert. Default 1. 0 is one per GOMAXPROCS,
#            as the build of v4.0.
#   OUT      where the result goes. Default: testdata/bench/v5.0.
#   WORK     where the binary and the log go. Default: $OUT/work.
#   IDLE     the stage waits until the keyboard and the trackpad have been
#            idle for this many seconds, with no other benchmark running, and
#            then wants the machine fully awake (macOS). Default 0.
#   NIGHT    1 makes it wait for the window of 23:00 to 03:00 besides, and
#            IDLE default to 900 (macOS). Default 0. AFTER=1 says that it
#            follows another session of the same night: it then starts when
#            the machine is quiet, whatever the hour.
#   EXTRA    more flags for the binary. A smoke:
#            EXTRA="-bench.hfresh.limit 30000 -bench.hfresh.queries 500"
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
WRITERS="${WRITERS:-1}"
NIGHT="${NIGHT:-0}"
AFTER="${AFTER:-0}"
if [ "$NIGHT" = 1 ]; then IDLE="${IDLE:-900}"; else IDLE="${IDLE:-0}"; fi
OS="$(uname)"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory of the index"; exit 1; }
HF="$WORK/hfresh-v50i.test"
LABEL="dbpedia-1m-hfresh-inserts"
RESULT="$OUT/$LABEL-w$WRITERS.json"
LOG="$WORK/$LABEL-w$WRITERS.log"
STRETCHES="$OUT/$LABEL-stretches.tsv"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-40GiB}"

if [ "$OS" = Darwin ]; then
  # hid_idle, awake, in_window, power_state and power_log_since
  # shellcheck source=/dev/null
  source "$HERE/../../v1.6/scripts/v16_queue.sh"
fi

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$HF" ./adapters/repos/db/vector/hfresh/) || exit 1
}

# wait_quiet: nothing else of these benchmarks may be running.
wait_quiet() {
  [ "$OS" = Darwin ] || return 0
  if [ "$NIGHT" = 1 ] || [ "$IDLE" -gt 0 ]; then
    date "+== %Y-%m-%d %H:%M waiting for $IDLE s without keyboard or trackpad$([ "$NIGHT" = 1 ] && [ "$AFTER" != 1 ] && echo ', in the window of 23:00 to 03:00')"
    until { [ "$NIGHT" != 1 ] || [ "$AFTER" = 1 ] || in_window 2300 0300; } \
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh|run_v50_timed\.sh|run_v51\.sh' > /dev/null \
      && [ "$(hid_idle)" -ge "$IDLE" ]; do
      sleep 60
    done
    if ! awake; then
      caffeinate -u -t 5
      sleep 10
    fi
    awake || { date "+== %H:%M not fully awake, nothing timed"; power_state; exit 1; }
  fi
  power_state
}

stage_inserts() {
  local start begin secs ok=1 idle0=-1 idle1=-1 events=0 logged
  wait_quiet
  [ -e "$STRETCHES" ] || printf 'round\tcurve\tstart\tend\tseconds\tidle at start\tidle at end\tevents\tpassed\n' > "$STRETCHES"
  [ "$OS" != Darwin ] || idle0=$(hid_idle)
  start="$(date '+%Y-%m-%d %H:%M:%S')"
  begin=$(date +%s)
  date "+== %Y-%m-%d %H:%M the inserts of HFresh start, $WRITERS writers: $DISK"
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskInserts$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.dir "$DISK" -bench.hfresh.writers "$WRITERS" \
    -bench.hfresh.out "$RESULT" ${EXTRA:-} > "$LOG" 2>&1)
  [ "$(tail -1 "$LOG")" = PASS ] || ok=0
  secs=$(( $(date +%s) - begin ))
  grep -E "bench_disk_test" "$LOG" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400
  if [ "$OS" = Darwin ]; then
    idle1=$(hid_idle)
    logged="$(power_log_since "$start")"
    [ -z "$logged" ] || events=$(printf '%s\n' "$logged" | wc -l | tr -d ' ')
    { echo "== inserts, w$WRITERS: from $start, $secs s"; [ -z "$logged" ] || printf '%s\n' "$logged"; } >> "${STRETCHES%.tsv}-events.txt"
    [ "$idle1" -ge "$secs" ] || echo "-- keyboard or trackpad used during the inserts: idle for $idle1 s at their end, and they took $secs s"
    power_state
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' inserts "w$WRITERS" "$start" "$(date '+%Y-%m-%d %H:%M:%S')" \
    "$secs" "$idle0" "$idle1" "$events" "$ok" >> "$STRETCHES"
  [ "$ok" = 1 ] || { echo "== the inserts did not pass: see $LOG"; exit 1; }
  date "+== %Y-%m-%d %H:%M the inserts end"
}

[ "$#" -gt 0 ] || set -- bin inserts
date "+== %Y-%m-%d %H:%M start: $* | $OS, writers: $WRITERS, disk: $DISK"
for stage in "$@"; do
  case "$stage" in
    bin|inserts) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
