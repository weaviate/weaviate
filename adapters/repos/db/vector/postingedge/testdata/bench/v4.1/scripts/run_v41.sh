#!/bin/bash
# v4.1 (pre-registered 2026-11-19): the scan with the sketches in memory
# (bench-report.md, "v4.1: el scan con los sketches en RAM"). One batch build
# of the posting-edge index, as that of v4.0, and three indexes written from
# it: the cell of 512 bits of v4.0 (r512), whose postings on disk hold their
# members with the full code and their edges, and two whose postings hold
# their edges only (s512, s768), the members scanned by the sketch of 512 or
# 768 bits that is in memory for the routing. Each of the two is a curve with
# a rescore limit of 200, of 400 and of 800. Every curve is timed as v4.0
# times a cell, twice: with the budget of 8 a query of v4.0 (b8) and with the
# budget the admission limiter of a node grants every query (node).
#
# The stages, in this order when none is named:
#   bin      builds the benchmark binary into $WORK.
#   write    builds the index, writes the three indexes under $DISK/pe, finds
#            the operating points of the seven curves, and checks that each
#            returns from disk what its state in memory returns and that the
#            store reads what the byte model says. Untimed. Its manifest goes
#            to $OUT.
#   timed    times every curve of $CURVES in every pass of $PASSES, one curve
#            per process, in the rounds of $ROUNDS.
#   verdict  writes the tables of the session to $OUT/sketch-verdict.md.
#
# usage: [VAR=value ...] run_v41.sh [stage ...]
#   DATASET  the dataset directory. Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     the directory the indexes go under, on the disk under test, with
#            some 8 GB free.
#   OUT      where the manifest, the results and the verdict go. Default:
#            testdata/bench/v4.1.
#   WORK     where the binary and the logs go. Default: $OUT/work.
#   COLD     how the cache of the system is dropped for the cold measures:
#            volume (DISK is on an APFS volume of its own, VOLUME_DEV mounted
#            at VOLUME_MOUNT, unmounted and mounted again) or none (no cold
#            measure). Default none.
#   CURVES   the curves to time. Default: all seven.
#   PASSES   b8, node or both. Default: both.
#   ROUNDS   the rounds of the timed session. Default: r1 r2.
#   IDLE     the timed stage waits until the keyboard and the trackpad have
#            been idle for this many seconds, with no other benchmark
#            running, and then wants the machine fully awake (macOS).
#            Default 0.
#   NIGHT    1 makes it wait for the window of 23:00 to 03:00 besides, and
#            IDLE default to 900 (macOS). Default 0. AFTER=1 says that it
#            follows another session of the same night: it then starts when
#            the machine is quiet, whatever the hour.
#   QUERIES (5000) and COLDQ (500): the queries of a warm pass and the cold
#            queries of every point.
#   EXTRA_PE more flags for the write stage. A smoke:
#            EXTRA_PE="-bench.limit 30000" QUERIES=500 COLDQ=40
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
COLDQ="${COLDQ:-500}"
COLD="${COLD:-none}"
NIGHT="${NIGHT:-0}"
AFTER="${AFTER:-0}"
if [ "$NIGHT" = 1 ]; then IDLE="${IDLE:-900}"; else IDLE="${IDLE:-0}"; fi
ROUNDS="${ROUNDS:-r1 r2}"
PASSES="${PASSES:-b8 node}"
ALL_CURVES="r512 s512r200 s512r400 s512r800 s768r200 s768r400 s768r800"
CURVES="${CURVES:-$ALL_CURVES}"
OS="$(uname)"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the indexes go under"; exit 1; }
PE="$WORK/postingedge-v41.test"
LABEL="dbpedia-1m-sketch"
STRETCHES="$OUT/$LABEL-stretches.tsv"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"

case "$COLD" in
  volume) [ -n "${VOLUME_DEV:-}" ] && [ -n "${VOLUME_MOUNT:-}" ] || { echo "== COLD=volume needs VOLUME_DEV and VOLUME_MOUNT"; exit 1; }
          case "$DISK/" in "$VOLUME_MOUNT"/*) ;; *) echo "== DISK must be under $VOLUME_MOUNT"; exit 1 ;; esac
          # The unmount is tried a few times: the system may be reading the
          # volume. If it never goes, the command fails, and so does the run:
          # a batch that was not dropped would be timed as cold.
          DROP="ok=0; for try in 1 2 3 4 5; do diskutil unmount '$VOLUME_MOUNT' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ \$ok = 1 ] && diskutil mount '$VOLUME_DEV' > /dev/null" ;;
  none)   DROP="" ;;
  *)      echo "== unknown COLD=$COLD"; exit 1 ;;
esac

if [ "$OS" = Darwin ]; then
  # hid_idle, awake, in_window, power_state and power_log_since
  # shellcheck source=/dev/null
  source "$HERE/../../v1.6/scripts/v16_queue.sh"
fi

# passed <log>: the test binary ended with PASS
passed() { [ "$(tail -1 "$1")" = PASS ]; }
# lines <log> <pattern>: what a test logged, without the file and line
lines() { grep -E "$2" "$1" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400; }

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
}

stage_write() {
  mkdir -p "$DISK" || exit 1
  date "+== %H:%M build, write and check, untimed: $ALL_CURVES"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestDiskWrite$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.diskdir "$DISK/pe" -bench.diskcells "${ALL_CURVES// /,}" -bench.diskmanifest "$OUT/$LABEL-manifest.json" \
    ${EXTRA_PE:-} > "$WORK/$LABEL-write.log" 2>&1)
  lines "$WORK/$LABEL-write.log" "built|index |cell |sketch of|STOP"
  passed "$WORK/$LABEL-write.log" || { echo "== the write stage did not pass: see $WORK/$LABEL-write.log"; exit 1; }
}

# timed_curve <curve> <round> <pass>: one curve at its two recalls, with the
# budget of the pass
timed_curve() {
  local admission=""
  [ "$3" != node ] || admission="-bench.diskadmission"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestDiskTimed$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.diskdir "$DISK/pe" -bench.diskmanifest "$OUT/$LABEL-manifest.json" \
    -bench.diskcell "$1" -bench.diskdrop "$DROP" -bench.diskcold "$COLDQ" $admission \
    -bench.out "$OUT/$LABEL-$2-$3-$1.json" > "$WORK/$LABEL-$2-$3-$1.log" 2>&1)
  lines "$WORK/$LABEL-$2-$3-$1.log" "cell "
  passed "$WORK/$LABEL-$2-$3-$1.log"
}

# stretch <round> <pass> <curve>: times one curve and leaves a line about its
# stretch in $STRETCHES: when it ran, how long the keyboard and the trackpad
# had been idle at its start and at its end, and how many events of power or
# of user activity the system logged meanwhile, which go to the .txt next to
# it. The keyboard or the trackpad were used during the stretch when at its
# end they have been idle for less than it lasted.
stretch() {
  local round="$1" pass="$2" curve="$3" start begin secs ok=1 idle0=-1 idle1=-1 events=0 logged
  [ -e "$STRETCHES" ] || printf 'round\tcurve\tstart\tend\tseconds\tidle at start\tidle at end\tevents\tpassed\n' > "$STRETCHES"
  [ "$OS" != Darwin ] || idle0=$(hid_idle)
  start="$(date '+%Y-%m-%d %H:%M:%S')"
  begin=$(date +%s)
  date "+== %H:%M $round, $pass, $curve"
  timed_curve "$curve" "$round" "$pass" || ok=0
  secs=$(( $(date +%s) - begin ))
  if [ "$OS" = Darwin ]; then
    idle1=$(hid_idle)
    logged="$(power_log_since "$start")"
    [ -z "$logged" ] || events=$(printf '%s\n' "$logged" | wc -l | tr -d ' ')
    { echo "== $round, $pass, $curve: from $start, $secs s"; [ -z "$logged" ] || printf '%s\n' "$logged"; } >> "${STRETCHES%.tsv}-events.txt"
    [ "$idle1" -ge "$secs" ] || echo "-- keyboard or trackpad used during $round, $pass, $curve: idle for $idle1 s at its end, and it took $secs s"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$round" "$pass-$curve" "$start" "$(date '+%Y-%m-%d %H:%M:%S')" \
    "$secs" "$idle0" "$idle1" "$events" "$ok" >> "$STRETCHES"
  [ "$ok" = 1 ] || echo "== $round, $pass, $curve did not pass: see $WORK/$LABEL-$round-$pass-$curve.log"
}

# wait_quiet: nothing else of these benchmarks may be running.
wait_quiet() {
  [ "$OS" = Darwin ] || return 0
  if [ "$NIGHT" = 1 ] || [ "$IDLE" -gt 0 ]; then
    date "+== %Y-%m-%d %H:%M waiting for $IDLE s without keyboard or trackpad$([ "$NIGHT" = 1 ] && [ "$AFTER" != 1 ] && echo ', in the window of 23:00 to 03:00')"
    until { [ "$NIGHT" != 1 ] || [ "$AFTER" = 1 ] || in_window 2300 0300; } \
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh|run_v50_timed\.sh|run_v51\.sh|run_hfresh_inserts\.sh' > /dev/null \
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

stage_timed() {
  local curve pass round
  for curve in $CURVES; do
    case " $ALL_CURVES " in *" $curve "*) ;; *) echo "== unknown curve $curve"; exit 1 ;; esac
  done
  for pass in $PASSES; do
    case "$pass" in b8|node) ;; *) echo "== unknown pass $pass"; exit 1 ;; esac
  done
  [ -e "$OUT/$LABEL-manifest.json" ] || { echo "== no manifest: the write stage comes first"; exit 1; }
  wait_quiet
  START="$(date '+%Y-%m-%d %H:%M:%S')"
  date "+== %Y-%m-%d %H:%M the timed session starts, cold: $COLD, rounds: $ROUNDS, passes: $PASSES, curves: $CURVES"
  for round in $ROUNDS; do
    for pass in $PASSES; do
      date "+== %Y-%m-%d %H:%M round $round, pass $pass"
      for curve in $CURVES; do
        stretch "$round" "$pass" "$curve"
      done
      # what there is so far: the session may be read before it ends
      stage_verdict
    done
  done
  date "+== %Y-%m-%d %H:%M the timed session ends"
  if [ "$OS" = Darwin ]; then
    power_state
    power_log_since "$START"
  fi
}

stage_verdict() {
  python3 -I -B "$HERE/sketch_verdict.py" "$OUT" > "$OUT/sketch-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/sketch-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin write timed verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD, disk: $DISK"
for stage in "$@"; do
  case "$stage" in
    bin|write|timed|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
