#!/bin/bash
# v5.1 (pre-registered 2026-11-19): consolidated edges (bench-report.md,
# "v5.1: aristas consolidadas"), on macOS and on Linux. The path of v5.0,
# with the changes to the edges of a posting waiting in memory and its row
# of edges written whole after K of them.
#
# The stages, in this order when none is named:
#   bin       builds the benchmark binary into $WORK.
#   validate  untimed: for every K, an index built in batch over the first
#             tenth of the train set grows to the whole of it next to its
#             twin in memory, the store compacting by levels and counting
#             what it reads. Every checkpoint writes what waits and checks
#             the coverage and the index against the twin: either failing
#             stops that run. The runs of the several K go at once.
#   inserts   timed, alone, for every K one after another: the base written
#             under $DISK/timed-k$K, untimed, and the inserts that grow it,
#             with the forced compaction of HFresh's postings bucket. Then
#             the coverage, the compaction for good and the operating
#             points, untimed.
#   search    times the searches, warm and, unless COLD=none, cold, one
#             process each, all the curves once and then all of them again:
#             the index of every K (curves k$K), the index v5.0 grew (curve
#             "grown", $GROWN) and the cell with codes of 512 bits written
#             in batch by v4.0 (curve "batch", $BATCH).
#   verdict   writes consolidated-verdict.md from what there is in $OUT.
#
# usage: [VAR=value ...] run_v51.sh [stage ...]
#   DATASET  the dataset directory. Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     the directory the indexes grow under, on the disk under test,
#            with some 8 GB free for each: validate-k$K and timed-k$K. None
#            may exist, or they must be empty.
#   KS       the values of K. Default "16 64".
#   GROWN, GROWN_MANIFEST  the index the timed run of v5.0 grew, and its
#            manifest. Without GROWN the curve "grown" is left out.
#   BATCH, BATCH_MANIFEST  the directory of the cells of v4.0 (its $DISK/pe)
#            and the manifest of its write stage. Without BATCH the curve
#            "batch" is left out.
#   V50      the results of v5.0, which the verdict compares with. Default:
#            ../v5.0.
#   OUT      where the results go. Default: testdata/bench/v5.1.
#   WORK     where the binary and the logs go. Default: $OUT/work.
#   BIN      the name of the binary in $WORK. Default postingedge-v51t.test.
#            A validation that may still run when a timed session builds
#            its binary wants another: BIN=postingedge-v51v.test.
#   COLD     how the system's cache of the index files is dropped before each
#            cold batch, as in run_v40_disk.sh: fadvise (default on Linux),
#            volume (with VOLUME_DEV and VOLUME_MOUNT; everything read must
#            be on that volume), cmd (with DROP) or none.
#   IDLE     a timed stage waits until the keyboard and the trackpad have
#            been idle for this many seconds, with no other benchmark
#            running, and then wants the machine fully awake (macOS).
#            Default 0.
#   NIGHT    1 makes it wait for the window of 23:00 to 03:00 besides, and
#            IDLE default to 900 (macOS). Default 0.
#   ROUNDS   the rounds of the search stage. Default "r1 r2".
#   CURVES   the curves of the search stage. Default: k$K for every K, then
#            grown and batch when their directories are given.
#   EXTRA    more flags for validate and inserts. A smoke:
#            EXTRA="-bench.limit 30000" QUERIES=500 COLDQ=40
#   QUERIES (5000) and COLDQ (500): the queries of a warm pass and the cold
#            queries of every point.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
V50="${V50:-$(cd "$HERE/../../v5.0" && pwd)}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
COLDQ="${COLDQ:-500}"
KS="${KS:-16 64}"
NIGHT="${NIGHT:-0}"
if [ "$NIGHT" = 1 ]; then IDLE="${IDLE:-900}"; else IDLE="${IDLE:-0}"; fi
ROUNDS="${ROUNDS:-r1 r2}"
OS="$(uname)"
if [ "$OS" = Darwin ]; then COLD="${COLD:-none}"; else COLD="${COLD:-fadvise}"; fi
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the indexes grow under"; exit 1; }
GROWN="${GROWN:-}"
GROWN_MANIFEST="${GROWN_MANIFEST:-}"
BATCH="${BATCH:-}"
BATCH_MANIFEST="${BATCH_MANIFEST:-}"
if [ -z "${CURVES:-}" ]; then
  CURVES=""
  for k in $KS; do CURVES="$CURVES k$k"; done
  [ -z "$GROWN" ] || CURVES="$CURVES grown"
  [ -z "$BATCH" ] || CURVES="$CURVES batch"
fi
PE="$WORK/${BIN:-postingedge-v51t.test}"
LABEL="dbpedia-1m-cons"
STRETCHES="$OUT/$LABEL-stretches.tsv"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"

case "$COLD" in
  volume)  [ -n "${VOLUME_DEV:-}" ] && [ -n "${VOLUME_MOUNT:-}" ] || { echo "== COLD=volume needs VOLUME_DEV and VOLUME_MOUNT"; exit 1; }
           case "$DISK/" in "$VOLUME_MOUNT"/*) ;; *) echo "== DISK must be under $VOLUME_MOUNT"; exit 1 ;; esac
           # The unmount is tried a few times: the system may be reading the
           # volume. If it never goes, the command fails, and so does the run.
           DROP="ok=0; for try in 1 2 3 4 5; do diskutil unmount '$VOLUME_MOUNT' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ \$ok = 1 ] && diskutil mount '$VOLUME_DEV' > /dev/null" ;;
  fadvise) DROP="for d in '$DISK' ${GROWN:+'$GROWN'} ${BATCH:+'$BATCH'}; do [ ! -d \"\$d\" ] || python3 -I -B '$HERE/../../v4.0/scripts/drop_cache.py' \"\$d\" || exit 1; done" ;;
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

# wait_quiet: the gate of a timed stage. Nothing else of these benchmarks may
# be running, a validation or an earlier session included. The window is
# asked for once: a stage that follows another of the same run starts when
# the first is done, whatever the hour.
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
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh|run_v50_timed\.sh' > /dev/null \
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
  [ "$ok" = 1 ] || echo "== $round, $curve did not pass: see the log of $round, $curve in $WORK"
  [ "$ok" = 1 ]
}

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
}

# run_validate <K>: the validation of one K, in its own directory
run_validate() {
  local log="$WORK/$LABEL-k$1-validate.log"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestGrowValidate$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.growdir "$DISK/validate-k$1" -bench.growforce=false -bench.growk "$1" \
    -bench.out "$OUT/$LABEL-k$1-validate.json" ${EXTRA:-} > "$log" 2>&1)
  passed "$log"
}

stage_validate() {
  local k pids="" failed=0
  date "+== %Y-%m-%d %H:%M the validations start, untimed, all at once: K = $KS, under $DISK"
  for k in $KS; do
    run_validate "$k" &
    pids="$pids $k:$!"
  done
  for pair in $pids; do
    k="${pair%%:*}"
    if wait "${pair##*:}"; then
      date "+== %Y-%m-%d %H:%M K = $k: the validation passed"
    else
      date "+== %Y-%m-%d %H:%M K = $k: the validation did NOT pass: see $WORK/$LABEL-k$k-validate.log"
      failed=1
    fi
    lines "$WORK/$LABEL-k$k-validate.log" "bench_grow_test|STOP"
  done
  stage_verdict
  [ "$failed" = 0 ] || exit 1
}

# run_inserts <K>
run_inserts() {
  local log="$WORK/$LABEL-inserts-k$1.log"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestGrowTimedInserts$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.growdir "$DISK/timed-k$1" -bench.growk "$1" -bench.growmanifest "$OUT/$LABEL-k$1-manifest.json" \
    ${EXTRA:-} > "$log" 2>&1)
  lines "$log" "bench_grow_test|STOP"
  passed "$log"
}

stage_inserts() {
  local k
  wait_quiet
  for k in $KS; do
    date "+== %Y-%m-%d %H:%M the timed inserts of K = $k start: $DISK/timed-k$k"
    stretch inserts "k$k" run_inserts "$k" || { echo "== the inserts of K = $k did not pass: see $WORK/$LABEL-inserts-k$k.log"; exit 1; }
    stage_verdict
  done
  date "+== %Y-%m-%d %H:%M the inserts stage ends"
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
    k*)    run_grown_in "$DISK/timed-$2" "$OUT/$LABEL-$2-manifest.json" "$1" "$2" ;;
    grown) run_grown_in "$GROWN" "$GROWN_MANIFEST" "$1" grown ;;
    batch) run_batch "$1" ;;
  esac
}

stage_search() {
  local curve round
  for curve in $CURVES; do
    case "$curve" in
      k*)    [ -e "$OUT/$LABEL-$curve-manifest.json" ] || { echo "== the curve $curve needs the stage inserts first"; exit 1; } ;;
      grown) [ -n "$GROWN" ] && [ -n "$GROWN_MANIFEST" ] || { echo "== the curve grown needs GROWN and GROWN_MANIFEST"; exit 1; } ;;
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
  python3 -I -B "$HERE/consolidated_verdict.py" "$OUT" "$V50" > "$OUT/consolidated-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/consolidated-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin validate inserts search verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD, disk: $DISK, K: $KS"
for stage in "$@"; do
  case "$stage" in
    bin|validate|inserts|search|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
