#!/bin/bash
# v5.2 (pre-registered 2026-11-19): inserts with their searches in parallel
# (bench-report.md, "v5.2: inserts con búsquedas en paralelo"). The index
# that grows on disk of v5.0, from 10 % to 100 % of the train set, in four
# cells: the insert of always (n1), 4 and 8 searchers over batches of 32
# inserts (n4, n8), whose searches all run over the index as the batch finds
# it before the writes of any of them, and 8 searchers over batches of 8
# (n8w8). Every cell is timed alone, with the coverage of its index checked
# at every checkpoint out of its time, and searched afterwards, untimed: at
# its own operating points and at the ef of n1.
#
# The stages, in this order when none is named:
#   bin      builds the benchmark binary into $WORK.
#   inserts  times every cell of $CELLS, one after another.
#   verdict  writes the tables of the session to $OUT/parallel-inserts-verdict.md.
#
# usage: [VAR=value ...] run_v52.sh [stage ...]
#   DATASET  the dataset directory. Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     the directory the indexes of the cells go under, one each, on
#            the disk under test, with some 30 GB free. A cell whose
#            directory exists is not run: this does not write over an index.
#   K        consolidated edges (v5.1): the changes a row of edges is written
#            after. Default 0: the path of v5.0.
#   CELLS    the cells to run. Default: n1 n4 n8 n8w8. n1 goes first: the
#            others are searched at its ef too.
#   OUT      where the manifests and the verdict go. Default: testdata/bench/v5.2.
#   WORK     where the binary and the logs go. Default: $OUT/work.
#   IDLE     the stage waits until the keyboard and the trackpad have been
#            idle for this many seconds, with no other benchmark running, and
#            then wants the machine fully awake (macOS). Default 0.
#   NIGHT    1 makes it wait for the window of 23:00 to 03:00 besides, and
#            IDLE default to 900 (macOS). Default 0. AFTER=1 says that it
#            follows another session of the same night: it then starts when
#            the machine is quiet, whatever the hour.
#   QUERIES  the queries the grown index is searched with. Default 5000.
#   EXTRA    more flags for the binary. A smoke: EXTRA="-bench.limit 30000" QUERIES=500
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
K="${K:-0}"
NIGHT="${NIGHT:-0}"
AFTER="${AFTER:-0}"
if [ "$NIGHT" = 1 ]; then IDLE="${IDLE:-900}"; else IDLE="${IDLE:-0}"; fi
ALL_CELLS="n1 n4 n8 n8w8"
CELLS="${CELLS:-$ALL_CELLS}"
OS="$(uname)"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the indexes of the cells go under"; exit 1; }
PE="$WORK/postingedge-v52.test"
LABEL="dbpedia-1m-par"
STRETCHES="$OUT/$LABEL-stretches.tsv"
IN_SESSION=0
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-40GiB}"

if [ "$OS" = Darwin ]; then
  # hid_idle, awake, in_window, power_state and power_log_since
  # shellcheck source=/dev/null
  source "$HERE/../../v1.6/scripts/v16_queue.sh"
fi

# passed <log>: the test binary ended with PASS
passed() { [ "$(tail -1 "$1")" = PASS ]; }
# lines <log> <pattern>: what a test logged, without the file and line
lines() { grep -E "$2" "$1" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400; }

# searchers <cell> and batch <cell>: the goroutines that search and the
# inserts of a batch
searchers() { case "$1" in n1) echo 1 ;; n4) echo 4 ;; n8|n8w8) echo 8 ;; esac; }
batch() { case "$1" in n1) echo 1 ;; n4|n8) echo 32 ;; n8w8) echo 8 ;; esac; }

stage_bin() {
  date "+== %H:%M building the binary"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/) || exit 1
}

# wait_quiet: the gate of the session. Nothing else of these benchmarks may
# be running. Once the session has started, a cell follows the one before.
wait_quiet() {
  [ "$OS" = Darwin ] || return 0
  if [ "$NIGHT" = 1 ] || [ "$IDLE" -gt 0 ]; then
    date "+== %Y-%m-%d %H:%M waiting for $IDLE s without keyboard or trackpad$([ "$NIGHT" = 1 ] && [ "$AFTER" != 1 ] && [ "$IN_SESSION" != 1 ] && echo ', in the window of 23:00 to 03:00')"
    until { [ "$NIGHT" != 1 ] || [ "$AFTER" = 1 ] || [ "$IN_SESSION" = 1 ] || in_window 2300 0300; } \
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh|run_v40_node\.sh|run_v50_timed\.sh|run_v51\.sh|run_hfresh_inserts\.sh|run_v41\.sh' > /dev/null \
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
  IN_SESSION=1
}

# run_cell <cell>: grows the index of one cell, timed, checks it and
# searches it. The cells after n1 are searched at the ef of n1 too.
run_cell() {
  local cell="$1" efs="" start begin secs ok=1 idle0=-1 idle1=-1 events=0 logged log="$WORK/$LABEL-$1.log"
  if [ "$cell" != n1 ] && [ -e "$OUT/$LABEL-n1-manifest.json" ]; then
    efs="$(python3 -I -B -c 'import json,sys; print(",".join(str(p["ef"]) for p in json.load(open(sys.argv[1]))["points"] if p["reached"]))' "$OUT/$LABEL-n1-manifest.json")"
  fi
  [ -e "$STRETCHES" ] || printf 'round\tcurve\tstart\tend\tseconds\tidle at start\tidle at end\tevents\tpassed\n' > "$STRETCHES"
  [ "$OS" != Darwin ] || idle0=$(hid_idle)
  start="$(date '+%Y-%m-%d %H:%M:%S')"
  begin=$(date +%s)
  date "+== %Y-%m-%d %H:%M cell $cell: $(searchers "$cell") searchers over batches of $(batch "$cell"), K = $K: $DISK/$cell"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestGrowTimedInserts$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.growdir "$DISK/$cell" -bench.growk "$K" -bench.growsearchers "$(searchers "$cell")" \
    -bench.growbatch "$(batch "$cell")" -bench.growcoverage ${efs:+-bench.growefs "$efs"} \
    -bench.growmanifest "$OUT/$LABEL-$cell-manifest.json" ${EXTRA:-} > "$log" 2>&1)
  passed "$log" || ok=0
  secs=$(( $(date +%s) - begin ))
  lines "$log" "bench_grow_test|STOP" | grep -vE '^ef [0-9]+: recall'
  if [ "$OS" = Darwin ]; then
    idle1=$(hid_idle)
    logged="$(power_log_since "$start")"
    [ -z "$logged" ] || events=$(printf '%s\n' "$logged" | wc -l | tr -d ' ')
    { echo "== inserts, $cell: from $start, $secs s"; [ -z "$logged" ] || printf '%s\n' "$logged"; } >> "${STRETCHES%.tsv}-events.txt"
    [ "$idle1" -ge "$secs" ] || echo "-- keyboard or trackpad used during cell $cell: idle for $idle1 s at its end, and it took $secs s"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' inserts "$cell" "$start" "$(date '+%Y-%m-%d %H:%M:%S')" \
    "$secs" "$idle0" "$idle1" "$events" "$ok" >> "$STRETCHES"
  [ "$ok" = 1 ]
}

stage_inserts() {
  local cell
  for cell in $CELLS; do
    case " $ALL_CELLS " in *" $cell "*) ;; *) echo "== unknown cell $cell"; exit 1 ;; esac
    [ ! -e "$DISK/$cell" ] || { echo "== $DISK/$cell exists: this does not write over an index"; exit 1; }
  done
  mkdir -p "$DISK" || exit 1
  for cell in $CELLS; do
    wait_quiet
    run_cell "$cell" || { echo "== cell $cell did not pass: see $WORK/$LABEL-$cell.log"; stage_verdict; exit 1; }
    stage_verdict
  done
  date "+== %Y-%m-%d %H:%M the cells end"
  [ "$OS" != Darwin ] || power_state
}

stage_verdict() {
  python3 -I -B "$HERE/parallel_inserts_verdict.py" "$OUT" > "$OUT/parallel-inserts-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/parallel-inserts-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin inserts verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, K: $K, cells: $CELLS, disk: $DISK"
for stage in "$@"; do
  case "$stage" in
    bin|inserts|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
