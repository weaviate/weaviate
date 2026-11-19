#!/bin/bash
# v4.0, the seventh amendment (pre-registered 2026-11-19): HFresh as a node
# runs it, next to the two cells of the posting-edge index (bench-report.md,
# "HFresh como en un nodo, junto a las dos celdas"). The benchmark of v4.0
# left HFresh worse than a node in two things: it never cleaned the deleted
# centroids out of its centroid graph, which a shard does every 5 minutes, and
# it gave every query a concurrency budget of 8 where the admission limiter of
# a node grants 20 to a query alone. Here the index of v4.0 is copied, its
# graph is cleaned, its best configurations are found again, and it is timed
# with the budget of the limiter, in one session with the cells of 512 bits
# in memory and of the halo, timed with that budget too.
#
# The stages, in this order when none is named:
#   bin      builds the two benchmark binaries into $WORK.
#   clean    copies the HFresh index of v4.0 to $DISK/hf-node, cleans its
#            centroid graph, and measures its recall by searchProbe again.
#            Untimed. The manifest of the cleaned index goes to $OUT.
#   count    what the store reads per query for the cleaned index, which
#            tells the best configuration of every recall, and the heap
#            profile of the cleaned index. Untimed.
#   timed    times every curve of $CURVES, one per process, in the rounds of
#            $ROUNDS.
#   verdict  writes the tables of the session to $OUT/node-verdict.md.
#
# usage: [VAR=value ...] run_v40_node.sh [stage ...]
#   DATASET  the dataset directory. Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     the directory of the indexes of v4.0: $DISK/pe and $DISK/hf.
#   V40      where the manifests of v4.0 are. Default: testdata/bench/v4.0.
#   OUT      where the manifest, the results and the verdict go. Default:
#            $V40/node.
#   WORK     where the binaries and the logs go. Default: $OUT/work.
#   HF_SRC, HF_MANIFEST  the HFresh index to copy and its manifest. Default:
#            $DISK/hf and that of v4.0.
#   PE_DISK, PE_MANIFEST  the cells of the posting-edge index and their
#            manifest. Default: $DISK/pe and that of v4.0.
#   COLD     volume (DISK is on an APFS volume of its own, VOLUME_DEV mounted
#            at VOLUME_MOUNT, unmounted and mounted again before every cold
#            batch) or none (no cold measure). Default none.
#   CURVES   the curves to time. Default: hfresh-best95 hfresh-best95-par
#            hfresh-best99 hfresh-best99-par r512 halo512.
#   READERS  the postings a -par curve reads at once. Default 8.
#   ROUNDS   the rounds of the timed session. Default: n1 n2.
#   IDLE, NIGHT, AFTER  as run_v41.sh: the gate of the timed stage.
#   QUERIES (5000) and COLDQ (500): the queries of a warm pass and the cold
#            queries of every point.
#   EXTRA_HF more flags for the HFresh binary in the untimed stages. A smoke:
#            EXTRA_HF="-bench.hfresh.limit 30000" QUERIES=500 COLDQ=40
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../.." && pwd)"                   # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
V40="${V40:-$(cd "$HERE/.." && pwd)}"
OUT="${OUT:-$V40/node}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin}"
QUERIES="${QUERIES:-5000}"
COLDQ="${COLDQ:-500}"
COLD="${COLD:-none}"
NIGHT="${NIGHT:-0}"
AFTER="${AFTER:-0}"
if [ "$NIGHT" = 1 ]; then IDLE="${IDLE:-900}"; else IDLE="${IDLE:-0}"; fi
ROUNDS="${ROUNDS:-n1 n2}"
READERS="${READERS:-8}"
ALL_CURVES="hfresh-best95 hfresh-best95-par hfresh-best99 hfresh-best99-par r512 halo512"
CURVES="${CURVES:-$ALL_CURVES}"
OS="$(uname)"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory of the indexes of v4.0"; exit 1; }
HF_SRC="${HF_SRC:-$DISK/hf}"
HF_MANIFEST="${HF_MANIFEST:-$V40/dbpedia-1m-disk-hfresh-manifest.json}"
PE_DISK="${PE_DISK:-$DISK/pe}"
PE_MANIFEST="${PE_MANIFEST:-$V40/dbpedia-1m-disk-manifest.json}"
HF_DIR="$DISK/hf-node"
PE="$WORK/postingedge-v40n.test"
HF="$WORK/hfresh-v40n.test"
LABEL="dbpedia-1m-node"
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
  date "+== %H:%M building the binaries"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/ \
    && go test -tags postingedgebench -c -o "$HF" ./adapters/repos/db/vector/hfresh/) || exit 1
}

stage_clean() {
  [ ! -e "$HF_DIR" ] || { echo "== $HF_DIR exists: this does not write over an index"; exit 1; }
  date "+== %H:%M hfresh: a copy of $HF_SRC, its centroid graph cleaned, and its recall by searchProbe again"
  # A clone where the file system has them: no data is copied.
  if [ "$OS" = Darwin ]; then cp -Rc "$HF_SRC" "$HF_DIR"; else cp -R "$HF_SRC" "$HF_DIR"; fi || exit 1
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskClean$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_DIR" \
    -bench.hfresh.manifest "$HF_MANIFEST" -bench.hfresh.out "$OUT/$LABEL-hfresh-manifest.json" \
    ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-clean.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-clean.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-clean.log" || { echo "== the clean stage did not pass: see $WORK/$LABEL-hfresh-clean.log"; exit 1; }
}

stage_count() {
  date "+== %H:%M hfresh: what the store reads per query, over the cleaned index"
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskCount$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_DIR" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.out "$OUT/$LABEL-hfresh-reads.json" \
    ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-count.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-count.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-count.log" || { echo "== the count stage did not pass: see $WORK/$LABEL-hfresh-count.log"; exit 1; }
  date "+== %H:%M hfresh: the heap profile of the cleaned index"
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskTimed$' -test.timeout 0 -test.v \
    -test.memprofilerate=1 -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_DIR" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.reads "$OUT/$LABEL-hfresh-reads.json" \
    -bench.hfresh.points 0.95 -bench.hfresh.out "$WORK/$LABEL-hfresh-heap.json" \
    -bench.hfresh.heap "$WORK/$LABEL-hfresh.heap" ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-heap.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-heap.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-heap.log" || { echo "== the heap profile did not pass: see $WORK/$LABEL-hfresh-heap.log"; exit 1; }
}

# timed_cell <cell> <round>: one cell of posting-edge, at its two recalls
timed_cell() {
  (cd "$PKG" && "$PE" -test.run 'TestDiskTimed$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.diskdir "$PE_DISK" -bench.diskmanifest "$PE_MANIFEST" \
    -bench.diskcell "$1" -bench.diskdrop "$DROP" -bench.diskcold "$COLDQ" -bench.diskadmission \
    -bench.out "$OUT/$LABEL-$2-$1.json" > "$WORK/$LABEL-$2-$1.log" 2>&1)
  lines "$WORK/$LABEL-$2-$1.log" "cell "
  passed "$WORK/$LABEL-$2-$1.log"
}

# timed_hfresh <curve> <round>: the best configuration of HFresh for one
# recall, with its postings read one by one or, for a -par curve, $READERS at
# a time
timed_hfresh() {
  local which readers=1
  case "$1" in *-par) readers="$READERS" ;; esac
  case "${1%-par}" in
    hfresh-best95)  which=0.95 ;;
    hfresh-best99)  which=0.99 ;;
  esac
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskTimed$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_DIR" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.reads "$OUT/$LABEL-hfresh-reads.json" \
    -bench.hfresh.points "$which" -bench.hfresh.readers "$readers" -bench.hfresh.drop "$DROP" -bench.hfresh.cold "$COLDQ" \
    -bench.hfresh.admission -bench.hfresh.out "$OUT/$LABEL-$2-$1.json" > "$WORK/$LABEL-$2-$1.log" 2>&1)
  lines "$WORK/$LABEL-$2-$1.log" "bench_disk_test"
  passed "$WORK/$LABEL-$2-$1.log"
}

# stretch <round> <curve>: times one curve and leaves a line about its
# stretch in $STRETCHES, as run_v40_disk.sh does.
stretch() {
  local round="$1" curve="$2" start begin secs ok=1 idle0=-1 idle1=-1 events=0 logged
  [ -e "$STRETCHES" ] || printf 'round\tcurve\tstart\tend\tseconds\tidle at start\tidle at end\tevents\tpassed\n' > "$STRETCHES"
  [ "$OS" != Darwin ] || idle0=$(hid_idle)
  start="$(date '+%Y-%m-%d %H:%M:%S')"
  begin=$(date +%s)
  date "+== %H:%M $round, $curve"
  case "$curve" in
    hfresh-*) timed_hfresh "$curve" "$round" || ok=0 ;;
    *)        timed_cell "$curve" "$round" || ok=0 ;;
  esac
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
}

# wait_quiet: nothing else of these benchmarks may be running.
wait_quiet() {
  [ "$OS" = Darwin ] || return 0
  if [ "$NIGHT" = 1 ] || [ "$IDLE" -gt 0 ]; then
    date "+== %Y-%m-%d %H:%M waiting for $IDLE s without keyboard or trackpad$([ "$NIGHT" = 1 ] && [ "$AFTER" != 1 ] && echo ', in the window of 23:00 to 03:00')"
    until { [ "$NIGHT" != 1 ] || [ "$AFTER" = 1 ] || in_window 2300 0300; } \
      && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|run_v40_disk\.sh|run_v50_timed\.sh|run_v51\.sh|run_hfresh_inserts\.sh|run_v41\.sh' > /dev/null \
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
  local curve round
  for curve in $CURVES; do
    case " $ALL_CURVES " in *" $curve "*) ;; *) echo "== unknown curve $curve"; exit 1 ;; esac
  done
  [ -e "$OUT/$LABEL-hfresh-reads.json" ] || { echo "== no best configurations of the cleaned index: the clean and count stages come first"; exit 1; }
  wait_quiet
  START="$(date '+%Y-%m-%d %H:%M:%S')"
  date "+== %Y-%m-%d %H:%M the timed session starts, cold: $COLD, rounds: $ROUNDS, curves: $CURVES"
  for round in $ROUNDS; do
    date "+== %Y-%m-%d %H:%M round $round"
    for curve in $CURVES; do
      stretch "$round" "$curve"
    done
    # what there is so far: the session may be read before it ends
    stage_verdict
  done
  date "+== %Y-%m-%d %H:%M the timed session ends"
  if [ "$OS" = Darwin ]; then
    power_state
    power_log_since "$START"
  fi
}

stage_verdict() {
  python3 -I -B "$HERE/node_verdict.py" "$OUT" "$V40" > "$OUT/node-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/node-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin clean count timed verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD, disk: $DISK"
for stage in "$@"; do
  case "$stage" in
    bin|clean|count|timed|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
