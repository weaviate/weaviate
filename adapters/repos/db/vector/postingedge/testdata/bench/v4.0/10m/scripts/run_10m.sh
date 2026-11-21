#!/bin/bash
# 10M on disk (bench-report.md, "10M en disco: wiki-dpr 10M con el camino de
# lectura de v4.0"): the two cells of the posting-edge index, with the codes
# its endpoints are ranked by in memory and with them on disk in a halo,
# written in the format of v4.0 over one batch build of 10M vectors of
# wiki-dpr, next to the HFresh of this branch as a node runs it: its centroid
# graph cleaned and the budget of the admission limiter.
#
# The stages, in this order when none is named:
#   bin      builds the two benchmark binaries into $WORK, and a copy of each
#            under another name for the untimed stages.
#   pe1m     untimed: the two cells over 1M of wiki-dpr, written by the same
#            harness, into $DISK/pe-1m: the loads of 1M the second condition
#            is read against, and a trial of the write stage.
#   hfresh   untimed: builds the HFresh index of 10M into $DISK/hf.
#   clean    untimed: a clone of it in $DISK/hf-node with its centroid graph
#            cleaned, and its recall by searchProbe again.
#   count    untimed: what the store reads per query over the cleaned index,
#            which tells the best configuration of every recall, and its heap.
#   pe       untimed: the batch build of 10M and its two cells, into $DISK/pe.
#   timed    times every curve of $CURVES, one per process, in the rounds of
#            $ROUNDS.
#   verdict  writes the tables to $OUT/tenm-verdict.md.
#
# usage: [VAR=value ...] run_10m.sh [stage ...]
#   DATASET  the dataset of 10M. Default: ~/Documents/datasets/wikidpr-10m-e5-inplace.bin
#   DATASET1M  that of 1M. Default: ~/Documents/datasets/wikidpr-1m-e5-gt.bin
#   DISK     the directory the indexes go under, on the disk under test.
#   OUT      where the manifests, the results and the verdict go. Default:
#            testdata/bench/v4.0/10m.
#   WORK     where the binaries and the logs go. Default: $OUT/work.
#   RESCORE  the rescore limit of the cells: the automatic one at 768 bits.
#            Default 150.
#   LADDER   more rescore limits, in ascending order and separated by commas,
#            a cell is looked for with if 0.99 is out of reach with RESCORE.
#            Default 200,400.
#   HF_RESCORES_BUILD, HF_RESCORES  the rescore limits the HFresh index is
#            swept with when it is built, which is only a check, and once it
#            is cleaned, which is where its best configurations come from.
#            Default 350 and 200,350,500,800,1200,2000.
#   PROBE, RESCUE  the build probe and the rescue limit of the build of 10M.
#            Default 320 and 256, as in v1.4.
#   COLD     volume (DISK is on an APFS volume of its own, VOLUME_DEV mounted
#            at VOLUME_MOUNT, unmounted and mounted again before every cold
#            batch) or none (no cold measure). Default none.
#   CURVES   the curves to time. Default: hfresh-best95 hfresh-best95-par
#            hfresh-best99 hfresh-best99-par r512 halo512.
#   READERS  the postings a -par curve reads at once. Default 8.
#   ROUNDS   the rounds of the timed session. Default: n1 n2.
#   IDLE     the timed stage waits until the keyboard and the trackpad have
#            been idle for this many seconds, with no other benchmark and no
#            compilation running. Default 0.
#   QUERIES (1000) and COLDQ (500): the queries of a warm pass and the cold
#            queries of every point.
#   EXTRA_PE, EXTRA_HF  more flags for the two binaries in the untimed stages.
#   LABEL, LABEL1M  the names of the files. Those of the cells of 1M carry
#            their rescore limit besides: $LABEL1M-r$RESCORE. A smoke over
#            the dataset of 1M:
#            DATASET=$DATASET1M LABEL=smoke PROBE=256 RESCUE=64 COLDQ=40
#
# It must not be edited while it runs.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../../../../.." && pwd)"                # the posting-edge package
REPO="$(cd "$PKG/../../../../.." && pwd)"
OUT="${OUT:-$(cd "$HERE/.." && pwd)}"
WORK="${WORK:-$OUT/work}"
DATASET="${DATASET:-$HOME/Documents/datasets/wikidpr-10m-e5-inplace.bin}"
DATASET1M="${DATASET1M:-$HOME/Documents/datasets/wikidpr-1m-e5-gt.bin}"
QUERIES="${QUERIES:-1000}"
COLDQ="${COLDQ:-500}"
COLD="${COLD:-none}"
IDLE="${IDLE:-0}"
ROUNDS="${ROUNDS:-n1 n2}"
READERS="${READERS:-8}"
RESCORE="${RESCORE:-150}"
LADDER="${LADDER:-200,400}"
HF_RESCORES_BUILD="${HF_RESCORES_BUILD:-350}"
HF_RESCORES="${HF_RESCORES:-200,350,500,800,1200,2000}"
PROBE="${PROBE:-320}"
RESCUE="${RESCUE:-256}"
ALL_CURVES="hfresh-best95 hfresh-best95-par hfresh-best99 hfresh-best99-par r512 halo512"
CURVES="${CURVES:-$ALL_CURVES}"
CELLS="r512 halo512"
OS="$(uname)"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the indexes go under"; exit 1; }
HF_SRC="$DISK/hf"
HF_DIR="$DISK/hf-node"
PE_DISK="$DISK/pe"
PE="$WORK/postingedge-v40m.test"
HF="$WORK/hfresh-v40m.test"
# The same binaries under names the timed sessions do not take for theirs.
PEU="$WORK/pe-tenm.test"
HFU="$WORK/pe-tenmhf.test"
LABEL="${LABEL:-wikidpr-10m}"
LABEL1M="${LABEL1M:-wikidpr-1m}"
PE_MANIFEST="$OUT/$LABEL-manifest.json"
STRETCHES="$OUT/$LABEL-stretches.tsv"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-54GiB}"

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
  source "$HERE/../../../v1.6/scripts/v16_queue.sh"
fi

# passed <log>: the test binary ended with PASS
passed() { [ "$(tail -1 "$1")" = PASS ]; }
# lines <log> <pattern>: what a test logged, without the file and line
lines() { grep -E "$2" "$1" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400; }

stage_bin() {
  date "+== %H:%M building the binaries"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/ \
    && go test -tags postingedgebench -c -o "$HF" ./adapters/repos/db/vector/hfresh/) || exit 1
  cp "$PE" "$PEU" && cp "$HF" "$HFU" || exit 1
}

# write_cells <dataset> <dir> <manifest> <log> <probe> <rescue> <ladder>: one
# batch build and its two cells
write_cells() {
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PEU" -test.run 'TestDiskWrite$' -test.timeout 0 -test.v -bench.dataset "$1" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe "$5" -bench.rescue "$6" -bench.streamtrain \
    -bench.halorescore "$RESCORE" -bench.diskrescores "$7" -bench.diskdir "$2" -bench.diskcells "${CELLS// /,}" \
    -bench.diskmanifest "$3" ${EXTRA_PE:-} > "$4" 2>&1)
  lines "$4" "built|train set loaded|cell |index |STOP"
  passed "$4"
}

stage_pe1m() {
  date "+== %Y-%m-%d %H:%M the two cells over 1M, into $DISK/pe-1m, untimed"
  # The cells of 1M are searched with one rescore limit, that of the cells
  # of 10M they are read against: no ladder.
  write_cells "$DATASET1M" "$DISK/pe-1m" "$OUT/$LABEL1M-r$RESCORE-manifest.json" "$WORK/$LABEL1M-r$RESCORE-write.log" 256 64 "" \
    || { echo "== the cells of 1M did not pass: see $WORK/$LABEL1M-r$RESCORE-write.log"; exit 1; }
  stage_verdict
}

stage_pe() {
  date "+== %Y-%m-%d %H:%M the batch build and its two cells, into $PE_DISK, untimed"
  write_cells "$DATASET" "$PE_DISK" "$PE_MANIFEST" "$WORK/$LABEL-write.log" "$PROBE" "$RESCUE" "$LADDER" \
    || { echo "== the cells did not pass: see $WORK/$LABEL-write.log"; exit 1; }
  stage_verdict
}

stage_hfresh() {
  date "+== %Y-%m-%d %H:%M hfresh: the index, into $HF_SRC, untimed"
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HFU" -test.run 'TestHFreshDiskBuild$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_SRC" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-build-manifest.json" -bench.hfresh.rescores "$HF_RESCORES_BUILD" \
    ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-build.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-build.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-build.log" || { echo "== the hfresh stage did not pass: see $WORK/$LABEL-hfresh-build.log"; exit 1; }
}

stage_clean() {
  [ ! -e "$HF_DIR" ] || { echo "== $HF_DIR exists: this does not write over an index"; exit 1; }
  date "+== %Y-%m-%d %H:%M hfresh: a copy of $HF_SRC, its centroid graph cleaned, and its recall by searchProbe again"
  # A clone where the file system has them: no data is copied.
  if [ "$OS" = Darwin ]; then cp -Rc "$HF_SRC" "$HF_DIR"; else cp -R "$HF_SRC" "$HF_DIR"; fi || exit 1
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HFU" -test.run 'TestHFreshDiskClean$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_DIR" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-build-manifest.json" -bench.hfresh.out "$OUT/$LABEL-hfresh-manifest.json" \
    -bench.hfresh.rescores "$HF_RESCORES" ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-clean.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-clean.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-clean.log" || { echo "== the clean stage did not pass: see $WORK/$LABEL-hfresh-clean.log"; exit 1; }
}

stage_count() {
  date "+== %Y-%m-%d %H:%M hfresh: what the store reads per query, over the cleaned index"
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HFU" -test.run 'TestHFreshDiskCount$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_DIR" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.out "$OUT/$LABEL-hfresh-reads.json" \
    ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-count.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-count.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-count.log" || { echo "== the count stage did not pass: see $WORK/$LABEL-hfresh-count.log"; exit 1; }
  date "+== %Y-%m-%d %H:%M hfresh: the heap profile of the cleaned index"
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HFU" -test.run 'TestHFreshDiskTimed$' -test.timeout 0 -test.v \
    -test.memprofilerate=1 -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$HF_DIR" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.reads "$OUT/$LABEL-hfresh-reads.json" \
    -bench.hfresh.points 0.95 -bench.hfresh.out "$WORK/$LABEL-hfresh-heap.json" \
    -bench.hfresh.heap "$WORK/$LABEL-hfresh.heap" ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-heap.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-heap.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-heap.log" || { echo "== the heap profile did not pass: see $WORK/$LABEL-hfresh-heap.log"; exit 1; }
  stage_verdict
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
# stretch in $STRETCHES.
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

# wait_quiet: the gate of the timed stage. Nothing else of these benchmarks
# may be running, and nothing may be compiling or testing on the machine.
wait_quiet() {
  [ "$OS" = Darwin ] || return 0
  if [ "$IDLE" -gt 0 ]; then
    date "+== %Y-%m-%d %H:%M waiting for $IDLE s without keyboard or trackpad"
    until ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test|pe-[a-z0-9]*\.test' > /dev/null \
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
  power_state
}

stage_timed() {
  local curve round
  for curve in $CURVES; do
    case " $ALL_CURVES " in *" $curve "*) ;; *) echo "== unknown curve $curve"; exit 1 ;; esac
  done
  [ -e "$OUT/$LABEL-hfresh-reads.json" ] || { echo "== no best configurations of the cleaned index: the clean and count stages come first"; exit 1; }
  [ -e "$PE_MANIFEST" ] || { echo "== no cells: the stage pe comes first"; exit 1; }
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
  python3 -I -B "$HERE/tenm_verdict.py" "$OUT" "$LABEL" "$LABEL1M" > "$OUT/tenm-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/tenm-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin pe1m hfresh clean count pe timed verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD"
for stage in "$@"; do
  case "$stage" in
    bin|pe1m|hfresh|clean|count|pe|timed|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
