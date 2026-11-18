#!/bin/bash
# v4.0 (pre-registered 2026-11-18): the first prototype of the posting-edge
# index that reads from disk, next to a real HFresh, on dbpedia-openai 1M
# (bench-report.md, "v4.0: primer prototipo que lee de disco"). One script for
# the whole run, on macOS and on Linux.
#
# The stages, in this order when none is named:
#   bin      builds the two benchmark binaries into $WORK.
#   write    builds the posting-edge index in batch (S = 236), writes its five
#            cells under $DISK/pe, finds their operating points and applies
#            the two checks that come before any timing: every cell returns
#            from disk what it returns in memory, and what the store reads
#            for it is within a quarter of the byte model of v3.0. Either
#            failing stops the script. Untimed.
#   hfresh   builds the real HFresh, default configuration, under $DISK/hf,
#            and sweeps its searchProbe with every rescore limit of 64, 100,
#            200 and 350 for the recall it reaches. Untimed.
#   count    counts what the store reads per query for HFresh, with its
#            default configuration and with every candidate of the sweep, and
#            picks for each recall the one that reads the least. Untimed.
#   timed    times every curve, one process each, warm and, unless COLD=none,
#            cold: the five cells, and HFresh with its default configuration
#            and with its best for each recall. All of them once and then all
#            of them again, in the same order: the first round is the
#            measure and the second tells drift (the fifth amendment in
#            bench-report.md). Of every curve of every round it leaves when
#            it ran and whether the keyboard or the trackpad were used
#            meanwhile (macOS), in $OUT/dbpedia-1m-disk-stretches.tsv.
#   verdict  writes disk-verdict.md from what the stages left in $OUT.
#
# usage: [VAR=value ...] run_v40_disk.sh [stage ...]
#   DATASET  the dataset directory (meta.txt, train.f32, test.f32,
#            neighbors.i32). Default: ~/Documents/datasets/dbpedia-openai-1000k-angular.bin
#   DISK     where the indexes are written and read: a directory on the disk
#            under test, with some 20 GB free. With COLD=image it is the
#            mountpoint of the disk image.
#   OUT      where the results go. Default: testdata/bench/v4.0. It must not
#            be under $DISK.
#   WORK     where the binaries and the logs go. Default: $OUT/work.
#   COLD     how the system's cache of the index files is dropped before each
#            cold batch, with the index closed:
#              fadvise  posix_fadvise on every file (Linux, no root). Default
#                       on Linux.
#              volume   $DISK is on a volume of its own, mounted at
#                       $VOLUME_MOUNT from the device $VOLUME_DEV, which is
#                       unmounted and mounted again (macOS, no root:
#                       diskutil apfs addVolume disk3 APFS v40bench, then
#                       VOLUME_DEV=disk3s7 VOLUME_MOUNT=/Volumes/v40bench).
#                       It is how the run of the report was timed.
#              image    $DISK is a disk image at $IMAGE (created if missing,
#                       $IMAGE_GB large), detached and attached again (macOS,
#                       no root). Default on macOS, because it needs nothing
#                       set up, but a disk image serves one read at a time:
#                       see the first amendment in bench-report.md.
#              cmd      the command in $DROP, e.g. as root
#                       'sync; echo 3 > /proc/sys/vm/drop_caches', or 'purge'.
#              none     no cold measure.
#   IDLE     the timed stage waits until the keyboard and the trackpad have
#            been idle for this many seconds with no other benchmark running,
#            and then wants the machine fully awake (macOS). Default 0.
#   NIGHT    1 makes the timed stage wait for the window of 23:00 to 03:00
#            besides, and IDLE default to 900 (macOS). Default 0.
#   ROUNDS   the rounds of the timed stage, by name. Default "r1 r2". The
#            repeat of the curves the verdict marks, on the same night:
#            ROUNDS=night CURVES="..." NIGHT=1 run_v40_disk.sh timed verdict
#   CURVES   the curves of the timed stage. Default: every cell of $CELLS,
#            then hfresh-default, hfresh-best95 and hfresh-best99.
#   EXTRA_PE, EXTRA_HF  more flags for the two binaries. A smoke:
#            EXTRA_PE="-bench.limit 30000 -bench.queries 500"
#            EXTRA_HF="-bench.hfresh.limit 30000 -bench.hfresh.queries 500" COLDQ=40
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
if [ "$OS" = Darwin ]; then COLD="${COLD:-image}"; else COLD="${COLD:-fadvise}"; fi
IMAGE="${IMAGE:-$WORK/v40.sparseimage}"
IMAGE_GB="${IMAGE_GB:-40}"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the indexes are written under"; exit 1; }
CELLS="${CELLS:-full r512 r384 halo512 halo512p}"
PE="$WORK/postingedge-v40.test"
HF="$WORK/hfresh-v40.test"
LABEL="dbpedia-1m-disk"
HF_CURVES="hfresh-default hfresh-best95 hfresh-best99"
CURVES="${CURVES:-$CELLS $HF_CURVES}"
STRETCHES="$OUT/$LABEL-stretches.tsv"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"

case "$COLD" in
  volume)  [ -n "${VOLUME_DEV:-}" ] && [ -n "${VOLUME_MOUNT:-}" ] || { echo "== COLD=volume needs VOLUME_DEV and VOLUME_MOUNT"; exit 1; }
           case "$DISK/" in "$VOLUME_MOUNT"/*) ;; *) echo "== DISK must be under $VOLUME_MOUNT"; exit 1 ;; esac
           # The unmount is tried a few times: the system may be reading the
           # volume. If it never goes, the command fails, and so does the run:
           # a batch that was not dropped would be timed as cold.
           DROP="ok=0; for try in 1 2 3 4 5; do diskutil unmount '$VOLUME_MOUNT' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ \$ok = 1 ] && diskutil mount '$VOLUME_DEV' > /dev/null" ;;
  image)   DROP="hdiutil detach '$DISK' -quiet && hdiutil attach '$IMAGE' -mountpoint '$DISK' -nobrowse -quiet" ;;
  fadvise) DROP="python3 -I -B '$HERE/drop_cache.py' '$DISK'" ;;
  cmd)     [ -n "${DROP:-}" ] || { echo "== COLD=cmd needs DROP"; exit 1; } ;;
  none)    DROP="" ;;
  *)       echo "== unknown COLD=$COLD"; exit 1 ;;
esac

# the disk image of COLD=image, attached at $DISK
attach_image() {
  [ "$COLD" = image ] || { mkdir -p "$DISK"; return; }
  if [ ! -e "$IMAGE" ]; then
    hdiutil create -size "${IMAGE_GB}g" -fs APFS -volname v40 -type SPARSE -quiet "$IMAGE" || exit 1
  fi
  mkdir -p "$DISK"
  mount | grep -q " on $DISK " || hdiutil attach "$IMAGE" -mountpoint "$DISK" -nobrowse -quiet || exit 1
}

# passed <log>: the test binary ended with PASS
passed() { [ "$(tail -1 "$1")" = PASS ]; }
# lines <log> <pattern>: what a test logged, without the file and line
lines() { grep -E "$2" "$1" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400; }

stage_bin() {
  date "+== %H:%M building the binaries"
  (cd "$REPO" && go test -tags postingedgebench -c -o "$PE" ./adapters/repos/db/vector/postingedge/ \
    && go test -tags postingedgebench -c -o "$HF" ./adapters/repos/db/vector/hfresh/) || exit 1
}

stage_write() {
  attach_image
  date "+== %H:%M posting-edge: build, write and check, cells $CELLS"
  # shellcheck disable=SC2086
  (cd "$PKG" && "$PE" -test.run 'TestDiskWrite$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.posting 236 -bench.probe 256 -bench.rescue 64 \
    -bench.diskdir "$DISK/pe" -bench.diskcells "${CELLS// /,}" -bench.diskmanifest "$OUT/$LABEL-manifest.json" \
    ${EXTRA_PE:-} > "$WORK/$LABEL-write.log" 2>&1)
  lines "$WORK/$LABEL-write.log" "built|cell |STOP"
  passed "$WORK/$LABEL-write.log" || { echo "== the write stage did not pass: see $WORK/$LABEL-write.log"; exit 1; }
}

stage_hfresh() {
  attach_image
  date "+== %H:%M hfresh: build with the default configuration, and its recall by searchProbe"
  # shellcheck disable=SC2086
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskBuild$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$DISK/hf" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" ${EXTRA_HF:-} > "$WORK/$LABEL-hfresh-build.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-build.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-build.log" || { echo "== the hfresh stage did not pass: see $WORK/$LABEL-hfresh-build.log"; exit 1; }
}

stage_count() {
  attach_image
  date "+== %H:%M hfresh: what the store reads per query"
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskCount$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$DISK/hf" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.out "$OUT/$LABEL-hfresh-reads.json" \
    > "$WORK/$LABEL-hfresh-count.log" 2>&1)
  lines "$WORK/$LABEL-hfresh-count.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh-count.log" || { echo "== the count stage did not pass: see $WORK/$LABEL-hfresh-count.log"; exit 1; }
}

# timed_cell <cell> <round>: one cell of posting-edge, at its two recalls
timed_cell() {
  (cd "$PKG" && "$PE" -test.run 'TestDiskTimed$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.diskdir "$DISK/pe" -bench.diskmanifest "$OUT/$LABEL-manifest.json" \
    -bench.diskcell "$1" -bench.diskdrop "$DROP" -bench.diskcold "$COLDQ" -bench.out "$OUT/$LABEL-$2-$1.json" \
    > "$WORK/$LABEL-$2-$1.log" 2>&1)
  lines "$WORK/$LABEL-$2-$1.log" "cell "
  passed "$WORK/$LABEL-$2-$1.log"
}

# timed_hfresh <curve> <round>: one configuration of HFresh
timed_hfresh() {
  local which
  case "$1" in
    hfresh-default) which=default ;;
    hfresh-best95)  which=0.95 ;;
    hfresh-best99)  which=0.99 ;;
  esac
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskTimed$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$DISK/hf" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.reads "$OUT/$LABEL-hfresh-reads.json" \
    -bench.hfresh.points "$which" -bench.hfresh.drop "$DROP" -bench.hfresh.cold "$COLDQ" \
    -bench.hfresh.out "$OUT/$LABEL-$2-$1.json" > "$WORK/$LABEL-$2-$1.log" 2>&1)
  lines "$WORK/$LABEL-$2-$1.log" "bench_disk_test"
  passed "$WORK/$LABEL-$2-$1.log"
}

# stretch <round> <curve>: times one curve and leaves a line about its
# stretch in $STRETCHES: when it ran, how long the keyboard and the trackpad
# had been idle at its start and at its end, and how many events of power or
# of user activity the system logged meanwhile, which go to the .log next to
# it. The keyboard or the trackpad were used during the stretch when at its
# end they have been idle for less than it lasted.
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
    { echo "== $round, $curve: from $start, $secs s"; [ -z "$logged" ] || printf '%s\n' "$logged"; } >> "${STRETCHES%.tsv}.log"
    [ "$idle1" -ge "$secs" ] || echo "-- keyboard or trackpad used during $round, $curve: idle for $idle1 s at its end, and it took $secs s"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$round" "$curve" "$start" "$(date '+%Y-%m-%d %H:%M:%S')" \
    "$secs" "$idle0" "$idle1" "$events" "$ok" >> "$STRETCHES"
  [ "$ok" = 1 ] || echo "== $round, $curve did not pass: see $WORK/$LABEL-$round-$curve.log"
}

stage_timed() {
  attach_image
  for curve in $CURVES; do
    case " $CELLS $HF_CURVES " in *" $curve "*) ;; *) echo "== unknown curve $curve"; exit 1 ;; esac
  done
  if [ "$OS" = Darwin ]; then
    # shellcheck source=/dev/null
    source "$HERE/../../v1.6/scripts/v16_queue.sh"
    if [ "$NIGHT" = 1 ] || [ "$IDLE" -gt 0 ]; then
      if [ "$NIGHT" = 1 ]; then
        date "+== %Y-%m-%d %H:%M waiting for the window of 23:00 to 03:00 and $IDLE s without keyboard or trackpad"
      else
        date "+== %Y-%m-%d %H:%M waiting for $IDLE s without keyboard or trackpad"
      fi
      until { [ "$NIGHT" != 1 ] || in_window 2300 0300; } \
        && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test' > /dev/null \
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
  fi
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
  python3 -I -B "$HERE/disk_verdict.py" "$OUT" > "$OUT/disk-verdict.md" 2>&1 \
    || echo "== the verdict script failed, see $OUT/disk-verdict.md"
}

[ "$#" -gt 0 ] || set -- bin write hfresh count timed verdict
date "+== %Y-%m-%d %H:%M start: $* | $OS, cold: $COLD, disk: $DISK"
for stage in "$@"; do
  case "$stage" in
    bin|write|hfresh|count|timed|verdict) "stage_$stage" ;;
    *) echo "== unknown stage $stage"; exit 1 ;;
  esac
done
date "+== %H:%M end"
