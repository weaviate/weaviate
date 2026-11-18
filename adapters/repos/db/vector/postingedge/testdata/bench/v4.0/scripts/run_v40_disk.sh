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
#            and sweeps its searchProbe for the recall it reaches. Untimed.
#   count    counts what the store reads per query for HFresh. Untimed.
#   timed    times every cell and HFresh, one process each: warm and, unless
#            COLD=none, cold. Then the first cell again, warm, to see drift.
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
#              image    $DISK is a disk image at $IMAGE (created if missing,
#                       $IMAGE_GB large), detached and attached again (macOS,
#                       no root). Default on macOS. A disk image serves one
#                       read at a time: see the amendment in bench-report.md.
#              cmd      the command in $DROP, e.g. as root
#                       'sync; echo 3 > /proc/sys/vm/drop_caches', or 'purge'.
#              none     no cold measure.
#   NIGHT    1 makes the timed stage wait for the window of 23:00 to 03:00
#            with the machine awake, 15 minutes without keyboard or trackpad
#            and no other benchmark running (macOS). Default 0.
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
OS="$(uname)"
if [ "$OS" = Darwin ]; then COLD="${COLD:-image}"; else COLD="${COLD:-fadvise}"; fi
IMAGE="${IMAGE:-$WORK/v40.sparseimage}"
IMAGE_GB="${IMAGE_GB:-40}"
[ -n "${DISK:-}" ] || { echo "== DISK is not set: the directory the indexes are written under"; exit 1; }
CELLS="${CELLS:-full r512 r384 halo512 halo512p}"
PE="$WORK/postingedge-v40.test"
HF="$WORK/hfresh-v40.test"
LABEL="dbpedia-1m-disk"
mkdir -p "$WORK" "$OUT" || exit 1
export GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"

case "$COLD" in
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

# timed_cell <cell> <result name> <drop command>
timed_cell() {
  date "+== %H:%M posting-edge, cell $1"
  (cd "$PKG" && "$PE" -test.run 'TestDiskTimed$' -test.timeout 0 -test.v -bench.dataset "$DATASET" \
    -bench.queries "$QUERIES" -bench.diskdir "$DISK/pe" -bench.diskmanifest "$OUT/$LABEL-manifest.json" \
    -bench.diskcell "$1" -bench.diskdrop "$3" -bench.diskcold "$COLDQ" -bench.out "$OUT/$LABEL-$2.json" \
    > "$WORK/$LABEL-$2.log" 2>&1)
  lines "$WORK/$LABEL-$2.log" "cell "
  passed "$WORK/$LABEL-$2.log" || echo "== cell $1 did not pass: see $WORK/$LABEL-$2.log"
}

stage_timed() {
  attach_image
  if [ "$OS" = Darwin ]; then
    # shellcheck source=/dev/null
    source "$HERE/../../v1.6/scripts/v16_queue.sh"
    if [ "$NIGHT" = 1 ]; then
      date "+== %Y-%m-%d %H:%M waiting for the window of 23:00 to 03:00"
      until in_window 2300 0300 && ! pgrep -f 'postingedge-v[0-9a-z]*\.test|hfresh-v[0-9a-z]*\.test|hnsw-work\.test' > /dev/null \
        && [ "$(hid_idle)" -ge 900 ]; do
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
  date "+== %Y-%m-%d %H:%M the timed session starts, cold: $COLD"
  first=""
  for cell in $CELLS; do
    [ -n "$first" ] || first="$cell"
    timed_cell "$cell" "$cell" "$DROP"
  done
  date "+== %H:%M hfresh"
  (cd "$REPO/adapters/repos/db/vector/hfresh" && "$HF" -test.run 'TestHFreshDiskTimed$' -test.timeout 0 -test.v \
    -bench.hfresh.dataset "$DATASET" -bench.hfresh.queries "$QUERIES" -bench.hfresh.dir "$DISK/hf" \
    -bench.hfresh.manifest "$OUT/$LABEL-hfresh-manifest.json" -bench.hfresh.drop "$DROP" -bench.hfresh.cold "$COLDQ" \
    -bench.hfresh.out "$OUT/$LABEL-hfresh.json" > "$WORK/$LABEL-hfresh.log" 2>&1)
  lines "$WORK/$LABEL-hfresh.log" "bench_disk_test"
  passed "$WORK/$LABEL-hfresh.log" || echo "== hfresh did not pass: see $WORK/$LABEL-hfresh.log"
  # The first cell again, warm only: what the machine drifted in between.
  timed_cell "$first" "$first-again" ""
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
