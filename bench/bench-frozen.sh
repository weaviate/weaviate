#!/usr/bin/env bash
# Run one benchmark of a frozen test binary ($BENCH_HOME/bin/tachiomfresh-<version>.test,
# built with `go test -c -tags=nfcorpus`) in a detached tmux session, so a
# version can be measured while the working tree moves on. bench.sh
# compiles and runs the working tree instead.
#
#   bench/bench-frozen.sh <version> <dataset> <frontier|routing|dynamics|muvera|filtered> <out-name> [VAR=value ...]
#
# Same output conventions as bench.sh: $BENCH_HOME/results/x86/runs/<dataset>/<out-name>.txt,
# first line the full environment + command. BENCH_FOREGROUND=1 runs in the
# calling shell (for chaining in a queue).
set -euo pipefail
FORK=$(cd "$(dirname "$0")/.." && pwd)
HERE=$(cd "${BENCH_HOME:-$FORK/..}" && pwd)
VERSION=$1; DATASET=$2; KIND=$3; OUT=$4; shift 4
# the muvera kind runs HFresh + MUVERA from bin/hfresh-<version>.test
case $KIND in
  muvera) BIN=$HERE/bin/hfresh-$VERSION.test ;;
  *) BIN=$HERE/bin/tachiomfresh-$VERSION.test ;;
esac
[ -x "$BIN" ] || { echo "no frozen binary $BIN" >&2; exit 1; }
case $DATASET in
  nfcorpus) CACHE=$HERE/datasets/nfcorpus-colbertv2.bin ;;
  scidocs) CACHE=$HERE/datasets/scidocs-colbertv2.bin ;;
  fiqa) CACHE=$HERE/datasets/fiqa-colbertv2.bin ;;
  # the same corpora encoded with LateOn (tuskanny/<dataset>_lateon)
  nfcorpus-lateon) CACHE=$HERE/datasets/nfcorpus-lateon.bin ;;
  scidocs-lateon) CACHE=$HERE/datasets/scidocs-lateon.bin ;;
  fiqa-lateon) CACHE=$HERE/datasets/fiqa-lateon.bin ;;
  *) echo "unknown dataset $DATASET" >&2; exit 1 ;;
esac
case $KIND in
  frontier) TEST='TestNFCorpusFrontier' ;;
  routing) TEST='TestNFCorpusRoutingQuality' ;;
  dynamics) TEST='TestNFCorpusDynamics' ;;
  muvera) TEST='TestNFCorpusMuvera' ;;
  # the filtered search on a saved index
  filtered) TEST='TestNFCorpusFiltered' ;;
  *) echo "unknown kind $KIND" >&2; exit 1 ;;
esac
# BENCH_RESULTS overrides the results root, relative to the project folder
# (results/x86-lsm for the runs on the object-store harness, same layout)
OUTFILE=$HERE/${BENCH_RESULTS:-results/x86}/runs/$DATASET/$OUT.txt
mkdir -p "$(dirname "$OUTFILE")"
ENV="TACHIOMFRESH_MVC1=$CACHE $*"
CMD="$BIN -test.run '^$TEST\$' -test.v -test.timeout 900m"
RUN="{ echo \"env+command: $ENV $CMD\"; echo \"started: \$(date -Is)\"; env $ENV $CMD 2>&1 | grep --line-buffered -v '^=== RUN\|^    ---'; echo \"finished: \$(date -Is)\"; } > '$OUTFILE' 2>&1"
if [ "${BENCH_FOREGROUND:-}" = 1 ]; then eval "$RUN"; exit 0; fi
SESSION=bench-$DATASET-$OUT
SESSION=${SESSION//[.\/]/-}
tmux new-session -d -s "$SESSION" "$RUN"
echo "started tmux session $SESSION -> $OUTFILE"
