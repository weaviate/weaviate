#!/usr/bin/env bash
# Launch one benchmark run in a detached tmux session.
#
#   bench/bench.sh <dataset> <frontier|routing|dynamics|muvera|muvera-dynamics> <out-name> [VAR=value ...]
#
# dataset: nfcorpus | scidocs | fiqa | lotte (picks the MVC1 cache under
# $BENCH_HOME/datasets). frontier -> TestNFCorpusFrontier (TachiomFresh),
# muvera -> TestNFCorpusMuvera (HFresh + MUVERA). Output goes to
# $BENCH_HOME/results/x86/runs/<dataset>/<out-name>.txt.
# Extra VAR=value pairs are the grid variables (TACHIOMFRESH_MPS, ...).
#
# The fork is the directory that holds bench/; the project folder
# (BENCH_HOME) defaults to the fork's parent.
#
# Why a script: the test prints its own command line, but not the grid
# variables it was given, so the first line of every output file is the
# full environment + command. tmux so an SSH drop does not kill the run;
# grep --line-buffered so progress is visible (block buffering hides it).
# The tmux session is named after the output file, so `tmux ls` shows what
# is running.
set -euo pipefail
FORK=$(cd "$(dirname "$0")/.." && pwd)
HERE=$(cd "${BENCH_HOME:-$FORK/..}" && pwd)
DATASET=$1; KIND=$2; OUT=$3; shift 3
case $DATASET in
  nfcorpus) CACHE=$HERE/datasets/nfcorpus-colbertv2.bin ;;
  scidocs) CACHE=$HERE/datasets/scidocs-colbertv2.bin ;;
  fiqa) CACHE=$HERE/datasets/fiqa-colbertv2.bin ;;
  lotte) CACHE=$HERE/datasets/lotte-full.bin ;;
  *) echo "unknown dataset $DATASET" >&2; exit 1 ;;
esac
case $KIND in
  frontier) TEST='TestNFCorpusFrontier$'; PKG=./adapters/repos/db/vector/tachiomfresh/... ;;
  routing) TEST='TestNFCorpusRoutingQuality$'; PKG=./adapters/repos/db/vector/tachiomfresh/... ;;
  dynamics) TEST='TestNFCorpusDynamics$'; PKG=./adapters/repos/db/vector/tachiomfresh/... ;;
  muvera) TEST='TestNFCorpusMuvera$'; PKG=./adapters/repos/db/vector/hfresh/... ;;
  muvera-dynamics) TEST='TestNFCorpusMuveraDynamics$'; PKG=./adapters/repos/db/vector/hfresh/... ;;
  *) echo "unknown kind $KIND" >&2; exit 1 ;;
esac
# BENCH_RESULTS overrides the results root, relative to the project folder
# (results/x86-lsm for the runs on the object-store harness, same layout)
OUTFILE=$HERE/${BENCH_RESULTS:-results/x86}/runs/$DATASET/$OUT.txt
mkdir -p "$(dirname "$OUTFILE")"
ENV="TACHIOMFRESH_MVC1=$CACHE $*"
# -count=1: without it go test replays a cached result when the command and
# environment repeat, which is what a second pass over a saved index does
CMD="go test -count=1 -tags=nfcorpus -run '$TEST' -v -timeout 900m $PKG"
SESSION=bench-$DATASET-$OUT
# <out-name> may name a subfolder (e.g. v3.0/frontier); tmux session
# names take neither "." nor "/"
SESSION=${SESSION//[.\/]/-}
# BENCH_FOREGROUND=1 runs the benchmark in the calling shell instead of a
# new tmux session, so a queue script (itself inside tmux) can chain runs
# one at a time.
if [ "${BENCH_FOREGROUND:-}" = 1 ]; then
  cd "$FORK"
  # eval, because $CMD carries the quoted -run pattern: split unevaluated
  # it keeps its quotes and matches no test
  { echo "env+command: cd $FORK && $ENV $CMD"; echo "started: $(date -Is)"; eval "env $ENV $CMD" 2>&1 | grep --line-buffered -v '^=== RUN\|^    ---'; echo "finished: $(date -Is)"; } > "$OUTFILE" 2>&1
  exit 0
fi
tmux new-session -d -s "$SESSION" -c "$FORK" \
  "{ echo \"env+command: cd $FORK && $ENV $CMD\"; echo \"started: \$(date -Is)\"; env $ENV $CMD 2>&1 | grep --line-buffered -v '^=== RUN\|^    ---'; echo \"finished: \$(date -Is)\"; } > '$OUTFILE' 2>&1"
echo "started tmux session $SESSION -> $OUTFILE"
