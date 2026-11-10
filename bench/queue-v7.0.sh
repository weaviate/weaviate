#!/usr/bin/env bash
# Compare two frozen TachiomFresh test binaries, v7.0 (frozen here from the
# working tree) and v6.0 (already in $BENCH_HOME/bin), on SCIDOCS
# ColBERTv2, maxPostingSize 64, with token ids: a build run of v7.0, then
# the two binaries on the same grid in B A A B order, each on a fresh copy
# of its own saved index (the two versions' posting formats differ, so a
# binary cannot read the other's index).
#
#   tmux new -d -s queue-v70 bench/queue-v7.0.sh
set -uo pipefail
FORK=$(cd "$(dirname "$0")/.." && pwd)
HERE=$(cd "${BENCH_HOME:-$FORK/..}" && pwd) || exit 1
BENCH=$FORK/bench
export BENCH_RESULTS=results/x86-lsm
LOG=$HERE/results/x86-lsm/runs/queue-v7.0.log
mkdir -p "$(dirname "$LOG")"
log() { echo "$(date -Is) $*" >> "$LOG"; }
OBJ=$HERE/indexes/objects
D=scidocs
GRID=(TACHIOMFRESH_TOKENIDS=$HERE/datasets/scidocs-colbertv2.tokenids.bin TACHIOMFRESH_MPS=64 TACHIOMFRESH_KC=4,8,16,32,64 TACHIOMFRESH_KDOCS=50,100,200,500,1000 TACHIOMFRESH_OBJECT_STORE=$OBJ)

log "queue-v7.0 started (pid $$)"
(cd "$FORK" && go test -c -tags=nfcorpus -o "$HERE/bin/tachiomfresh-v7.0.test" ./adapters/repos/db/vector/tachiomfresh/) || { log "FREEZE FAILED, queue stopped"; exit 1; }
log "frozen bin/tachiomfresh-v7.0.test md5 $(md5sum "$HERE/bin/tachiomfresh-v7.0.test" | cut -c1-8)"

# the build run: builds and saves indexes/scidocs/v7.0-lsm/tachiomfresh-mps64
log "start build run v7.0"
BENCH_FOREGROUND=1 timeout 3h "$BENCH/bench-frozen.sh" v7.0 $D frontier v7.0/frontier "${GRID[@]}" TACHIOMFRESH_INDEX_DIR=$HERE/indexes/$D/v7.0-lsm
log "done build run v7.0: $(grep -cE '^([[:space:]]*[^[:space:]]+\.go:[0-9]+: )?cell' "$HERE/results/x86-lsm/runs/$D/v7.0/frontier.txt") cells, $(grep -o -- '--- PASS\|--- FAIL\|^FAIL\|^ok\|^PASS' "$HERE/results/x86-lsm/runs/$D/v7.0/frontier.txt" | head -1)"

# B A A B: each pass on a fresh copy of its version's saved index
COPIES=$HERE/indexes/$D/baab-v6.0-v7.0
n=0
for V in v7.0 v6.0 v6.0 v7.0; do
  n=$((n+1))
  dst=$COPIES/$n-$V
  rm -rf "$dst"; mkdir -p "$dst"
  cp -r "$HERE/indexes/$D/$V-lsm/tachiomfresh-mps64" "$dst/"
  log "start baab $n $V"
  BENCH_FOREGROUND=1 timeout 3h "$BENCH/bench-frozen.sh" $V $D frontier baab-v6.0-v7.0/$n-$V "${GRID[@]}" TACHIOMFRESH_INDEX_DIR=$dst
  log "done baab $n $V: $(grep -cE '^([[:space:]]*[^[:space:]]+\.go:[0-9]+: )?cell' "$HERE/results/x86-lsm/runs/$D/baab-v6.0-v7.0/$n-$V.txt") cells"
done
log "queue-v7.0 finished"
