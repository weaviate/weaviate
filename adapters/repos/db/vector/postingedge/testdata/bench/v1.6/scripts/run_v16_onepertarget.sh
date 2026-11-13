#!/bin/bash
# Queued behind the night chain of 2026-11-13 to 14 (run_v16_night.sh): one
# edge per target posting at 10M, as a filter over one build, timed between an
# anchor before it and one after it (bench-report.md, v1.6, "Una arista por
# posting de destino: prototipo a 10M").
#
# usage: WORK=<dir> caffeinate -s -i run_v16_onepertarget.sh <HHMM not before> <HHMM latest start> [nights]
#   WORK holds postingedge-v16o.test, a binary with -bench.edgecells, built
#   apart so that the binary of the night chain is not touched:
#   go test -tags postingedgebench -c -o $WORK/postingedge-v16o.test ./adapters/repos/db/vector/postingedge/
#
# Launched in the day from a machine fully awake, like the night chain. It
# waits for that chain to end, then for the window, for no other benchmark
# binary to be running, and for 15 minutes without keyboard or trackpad. The
# step takes about 90 minutes, so it does not start after the latest start: a
# night it cannot start in is left for the next one, up to [nights] nights
# (2 unless given).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/v16_lib.sh"
source "$HERE/v16_queue.sh"
PE="$WORK/postingedge-v16o.test"
nights="${3:-2}"

night_chain() { pgrep -f 'run_v16_night\.sh' > /dev/null; }

date "+== %Y-%m-%d %H:%M queued behind the night chain, then $1 to $2 (latest start), no other benchmark and 15 idle minutes, up to $nights nights"
night_chain || echo "== the night chain is not running"
power_state
pmset -g assertions | grep -A3 'caffeinate' | grep -q PreventSystemSleep \
  || echo "== warning: no PreventSystemSleep assertion of caffeinate seen"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }

entered=0
while true; do
  if in_window "$1" "$2"; then
    entered=1
    if ! night_chain && ! others && [ "$(hid_idle)" -ge 900 ]; then
      break
    fi
  elif [ "$entered" = 1 ]; then
    entered=0
    nights=$((nights - 1))
    still=""
    night_chain && still=" (the night chain still running)"
    if [ "$nights" -le 0 ]; then
      date "+== %Y-%m-%d %H:%M the last window passed without a start$still, nothing run"
      exit 1
    fi
    date "+== %Y-%m-%d %H:%M the window passed without a start$still: waiting for the next night"
    power_state
  fi
  sleep 60
done

if ! awake; then
  echo "== not fully awake, declaring user activity for 5 s"
  caffeinate -u -t 5
  sleep 10
fi
awake || { date "+== %H:%M still not fully awake, nothing run"; power_state; exit 1; }
START="$(date '+%Y-%m-%d %H:%M:%S')"
date "+== %Y-%m-%d %H:%M one edge per target starts"
power_state

label=wikidpr-10m-onepertarget
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D10M" -bench.arm postingedge -bench.queries 1000 -bench.runs 3 -bench.repeat \
  "${PE10M[@]}" -bench.edgecells full,onepertarget,full -bench.edgecellfloor 0.99 \
  -bench.sweep "$GRID threads=1,8 rescore=200 batch=4 margin=0.06" \
  -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label
grep -E "edge cell|one edge per target" "$WORK/$label.log" | sed -E 's/.*_test.go:[0-9]+: //' | cut -c1-400
python3 -I "$HERE/onepertarget_verdict.py" "$OUT" > "$OUT/onepertarget-verdict.md" 2>&1 \
  || echo "== the verdict script failed, see $OUT/onepertarget-verdict.md"

date "+== %H:%M end"
power_log_since "$START"
