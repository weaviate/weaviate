#!/bin/bash
# Night queue of 2026-11-13 to 14: 10M on wiki-dpr e5 with the fused kernel of
# weaviate/weaviate#13880 in both arms (v1.6).
#
#  1. HNSW, rescoreLimit 512, at 1 and 8 threads, with a second round.
#  2. Posting-edge with the v1.4 default (rescoreLimit 200, margin 0.06,
#     batches of 4), at 1 and 8 threads, with a second round; then, on the
#     same build, the centroid graph search at efs 48, 32, 16 and 8 below the
#     default 64, untimed at ef=20 and ef=160 (the first efs at recall 0.95
#     and 0.99), and the plan timed with the lowest one that keeps the recall.
#
# usage: WORK=<dir> caffeinate -s -i run_v16_night.sh <HHMM not before> <HHMM not after>
#
# Launched in the day from a machine fully awake, so that caffeinate holds it
# awake until the night: -s keeps the system from sleeping on AC, which -i
# alone does not do from a maintenance wake. It waits for the window, for no
# other benchmark binary to be running, and for 15 minutes without keyboard
# or trackpad, and gives up if the window passes without that.
set -u
source "$(cd "$(dirname "$0")" && pwd)/v16_lib.sh"

hid_idle() { ioreg -c IOHIDSystem | awk '/HIDIdleTime/ {print int($NF/1000000000); exit}'; }
awake() { pmset -g systemstate | grep -q 'Capabilities are:.*Graphics'; }
others() { pgrep -f 'postingedge-v1[0-9a-z]*\.test|hnsw-work\.test' > /dev/null; }
# inside the window, which crosses midnight when the first time is the later
in_window() {
  local now
  now=$(date +%H%M)
  if [ "$1" -le "$2" ]; then
    [ "$now" -ge "$1" ] && [ "$now" -lt "$2" ]
  else
    [ "$now" -ge "$1" ] || [ "$now" -lt "$2" ]
  fi
}
power_state() {
  echo "-- power: $(pmset -g systemstate | tr '\n' ' ')"
  pmset -g assertions | grep -E 'caffeinate' | sed 's/^ */-- assertion: /'
  echo "-- keyboard and trackpad idle for $(hid_idle) s"
}
# what happened to the machine since the given time (YYYY-MM-DD HH:MM:SS)
power_log_since() {
  pmset -g log | awk -v since="$1" '/^20[0-9][0-9]-/ && substr($0,1,19) >= since {
      if ($4 == "Sleep" || $4 == "DarkWake" || ($4 == "Wake" && $5 != "Requests") || /Display is turned/ ||
          (/(Created|TimedOut) UserIsActive/ && !/Loginwindow/)) print }' \
    | cut -c1-150 | sed 's/^/-- log: /'
}

date "+== %Y-%m-%d %H:%M queued, waiting for $1 to $2, no other benchmark and 15 idle minutes"
power_state
pmset -g assertions | grep -A3 'caffeinate' | grep -q PreventSystemSleep \
  || echo "== warning: no PreventSystemSleep assertion of caffeinate seen"
[ -x "$PE" ] || { echo "== no binary at $PE"; exit 1; }

entered=0
while true; do
  if in_window "$1" "$2"; then
    entered=1
    if others; then
      sleep 60
      continue
    fi
    [ "$(hid_idle)" -ge 900 ] && break
  elif [ "$entered" = 1 ]; then
    date "+== %H:%M the window passed without 15 idle minutes, nothing run"
    exit 1
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
date "+== %H:%M night queue starts"
power_state

label=wikidpr-10m-hnsw
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D10M" -bench.arm hnsw -bench.queries 1000 -bench.runs 3 -bench.repeat \
  -bench.efs 10,16,24,32,48,64,96,128,192,256,384,512,768,1024 \
  -bench.sweep "rescore=512 threads=1,8" \
  -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label
power_log_since "$START"

label=wikidpr-10m-pe
date "+== %H:%M start $label"
"$PE" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D10M" -bench.arm postingedge -bench.queries 1000 -bench.runs 3 -bench.repeat \
  "${PE10M[@]}" -bench.ceiling 0.99 \
  -bench.centroidefs 48,32,16,8 -bench.centroidpoints 20,160 -bench.centroidmaxdrop 0.0005 \
  -bench.sweep "$GRID threads=1,8 rescore=200 batch=4 margin=0.06" \
  -bench.out "$OUT/$label.json" > "$WORK/$label.log" 2>&1
summary $label

date "+== %H:%M end"
power_log_since "$START"
