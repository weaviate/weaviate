#!/bin/bash
# Night queue of 2026-11-12 to 13.
#
#  1. The cap on the edge degree at 10M (wiki-dpr e5, v1.5): one build with
#     the v1.4 default (S=236, P=320 over the cached P=384 assignment, R=256,
#     full code), measured on the same build with the full edges, capped to
#     the 10 and to the 8 nearest edges of every vector, and with the full
#     edges again, at rescoreLimit 200 on 1 and 8 threads.
#  2. The 8-thread curve of the dbpedia control from ef 160 to 640, alone,
#     with its 1-thread curve as the anchor, with the binary of the control.
#     The last attempt: it is not queued again.
#
# usage: caffeinate -s -i run_v15_night.sh <HHMM not before> <HHMM not after>
#
# Launched in the day from a machine fully awake, so that caffeinate holds it
# awake until the night: -s keeps the system from sleeping on AC, which -i
# alone does not do from a maintenance wake (the morning of 2026-11-12). It
# waits for the window and for 15 minutes without keyboard or trackpad, and
# gives up if the window passes without that.
set -u
source "$(cd "$(dirname "$0")" && pwd)/v14_lib.sh"
BIN="$S/postingedge-v15.test"
OUT15="$(dirname "$OUT")/v1.5"
V12="$(dirname "$OUT")/v1.2"
mkdir -p "$OUT15"

hid_idle() { ioreg -c IOHIDSystem | awk '/HIDIdleTime/ {print int($NF/1000000000); exit}'; }
awake() { pmset -g systemstate | grep -q 'Capabilities are:.*Graphics'; }
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

date "+== %Y-%m-%d %H:%M queued, waiting for $1 to $2 and 15 idle minutes"
power_state
pmset -g assertions | grep -q 'PreventSystemSleep.*caffeinate\|caffeinate.*PreventSystemSleep' \
  || pmset -g assertions | grep -A3 'caffeinate' | grep -q PreventSystemSleep \
  || echo "== warning: no PreventSystemSleep assertion of caffeinate seen"

entered=0
while true; do
  if in_window "$1" "$2"; then
    entered=1
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

label=wikidpr-10m-degree
date "+== %H:%M start $label"
"$BIN" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D10M" -bench.arm postingedge -bench.queries 1000 -bench.runs 3 -bench.repeat \
  -bench.posting 236 -bench.probe 320 -bench.cacheprobe 384 -bench.codebits 0 -bench.rescue 256 \
  -bench.minedgerecall 0.94 -bench.workthreads 8 -bench.ceiling 0.99 \
  -bench.streamtrain -bench.buildcache "$CACHE" \
  -bench.degreecaps 0,10,8,0 -bench.capmaxdrop 0.01 \
  -bench.sweep "$GRID threads=1,8 rescore=200 batch=4 margin=0.06" \
  -bench.out "$OUT15/$label.json" > "$S/$label.log" 2>&1
echo "== $label: $(tail -1 "$S/$label.log")"
grep -E "bench_test.go.*(build:|memory:|coverage|cap [0-9]+:|ceiling|ef=)|FAIL|panic|fatal" "$S/$label.log" \
  | sed -E 's/.*bench_test.go:[0-9]+: //; s/ p50=[^|]*//; s/\| rounds ([0-9.]+) \(p95 ([0-9]+)\) loads ([0-9.\/]+).*endpoints ([0-9]+).*/| rounds \1 p95 \2 loads \3 endpoints \4/' \
  | cut -c1-400
power_log_since "$START"

label=dbpedia-1m-control-8threads-last
D="$HOME/Documents/datasets/dbpedia-openai-1000k-angular.bin"
date "+== %H:%M start $label"
"$S/postingedge-c477572ae2.test" -test.run 'TestBenchmark$' -test.timeout 0 -test.v \
  -bench.dataset "$D" -bench.arm postingedge -bench.queries 5000 -bench.runs 3 \
  -bench.posting 236 -bench.probe 256 -bench.codebits 0 -bench.rescue 64 \
  -bench.minedgerecall 0.955 -bench.workthreads 8 \
  -bench.sweep "$GRID threads=1 rescore=64 batch=4 margin=0.06" \
  -bench.sweep "ef=160,320,640 threads=8 rescore=64 batch=4 margin=0.06" \
  -bench.out "$V12/$label.json" > "$S/$label.log" 2>&1
echo "== $label: $(tail -1 "$S/$label.log")"
grep -E "bench_test.go.*(build:|ef=)|FAIL|panic|fatal" "$S/$label.log" \
  | sed -E 's/.*bench_test.go:[0-9]+: //; s/ p50=[^|]*//; s/\| rounds.*//' | cut -c1-160

date "+== %H:%M end"
power_log_since "$START"
