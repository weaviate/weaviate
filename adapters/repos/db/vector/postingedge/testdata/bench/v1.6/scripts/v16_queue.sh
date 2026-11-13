# Sourced by the v1.6 chains queued after run_v16_night.sh: what a chain waits
# on and what it leaves in its output about the machine. The same functions as
# run_v16_night.sh, which was already queued when these were split out: bash
# reads a running script as it goes, so that one is not edited.

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
