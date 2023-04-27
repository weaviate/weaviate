#!/bin/sh

# Check if a name parameter is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <name> [-p | --png]"
  exit 1
fi

NAME=$1
SAVE_PNG=false

# Check for -p or --png flag
if [ "$2" = "-p" ] || [ "$2" = "--png" ]; then
  SAVE_PNG=true
fi

if $SAVE_PNG; then
  go tool pprof -png -lines http://localhost:6060/debug/pprof/heap > "${NAME}_heap.png" &
  go tool pprof -png http://localhost:6060/debug/pprof/profile\?seconds\=30 > "${NAME}_profile.png" &
  go tool pprof -png http://localhost:6060/debug/pprof/allocs > "${NAME}_allocs.png" &

  wait

  open "${NAME}_heap.png"
  open "${NAME}_profile.png"
  open "${NAME}_allocs.png"
else
  curl http://127.0.0.1:6060/debug/pprof/allocs\?seconds=30 > "${NAME}_allocs.prof"
  curl http://127.0.0.1:6060/debug/pprof/heap\?seconds=30 > "${NAME}_heap.prof"
  curl http://127.0.0.1:6060/debug/pprof/profile\?seconds=30 > "${NAME}_profile.prof"

# Check if the FlameGraph directory exists
if [ -d "./FlameGraph" ]; then
  go tool pprof -raw -output="${NAME}_cpu.txt" 'http://localhost:6060/debug/pprof/profile?seconds=30'
  # Checkout the FlameGraph git repository for really nice flame graphs
  # git checkout https://github.com/brendangregg/FlameGraph
  ./FlameGraph/stackcollapse-go.pl "${NAME}_cpu.txt" | ./FlameGraph/flamegraph.pl > "${NAME}_flame.svg" &
  open "${NAME}_flame.svg"
else
  echo "FlameGraph directory not found. Please clone the FlameGraph repository at https://github.com/brendangregg/FlameGraph to get nice flamegraphs."
fi
 
fi


