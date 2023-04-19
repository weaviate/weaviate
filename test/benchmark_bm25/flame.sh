#!/bin/sh

# Check if a name parameter is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <name>"
  exit 1
fi

NAME=$1

#go tool pprof -png -lines http://localhost:6060/debug/pprof/heap > "${NAME}_heap.png" &
#go tool pprof -png http://localhost:6060/debug/pprof/profile\?seconds\=30 > "${NAME}_profile.png" &
#go tool pprof -png http://localhost:6060/debug/pprof/allocs > "${NAME}_allocs.png" &

#Write profiles to file instead of pngs
curl localhost:6060/debug/pprof/trace\?seconds=30 > "${NAME}_trace.out" &
curl http://127.0.0.1:6060/debug/pprof/allocs\?seconds=30 > "${NAME}_allocs.prof" &
curl http://127.0.0.1:6060/debug/pprof/heap\?seconds=30 > "${NAME}_heap.prof" &
curl http://127.0.0.1:6060/debug/pprof/profile\?seconds=30 > "${NAME}_profile.prof" &

#go tool pprof -raw -output="${NAME}_cpu.txt" 'http://localhost:6060/debug/pprof/profile?seconds=30'

wait

#./FlameGraph/stackcollapse-go.pl "${NAME}_cpu.txt" | ./FlameGraph/flamegraph.pl > "${NAME}_flame.svg" &
open "${NAME}_heap.png"
open "${NAME}_profile.png"
open "${NAME}_allocs.png"


open "${NAME}_flame.svg"


