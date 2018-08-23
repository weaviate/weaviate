#!/usr/bin/env bash

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

go run ./cmd/weaviate-server \
  --scheme http \
  --host "127.0.0.1" \
  --port 8080 \
  --config-file=tools/dev/config.json \
  --config janusgraph
