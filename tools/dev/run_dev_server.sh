#!/usr/bin/env bash

CONFIG=${1:-janusgraph}

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

export GO111MODULE=on

export DEVELOPMENT_UI=on
go run ./cmd/weaviate-server \
  --scheme http \
  --host "127.0.0.1" \
  --port 8080 \
  --config-file=tools/dev/config.yaml \
  --config "$CONFIG"
