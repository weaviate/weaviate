#!/usr/bin/env bash

CONFIG=${1:-local-development}

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

export GO111MODULE=on
export DEVELOPMENT_UI=on
export LOG_LEVEL=debug
export LOG_FORMAT=text

CONFIGURATION_STORAGE_TYPE=etcd \
  CONFIGURATION_STORAGE_URL=http://localhost:2379 \
  CONTEXTIONARY_URL=localhost:9999 \
  QUERY_DEFAULTS_LIMIT=20 \
  ORIGIN=http://localhost:8080 \
  AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
  ESVECTOR_URL=http://localhost:9201 \
  go run ./cmd/weaviate-server \
    --scheme http \
    --host "127.0.0.1" \
    --port 8080
