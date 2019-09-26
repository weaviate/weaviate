#!/bin/bash

set -e 

if [[ $1 == --no-restart ]]; then
  echo "Found --no-restart flag, reusing running dependencies from previous run..."
  shift
else
  docker-compose down --remove-orphans
  docker-compose up -d esvector
fi

go test -count 1 -tags=integrationTest "$@" ./adapters/repos/...
