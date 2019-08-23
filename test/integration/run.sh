#!/bin/bash

set -e 

if [[ $* == *--no-restart* ]]; then
  echo "Found --no-restart flag, reusing running dependencies from previous run..."
else
  docker-compose down --remove-orphans
  docker-compose up -d esvector
fi

go test -count 1 -tags=integrationTest ./adapters/repos/...
