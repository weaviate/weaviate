#!/bin/bash

set -e 

if [[ $1 == --no-restart ]]; then
  echo "Found --no-restart flag, reusing running dependencies from previous run..."
  shift
else
  docker-compose down --remove-orphans
  docker-compose up -d esvector
fi

echo "" > coverage-integration.txt
go test -count 1 -coverprofile=profile.out -tags=integrationTest "$@" ./adapters/repos/...
if [ -f profile.out ]; then
  cat profile.out >> coverage-integration.txt
  rm profile.out
fi
