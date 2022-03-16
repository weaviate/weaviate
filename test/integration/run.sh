#!/bin/bash

set -e 

echo 'BEGIN TEMP DEBUG ON CI'
echo 'run stalling test in isolation'
(cd adapters/repos/db && go test -tags integrationTest -run Halt)

echo 'END TEMP DEBUG ON CI'
norestart=false
includeslow=false

for arg in "$@"; do
  if [[ $arg == --no-restart ]]; then
    norestart=true
    shift
  fi

  if [[ $arg == --include-slow ]]; then
    includeslow=true
    shift
  fi
done

tags=integrationTest
if [ $includeslow = true ]; then
  echo "Found --include-slow flag, running all tests, including the slow ones"
  tags="$tags,integrationTestSlow"
else 
  echo "Found no --include-slow flag, skipping the slow ones"
fi

go test -count 1 -coverpkg=./adapters/repos/... -coverprofile=coverage-integration.txt -race -tags=$tags "$@" ./adapters/repos/...
go test -count 1 -race -tags=$tags "$@" ./usecases/classification/...
