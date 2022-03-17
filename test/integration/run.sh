#!/bin/bash

set -e 

function echo_yellow() {
  yellow='\033[0;33m'
  nc='\033[0m'
  echo -e "${yellow}${*}${nc}"
}

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

echo_yellow "Run the regular integration tests with race detector ON"
go test -count 1 -coverpkg=./adapters/repos/... -coverprofile=coverage-integration.txt -race -tags=$tags "$@" ./adapters/repos/...
echo_yellow "Run the !race integration tests with race detector OFF"
go test -count 1 -coverpkg=./adapters/repos/... -tags=$tags "$@" -run Test_NoRace ./adapters/repos/...
echo_yellow "Run the classification integration tests with race detector ON"
go test -count 1 -race -tags=$tags "$@" ./usecases/classification/...
