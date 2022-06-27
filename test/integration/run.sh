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
# ( cd adapters/repos && go test -v -count 1 -timeout 3000s -coverpkg=./adapters/repos/... -coverprofile=coverage-integration.txt -race -tags=$tags "$@")
( cd adapters/repos/db && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/aggregator/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/clusterintegrationtest/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/docid/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/helpers/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/indexcounter/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/inverted/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/lsmkv/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/lsmkv/segmentindex/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/propertyspecific/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/refcache/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/sorter/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/vector/geo/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
( cd adapters/repos/db/vector/hnsw/ && go test -v -count 1 -timeout 3000s -race -tags=$tags "$@")
echo_yellow "Run the !race integration tests with race detector OFF"
go test -count 1 -coverpkg=./adapters/repos/... -tags=$tags "$@" -run Test_NoRace ./adapters/repos/...
echo_yellow "Run the classification integration tests with race detector ON"
go test -count 1 -race -tags=$tags "$@" ./usecases/classification/...
