#!/bin/bash

set -e 

function echo_yellow() {
  yellow='\033[0;33m'
  nc='\033[0m'
  echo -e "${yellow}${*}${nc}"
}

export DISABLE_RECOVERY_ON_PANIC=true 

includeslow=false
onlyvectorpkg=false
withoutvectorpkg=false

for arg in "$@"; do
  if [[ $arg == --include-slow ]]; then
    includeslow=true
    shift
  fi
  if [[ $arg == --only-vector-pkg ]]; then
    onlyvectorpkg=true
    shift
  fi
  if [[ $arg == --without-vector-pkg ]]; then
    withoutvectorpkg=true
    shift
  fi
done

tags=integrationTest
if [ $includeslow = true ]; then
  echo_yellow "Found --include-slow flag, running all tests, including the slow ones"
  tags="$tags,integrationTestSlow"
else 
  echo_yellow "Found no --include-slow flag, skipping the slow ones"
fi

pkgs=""
if [ $withoutvectorpkg = true ]; then
  echo_yellow "Running integration tests without adapters/repos/db/vector package"
  pkgs=$(go list ./adapters/repos/... | grep -v "adapters/repos/db/vector")
elif [ $onlyvectorpkg = true ]; then
  echo_yellow "Running only adapters/repos/db/vector package integration tests"
  pkgs="./adapters/repos/db/vector/..."
fi


echo_yellow "Run the regular integration tests with race detector ON"
go test $pkgs -count 1 -timeout 3000s -coverpkg=./adapters/repos/... -coverprofile=coverage-integration.txt -race -tags=$tags "$@" ./adapters/repos/...
echo_yellow "Run the !race integration tests with race detector OFF"
go test $pkgs -count 1 -coverpkg=./adapters/repos/... -tags=$tags "$@" -run Test_NoRace ./adapters/repos/...
echo_yellow "Run the classification integration tests with race detector ON"
go test -count 1 -race -tags=$tags "$@" ./usecases/classification/...
