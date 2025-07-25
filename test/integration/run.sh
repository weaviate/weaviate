#!/bin/bash

set -e 

function echo_yellow() {
  yellow='\033[0;33m'
  nc='\033[0m'
  echo -e "${yellow}${*}${nc}"
}

function to_pkgs_string() {
  local pkgs=""
  for pkg in "$@"; do
    pkgs="$pkgs ./$pkg/..."
  done
  echo "$pkgs"
}

export DISABLE_RECOVERY_ON_PANIC=true 

includeslow=false
onlyslowpkgs=false
onlyfastpkgs=false

for arg in "$@"; do
  if [[ $arg == --include-slow ]]; then
    includeslow=true
    shift
  fi
  if [[ $arg == --only-slow-pkgs ]]; then
    onlyslowpkgs=true
    shift
  fi
  if [[ $arg == --only-fast-pkgs ]]; then
    onlyfastpkgs=true
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
if [ $onlyfastpkgs = true ]; then
  echo_yellow "Running integration tests without /adapters/repos/db package"
  pkgs=$(go list ./adapters/repos/... | grep -v "/adapters/repos/db$")
elif [ $onlyslowpkgs = true ]; then
  echo_yellow "Running only slow integration tests"
  pkgs=$(go list ./adapters/repos/... | grep "/adapters/repos/db$")
fi


echo_yellow "Run the regular integration tests with race detector ON"
go test $pkgs -count 1 -timeout 3000s -coverpkg=./adapters/repos/... -coverprofile=coverage-integration.txt -race -tags=$tags "$@" ./adapters/repos/...
echo_yellow "Run the !race integration tests with race detector OFF"
go test $pkgs -count 1 -coverpkg=./adapters/repos/... -tags=$tags "$@" -run Test_NoRace ./adapters/repos/...
echo_yellow "Run the classification integration tests with race detector ON"
go test -count 1 -race -tags=$tags "$@" ./usecases/classification/...
