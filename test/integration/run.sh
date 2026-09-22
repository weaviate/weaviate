#!/bin/bash

set -e

# Optional JUnit XML output for CI result reporting; no-op unless JUNIT_DIR
# is set (see test/tools/gotest_junit.sh).
source "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../tools/gotest_junit.sh"
junit_init

function echo_yellow() {
  yellow='\033[0;33m'
  nc='\033[0m'
  echo -e "${yellow}${*}${nc}"
}

export DISABLE_RECOVERY_ON_PANIC=true 

includeslow=false
onlyvectorpkg=false
withoutvectorpkg=false
onlyslowpkg=false

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
  if [[ $arg == --only-slow-pkg ]]; then
    onlyslowpkg=true
    shift
  fi
done

tags=integrationTest
if [[ $includeslow == true ]]; then
  echo_yellow "Found --include-slow flag, running all tests, including the slow ones"
  tags="$tags,integrationTestSlow"
else 
  echo_yellow "Found no --include-slow flag, skipping the slow ones"
fi

pkgs=""
if [[ $withoutvectorpkg == true ]]; then
  echo_yellow "Running integration tests without adapters/repos/db/vector and adapters/repos/db/integrationslowtest packages"
  pkgs=$(go list ./adapters/repos/... | grep -v "adapters/repos/db/vector" | grep -v "adapters/repos/db/integrationslowtest")
elif [[ $onlyvectorpkg == true ]]; then
  echo_yellow "Running only adapters/repos/db/vector package integration tests"
  pkgs="./adapters/repos/db/vector/..."
elif [[ $onlyslowpkg == true ]]; then
  echo_yellow "Running only adapters/repos/db/integrationslowtest package integration tests"
  pkgs="./adapters/repos/db/integrationslowtest"
fi


echo_yellow "Run the regular integration tests with race detector ON"
go_test $pkgs -count 1 -timeout 3000s -coverpkg=./adapters/repos/... -coverprofile=coverage-integration.txt -race -tags=$tags "$@" ./adapters/repos/...
echo_yellow "Run the !race integration tests with race detector OFF"
go_test $pkgs -count 1 -coverpkg=./adapters/repos/... -tags=$tags "$@" -run Test_NoRace ./adapters/repos/...
if [[ $onlyvectorpkg == false ]] && [[ $onlyslowpkg == false ]]; then
  echo_yellow "Run the classification integration tests with race detector ON"
  go_test -count 1 -race -tags=$tags "$@" ./usecases/classification/...
  # only the tagged tests: the package's unit tests already run in the unit job
  echo_yellow "Run the apikey integration tests with race detector ON"
  apikey_tests=$(grep -lE '^//go:build .*integrationTest' ./usecases/auth/authentication/apikey/*_test.go | xargs grep -hoE '^func Test\w+' | sed 's/^func //' | paste -sd'|' -)
  go_test -count 1 -race -tags=$tags "$@" -run "^($apikey_tests)\$" ./usecases/auth/authentication/apikey
fi
