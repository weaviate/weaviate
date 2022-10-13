#!/usr/bin/env bash

set -eou pipefail

function main() {
  # This script runs all non-benchmark tests if no CMD switch is given and the respective tests otherwise.
  run_all_tests=true
  run_acceptance_tests=false
  run_module_tests=false
  run_unit_and_integration_tests=false
  run_unit_tests=false
  run_integration_tests=false
  run_benchmark=false

  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --acceptance-only) run_all_tests=false; run_acceptance_tests=true ;;
          --unit-and-integration-only) run_all_tests=false; run_unit_and_integration_tests=true;;
          --unit-only) run_all_tests=false; run_unit_tests=true;;
          --integration-only) run_all_tests=false; run_integration_tests=true;;
          --benchmark-only) run_all_tests=false; run_benchmark=true;;
          --acceptance-module-tests-only) run_all_tests=false; run_module_tests=true; echo $run_module_tests ;;
          *) echo "Unknown parameter passed: $1"; exit 1 ;;
      esac
      shift
  done

  # Jump to root directory
  cd "$( dirname "${BASH_SOURCE[0]}" )"/..

  echo "INFO: In directory $PWD"

  echo "INFO: This script will surpress most output, unless a command ultimately fails"
  echo "      Then it will print the output of the failed command."

  echo_green "Prepare workspace..."

  # Remove data directory in case of previous runs
  rm -rf data
  echo "Done!"

  if $run_unit_and_integration_tests || $run_unit_tests || $run_all_tests
  then
    echo_green "Run all unit tests..."
    run_unit_tests "$@"
    echo_green "Unit tests successful"
  fi

  if $run_unit_and_integration_tests || $run_integration_tests || $run_all_tests
  then
    echo_green "Run integration tests..."
    run_integration_tests "$@"
    echo_green "Integration tests successful"
  fi 

  if $run_acceptance_tests || $run_all_tests || $run_benchmark
  then
    echo "Start docker container needed for acceptance and/or benchmark test"
    echo_green "Stop any running docker-compose containers..."
    suppress_on_success docker compose -f docker-compose-test.yml down --remove-orphans

    echo_green "Start up weaviate and backing dbs in docker-compose..."
    echo "This could take some time..."
    tools/test/run_ci_server.sh

    # echo_green "Import required schema and test fixtures..."
    # # Note: It's not best practice to do this as part of the test script
    # # It would be better if each test independently prepared (and also 
    # # cleaned up) the test fixtures it needs, but one step at a time ;)
    # suppress_on_success import_test_fixtures

    if $run_benchmark
    then
      echo_green "Run performance tracker..."
      ./test/benchmark/run_performance_tracker.sh
    fi

    if $run_acceptance_tests || $run_all_tests
    then
      echo_green "Run acceptance tests..."
      run_acceptance_tests "$@"
    fi
  fi

  if $run_module_tests; then
    local module_test_image=weaviate:module-tests
    echo_green "Running module acceptance tests..."
    echo_green "Stop any running docker-compose containers..."
    suppress_on_success docker compose -f docker-compose-test.yml down --remove-orphans
    echo_green "Building weaviate image for module acceptance tests..."
    echo "This could take some time..."
    GIT_HASH=$(git rev-parse --short HEAD)
    docker build --build-arg GITHASH=$GIT_HASH -t $module_test_image .
    export "TEST_WEAVIATE_IMAGE"=$module_test_image

    run_module_tests "$@"
    echo_green "Module acceptance tests successful"
  fi
  
  echo "Done!"
}

function run_unit_tests() {
  if [[ "$*" == *--acceptance-only* ]]; then
    echo "Skipping unit test"
    return
  fi
  go test -race -coverprofile=coverage-unit.txt -covermode=atomic -count 1 $(go list ./... | grep -v 'test/acceptance' | grep -v 'test/modules') | grep -v '\[no test files\]'
}

function run_integration_tests() {
  if [[ "$*" == *--acceptance-only* ]]; then
    echo "Skipping integration test"
    return
  fi

  ./test/integration/run.sh --include-slow
}

function run_acceptance_tests() {
  # for now we need to run the tests sequentially, there seems to be some sort of issues with running them in parallel
    for pkg in $(go list ./... | grep 'test/acceptance'); do
      if ! go test -count 1 -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
    done
}

function run_module_tests() {
  # for now we need to run the tests sequentially, there seems to be some sort of issues with running them in parallel
    for pkg in $(go list ./... | grep 'test/modules'); do
      if ! go test -v -count 1 -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
    done
}

suppress_on_success() {
  out="$("${@}" 2>&1)" || { echo_red "FAILED!";  echo "$out"; return 1; }
  echo "Done!"
}

function echo_green() {
  green='\033[0;32m'
  nc='\033[0m' 
  echo -e "${green}${*}${nc}"
}

function echo_red() {
  red='\033[0;31m'
  nc='\033[0m' 
  echo -e "${red}${*}${nc}"
}

main "$@"
