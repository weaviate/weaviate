#!/bin/bash

set -eou pipefail

function main() {
  # This script runs all non-benchmark tests if no CMD switch is given and the respective tests otherwise.
  run_all_tests=true
  run_acceptance_tests=false
  run_acceptance_only_fast=false
  run_acceptance_only_python=false
  run_acceptance_graphql_tests=false
  run_acceptance_replication_tests=false
  run_module_tests=false
  only_module=false
  only_module_value=false
  run_unit_and_integration_tests=false
  run_unit_tests=false
  run_integration_tests=false
  run_benchmark=false
  run_module_only_backup_tests=false
  run_module_except_backup_tests=false

  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --unit-only|-u) run_all_tests=false; run_unit_tests=true;;
          --unit-and-integration-only|-ui) run_all_tests=false; run_unit_and_integration_tests=true;;
          --integration-only|-i) run_all_tests=false; run_integration_tests=true;;
          --acceptance-only|--e2e-only|-a) run_all_tests=false; run_acceptance_tests=true ;;
          
          --acceptance-only-fast|-aof) run_all_tests=false; run_acceptance_only_fast=true;;
          --acceptance-only-python|-aop) run_all_tests=false; run_acceptance_only_python=true;;
          --acceptance-only-graphql|-aog) run_all_tests=false; run_acceptance_graphql_tests=true ;;
          --acceptance-only-replication|-aor) run_all_tests=false; run_acceptance_replication_tests=true ;;
          --only-module-*|-om)run_all_tests=false; only_module=true;only_module_value=$1;;
          --acceptance-module-tests-only|--modules-only|-m) run_all_tests=false; run_module_tests=true; run_module_only_backup_tests=true; run_module_except_backup_tests=true;;
          --acceptance-module-tests-only-backup|--modules-backup-only|-mob) run_all_tests=false; run_module_tests=true; run_module_only_backup_tests=true;;
          --acceptance-module-tests-except-backup|--modules-except-backup|-meb) run_all_tests=false; run_module_tests=true; run_module_except_backup_tests=true; echo $run_module_except_backup_tests ;;
          --benchmark-only|-b) run_all_tests=false; run_benchmark=true;;
          --help|-h) printf '%s\n' \
              "Options:"\
              "--unit-only | -u"\
              "--unit-and-integration-only | -ui"\
              "--integration-only | -i"\
              "--acceptance-only | -a"\
              "--acceptance-only-fast | -aof"\
              "--acceptance-only-python | -aop"\
              "--acceptance-only-graphql | -aog"\
              "--acceptance-only-replication| -aor"\
              "--acceptance-module-tests-only | --modules-only | -m"\
              "--acceptance-module-tests-only-backup | --modules-backup-only | -mob"\
              "--acceptance-module-tests-except-backup | --modules-except-backup | -meb"\
              "--only-module-{moduleName}"
              "--benchmark-only | -b" \
              "--help | -h"; exit 1;;
          *) echo "Unknown parameter passed: $1"; exit 1 ;;
      esac
      shift
  done

  # Jump to root directory
  cd "$( dirname "${BASH_SOURCE[0]}" )"/..

  echo "INFO: In directory $PWD"

  echo "INFO: This script will suppress most output, unless a command ultimately fails"
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

  if $run_acceptance_tests  || $run_acceptance_only_fast || $run_acceptance_graphql_tests || $run_acceptance_replication_tests || $run_acceptance_only_python || $run_all_tests || $run_benchmark
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

    if $run_acceptance_tests  || $run_acceptance_only_fast || $run_acceptance_graphql_tests || $run_acceptance_replication_tests || $run_all_tests
    then
      echo_green "Run acceptance tests..."
      run_acceptance_tests "$@"
    fi
  fi

  if $run_acceptance_only_python || $run_all_tests
  then
    echo_green "Run python acceptance tests..."
    ./test/acceptance_with_python/run.sh
    echo_green "Python tests successful"
  fi

  if $only_module; then
    mod=${only_module_value//--only-module-/}
    echo_green "Running module acceptance tests for $mod..."
    for pkg in $(go list ./test/modules/... | grep '/modules/'${mod}); do
      build_docker_image_for_tests
      echo_green "Weaviate image successfully built, run module tests for $mod..."
      if ! go test -count 1 -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
      echo_green "Module acceptance tests for $mod successful"
    done    
  fi
  if $run_module_tests; then
    echo_green "Running module acceptance tests..."
    build_docker_image_for_tests
    echo_green "Weaviate image successfully built, run module tests..."
    run_module_tests "$@"
    echo_green "Module acceptance tests successful"
  fi
  
  echo "Done!"
}

function build_docker_image_for_tests() {
  local module_test_image=weaviate:module-tests
  echo_green "Stop any running docker-compose containers..."
  suppress_on_success docker compose -f docker-compose-test.yml down --remove-orphans
  echo_green "Building weaviate image for module acceptance tests..."
  echo "This could take some time..."
  GIT_HASH=$(git rev-parse --short HEAD)
  docker build --build-arg GITHASH="$GIT_HASH" -t $module_test_image .
  export "TEST_WEAVIATE_IMAGE"=$module_test_image
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
  if $run_acceptance_only_fast || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance fast only"
    run_acceptance_only_fast "$@"
  fi
  if $run_acceptance_graphql_tests || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance graphql"
    run_acceptance_graphql_tests "$@"
  fi
  if $run_acceptance_replication_tests || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance replication"
    run_acceptance_replication_tests "$@"
  fi  
}

function run_acceptance_only_fast() {
  # needed for test/docker package during replication tests
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
  # for now we need to run the tests sequentially, there seems to be some sort of issues with running them in parallel
  for pkg in $(go list ./... | grep 'test/acceptance' | grep -v 'test/acceptance/stress_tests' | grep -v 'test/acceptance/replication' | grep -v 'test/acceptance/graphql_resolvers'); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
  for pkg in $(go list ./... | grep 'test/acceptance/stress_tests' ); do
    if ! go test -count 1 "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
  # tests with go client are in a separate package with its own dependencies to isolate them
  cd 'test/acceptance_with_go_client'
  for pkg in $(go list ./... ); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}
function run_acceptance_graphql_tests() {
  for pkg in $(go list ./... | grep 'test/acceptance/graphql_resolvers'); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_replication_tests() {
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication'); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_module_only_backup_tests() {
  for pkg in $(go list ./... | grep 'test/modules' | grep 'test/modules/backup'); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_module_except_backup_tests() {
  for pkg in $(go list ./... | grep 'test/modules' | grep -v 'test/modules/backup'); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_module_tests() {
  if $run_module_only_backup_tests; then
    run_module_only_backup_tests "$@"
  fi
  if $run_module_except_backup_tests; then
    run_module_except_backup_tests "$@"
  fi
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
