#!/bin/bash

set -eou pipefail

function main() {
  # This script runs all non-benchmark tests if no CMD switch is given and the respective tests otherwise.
  run_all_tests=true
  run_acceptance_tests=false
  run_acceptance_only_fast=false
  run_acceptance_only_authz=false
  run_acceptance_only_python=false
  run_acceptance_go_client=false
  run_acceptance_graphql_tests=false
  run_acceptance_replication_tests=false
  run_acceptance_replica_replication_fast_tests=false
  run_acceptance_replica_replication_slow_tests=false
  run_acceptance_async_replication_tests=false
  run_acceptance_objects=false
  only_acceptance=false
  run_module_tests=false
  only_module=false
  only_module_value=false
  run_unit_and_integration_tests=false
  run_unit_tests=false
  run_integration_tests=false
  run_integration_tests_only_vector_package=false
  run_integration_tests_without_vector_package=false
  run_benchmark=false
  run_module_only_backup_tests=false
  run_module_only_offload_tests=false
  run_module_except_backup_tests=false
  run_module_except_offload_tests=false
  run_cleanup=false
  run_acceptance_go_client_only_fast=false
  run_acceptance_go_client_named_vectors_single_node=false
  run_acceptance_go_client_named_vectors_cluster=false
  run_acceptance_lsmkv=false

  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --unit-only|-u) run_all_tests=false; run_unit_tests=true;;
          --unit-and-integration-only|-ui) run_all_tests=false; run_unit_and_integration_tests=true;;
          --integration-only|-i) run_all_tests=false; run_integration_tests=true;;
          --integration-vector-package-only|-ivpo) run_all_tests=false; run_integration_tests=true; run_integration_tests_only_vector_package=true;;
          --integration-without-vector-package|-iwvp) run_all_tests=false; run_integration_tests=true; run_integration_tests_without_vector_package=true;;
          --acceptance-only|--e2e-only|-a) run_all_tests=false; run_acceptance_tests=true ;;
          --acceptance-only-fast|-aof) run_all_tests=false; run_acceptance_only_fast=true;;
          --acceptance-only-python|-aop) run_all_tests=false; run_acceptance_only_python=true;;
          --acceptance-go-client|-ag) run_all_tests=false; run_acceptance_go_client=true;;
          --acceptance-go-client-only-fast|-agof) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_only_fast=true;;
          --acceptance-go-client-named-vectors-single-node|-agnv) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_named_vectors_single_node=true;;
          --acceptance-go-client-named-vectors-cluster|-agnv) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_named_vectors_cluster=true;;
          --acceptance-only-graphql|-aog) run_all_tests=false; run_acceptance_graphql_tests=true ;;
          --acceptance-only-authz|-aoa) run_all_tests=false; run_acceptance_only_authz=true;;
          --acceptance-only-replication|-aor) run_all_tests=false; run_acceptance_replication_tests=true ;;
          --acceptance-only-replica-replication-fast|-aorrf) run_all_tests=false; run_acceptance_replica_replication_fast_tests=true ;;
          --acceptance-only-replica-replication-slow|-aorrs) run_all_tests=false; run_acceptance_replica_replication_slow_tests=true ;;
          --acceptance-only-async-replication|-aoar) run_all_tests=false; run_acceptance_async_replication_tests=true ;;
          --acceptance-only-objects|-aoob) run_all_tests=false; run_acceptance_objects=true ;;
          --only-acceptance-*|-oa)run_all_tests=false; only_acceptance=true;only_acceptance_value=$1;;
          --only-module-*|-om)run_all_tests=false; only_module=true;only_module_value=$1;;
          --acceptance-module-tests-only|--modules-only|-m) run_all_tests=false; run_module_tests=true; run_module_only_backup_tests=true; run_module_except_backup_tests=true;run_module_only_offload_tests=true;run_module_except_offload_tests=true;;
          --acceptance-module-tests-only-backup|--modules-backup-only|-mob) run_all_tests=false; run_module_tests=true; run_module_only_backup_tests=true;;
          --acceptance-module-tests-only-offload|--modules-offload-only|-moo) run_all_tests=false; run_module_tests=true; run_module_only_offload_tests=true;;
          --acceptance-module-tests-except-backup|--modules-except-backup|-meb) run_all_tests=false; run_module_tests=true; run_module_except_backup_tests=true; echo $run_module_except_backup_tests ;;
          --acceptance-module-tests-except-offload|--modules-except-offload|-meo) run_all_tests=false; run_module_tests=true; run_module_except_offload_tests=true; echo $run_module_except_offload_tests ;;
          --acceptance-lsmkv|--lsmkv) run_all_tests=false; run_acceptance_lsmkv=true;;
          --benchmark-only|-b) run_all_tests=false; run_benchmark=true;;
          --cleanup) run_all_tests=false; run_cleanup=true;;
          --help|-h) printf '%s\n' \
              "Options:"\
              "--unit-only | -u"\
              "--unit-and-integration-only | -ui"\
              "--integration-only | -i"\
              "--acceptance-only | -a"\
              "--acceptance-only-fast | -aof"\
              "--acceptance-only-python | -aop"\
              "--acceptance-go-client | -ag"\
              "--acceptance-go-client-only-fast | -agof"\
              "--acceptance-go-client-named-vectors | -agnv"\
              "--acceptance-only-graphql | -aog"\
              "--acceptance-only-replication| -aor"\
              "--acceptance-only-async-replication-fast| -aoarf"\
              "--acceptance-only-async-replication-slow| -aoars"\
              "--acceptance-module-tests-only | --modules-only | -m"\
              "--acceptance-module-tests-only-backup | --modules-backup-only | -mob"\
              "--acceptance-module-tests-except-backup | --modules-except-backup | -meb"\
              "--acceptance-lsmkv | --lsmkv"\
              "--only-acceptance-{packageName}"
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

  if $run_acceptance_tests  || $run_acceptance_only_fast || $run_acceptance_only_authz || $run_acceptance_go_client || $run_acceptance_graphql_tests || $run_acceptance_replication_tests || $run_acceptance_replica_replication_fast_tests || $run_acceptance_replica_replication_slow_tests || $run_acceptance_async_replication_tests || $run_acceptance_only_python || $run_all_tests || $run_benchmark || $run_acceptance_go_client_only_fast || $run_acceptance_go_client_named_vectors_single_node || $run_acceptance_go_client_named_vectors_cluster || $only_acceptance || $run_acceptance_objects
  then
    echo "Start docker container needed for acceptance and/or benchmark test"
    echo_green "Stop any running docker-compose containers..."
    suppress_on_success docker compose -f docker-compose-test.yml down --remove-orphans

    echo_green "Start up weaviate and backing dbs in docker-compose..."
    echo "This could take some time..."
    if $run_acceptance_only_authz || $run_acceptance_only_python
    then
      tools/test/run_ci_server.sh --with-auth
      build_mockoidc_docker_image_for_tests
    else
      tools/test/run_ci_server.sh
    fi

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

    if $run_acceptance_tests || $run_acceptance_only_fast || $run_acceptance_only_authz || $run_acceptance_go_client || $run_acceptance_graphql_tests || $run_acceptance_replication_tests || $run_acceptance_replica_replication_fast_tests || $run_acceptance_replica_replication_slow_tests || $run_acceptance_async_replication_tests || $run_acceptance_go_client_only_fast || $run_acceptance_go_client_named_vectors_single_node || $run_acceptance_go_client_named_vectors_cluster || $run_all_tests || $only_acceptance || $run_acceptance_objects
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
      if ! go test -count 1 -race -timeout 15m -v "$pkg"; then
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
  if $run_cleanup; then
    echo_green "Cleaning up all running docker containers..."
    docker rm -f $(docker ps -a -q)
  fi

  if $run_acceptance_lsmkv || $run_acceptance_tests || $run_all_tests; then
  echo "running lsmkv acceptance lsmkv tests"
    run_acceptance_lsmkv "$@"
  fi
  echo "Done!"
}

function build_docker_image_for_tests() {
  local module_test_image=weaviate:module-tests
  echo_green "Stop any running docker-compose containers..."
  suppress_on_success docker compose -f docker-compose-test.yml down --remove-orphans
  echo_green "Building weaviate image for module acceptance tests..."
  echo "This could take some time..."
  GIT_REVISION=$(git rev-parse --short HEAD)
  GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

  docker build --build-arg GIT_REVISION="$GIT_REVISION" --build-arg GIT_BRANCH="$GIT_BRANCH" --target weaviate -t $module_test_image .
  export "TEST_WEAVIATE_IMAGE"=$module_test_image
}

function build_mockoidc_docker_image_for_tests() {
  local mockoidc_test_image=mockoidc:module-tests
  echo_green "Building MockOIDC image for module acceptance tests..."
  docker build  -t $mockoidc_test_image test/docker/mockoidc
  export "TEST_MOCKOIDC_IMAGE"=$mockoidc_test_image
  local mockoidc_helper_test_image=mockoidchelper:module-tests
  echo_green "MockOIDC image successfully built"
  echo_green "Building MockOIDC Helper image for module acceptance tests..."
  docker build  -t $mockoidc_helper_test_image test/docker/mockoidchelper
  export "TEST_MOCKOIDC_HELPER_IMAGE"=$mockoidc_helper_test_image
  echo_green "MockOIDC Helper image successfully built"
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

  if $run_integration_tests_only_vector_package; then
    ./test/integration/run.sh --include-slow --only-vector-pkg
  elif $run_integration_tests_without_vector_package; then
    ./test/integration/run.sh --include-slow --without-vector-pkg
  else
    ./test/integration/run.sh --include-slow
  fi
}

function run_acceptance_lsmkv() {
    echo "This test runs without the race detector because it asserts performance"
    cd 'test/acceptance_lsmkv'
    for pkg in $(go list ./...); do
      if ! go test -timeout=15m -count 1 "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
    done
    cd -
}

function run_acceptance_tests() {
  if $run_acceptance_only_fast || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance fast only"
    run_acceptance_only_fast "$@"
  fi
  if $run_acceptance_only_authz || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance authz"
    run_acceptance_only_authz "$@"
  fi
  if $run_acceptance_graphql_tests || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance graphql"
    run_acceptance_graphql_tests "$@"
  fi
  if $run_acceptance_replication_tests || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance replication"
    run_acceptance_replication_tests "$@"
  fi
  if $run_acceptance_replica_replication_fast_tests || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance replica replication replication fast"
    run_acceptance_replica_replication_fast_tests "$@"
  fi
  if $run_acceptance_replica_replication_slow_tests || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance replica replication replication slow"
    run_acceptance_replica_replication_slow_tests "$@"
  fi
  if $run_acceptance_async_replication_tests || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance async replication"
    run_acceptance_async_replication_tests "$@"
  fi
  if $only_acceptance; then
  echo "running only acceptance"
    run_acceptance_only_tests
  fi
  if $run_acceptance_go_client_only_fast || $run_acceptance_go_client || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance go client only fast"
    run_acceptance_go_client_only_fast "$@"
  fi
  if $run_acceptance_go_client_named_vectors_single_node || $run_acceptance_go_client || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance go client named vectors for single node"
    run_acceptance_go_client_named_vectors_single_node "$@"
  fi
  if $run_acceptance_go_client_named_vectors_cluster || $run_acceptance_go_client || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance go client named vectors for cluster"
    run_acceptance_go_client_named_vectors_cluster "$@"
  fi
  if $run_acceptance_objects || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance objects"
    run_acceptance_objects "$@"
  fi
}

function run_acceptance_only_fast() {
  # needed for test/docker package during replication tests
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
  # to make sure all tests are run and the script fails if one of them fails
  # but after all tests ran
  testFailed=0
  # for now we need to run the tests sequentially, there seems to be some sort of issues with running them in parallel
    for pkg in $(go list ./... | grep 'test/acceptance' | grep -v 'test/acceptance/stress_tests' | grep -v 'test/acceptance/replication' | grep -v 'test/acceptance/graphql_resolvers' | grep -v 'test/acceptance_lsmkv' | grep -v 'test/acceptance/authz'); do
      if ! go test -count 1 -timeout=20m -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        testFailed=1
      fi
    done
    if [ "$testFailed" -eq 1 ]; then
      return 1
    fi
    for pkg in $(go list ./... | grep 'test/acceptance/stress_tests' ); do
      if ! go test -count 1 "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
    done
}

function run_acceptance_go_client_only_fast() {
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
    # tests with go client are in a separate package with its own dependencies to isolate them
    cd 'test/acceptance_with_go_client'
    for pkg in $(go list ./... | grep -v 'acceptance_tests_with_client/named_vectors_tests'); do
      if ! go test -count 1 -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
    done
    cd -
}

function run_acceptance_go_client_named_vectors_single_node() {
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
    # tests with go client are in a separate package with its own dependencies to isolate them
    cd 'test/acceptance_with_go_client'
    for pkg in $(go list ./... | grep 'acceptance_tests_with_client/named_vectors_tests/singlenode'); do
      if ! go test -timeout=15m -count 1 -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
    done
    cd -
}

function run_acceptance_go_client_named_vectors_cluster() {
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
    # tests with go client are in a separate package with its own dependencies to isolate them
    cd 'test/acceptance_with_go_client'
    for pkg in $(go list ./... | grep 'acceptance_tests_with_client/named_vectors_tests/cluster'); do
      if ! go test -timeout=15m -count 1 -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        return 1
      fi
    done
    cd -
}

function run_acceptance_graphql_tests() {
  for pkg in $(go list ./... | grep 'test/acceptance/graphql_resolvers'); do
    if ! go test -timeout=15m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_only_authz() {
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
  for pkg in $(go list ./.../ | grep 'test/acceptance/authz'); do
    if ! go test -timeout=15m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_replica_replication_fast_tests() {
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication/replica_replication/fast'); do
    if ! go test -timeout=30m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_replica_replication_slow_tests() {
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication/replica_replication/slow_file_copy'); do
    if ! go test -timeout=30m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_replication_tests() {
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication/read_repair'); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_async_replication_tests() {
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication/async_replication'); do
    if ! go test -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_objects() {
  for pkg in $(go list ./.../ | grep 'test/acceptance/objects'); do
    if ! go test -count 1 -race -v "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_only_tests() {
  package=${only_acceptance_value//--only-acceptance-/}
  echo_green "Running acceptance tests for $package..."
  for pkg in $(go list ./.../ | grep 'test/acceptance/'${package}); do
    if ! go test -v -count 1 -race "$pkg"; then
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

function run_module_only_offload_tests() {
  for pkg in $(go list ./... |grep 'test/modules/offload'); do
    if ! go test -count 1 -race -v "$pkg"; then
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

function run_module_except_offload_tests() {
  for pkg in $(go list ./... | grep 'test/modules' | grep -v 'test/modules/offload'); do
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
  if $run_module_only_offload_tests; then
    run_module_only_offload_tests "$@"
  fi
  if $run_module_except_backup_tests; then
    run_module_except_backup_tests "$@"
  fi
  if $run_module_except_offload_tests; then
    run_module_except_offload_tests "$@"
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
