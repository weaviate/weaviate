#!/bin/bash
set -eou pipefail

function main() {
  # This script runs all non-benchmark tests if no CMD switch is given and the respective tests otherwise.
  run_all_tests=true
  run_acceptance_tests=false
  run_acceptance_only_fast_group_1=false
  run_acceptance_only_fast_group_2=false
  run_acceptance_only_fast_group_3=false
  run_acceptance_only_fast_group_4=false
  run_acceptance_only_fast_group_5=false
  run_acceptance_distributed_tasks=false
  run_acceptance_only_authz=false
  run_acceptance_only_mcp=false
  run_acceptance_only_python=false
  run_acceptance_only_python_namespaces=false
  run_acceptance_go_client=false
  run_acceptance_go_client_only_fast_group_1=false
  run_acceptance_go_client_only_fast_group_2=false
  run_acceptance_go_client_only_fast_group_3=false
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
  run_acceptance_go_client_named_vectors_single_node=false
  run_acceptance_go_client_named_vectors_cluster=false
  run_acceptance_lsmkv=false
  run_acceptance_compaction_recovery=false
  run_acceptance_compaction=false
  run_acceptance_recovery=false
  run_acceptance_reindex_multinode=false
  run_acceptance_reindex_multinode_aj=false
  run_acceptance_reindex_multinode_rm=false
  run_acceptance_reindex_multinode_restart_a=false
  run_acceptance_reindex_multinode_restart_b=false
  run_acceptance_reindex_multinode_scale=false
  run_acceptance_reindex_multinode_changetok=false
  run_acceptance_reindex_singlenode_a=false
  run_acceptance_reindex_singlenode_b=false
  run_acceptance_reindex_concurrent=false
  run_acceptance_reindex_mt=false
  run_acceptance_reindex_backup=false

  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --unit-only|-u) run_all_tests=false; run_unit_tests=true;;
          --unit-and-integration-only|-ui) run_all_tests=false; run_unit_and_integration_tests=true;;
          --integration-only|-i) run_all_tests=false; run_integration_tests=true;;
          --integration-vector-package-only|-ivpo) run_all_tests=false; run_integration_tests=true; run_integration_tests_only_vector_package=true;;
          --integration-without-vector-package|-iwvp) run_all_tests=false; run_integration_tests=true; run_integration_tests_without_vector_package=true;;
          --acceptance-only|--e2e-only|-a) run_all_tests=false; run_acceptance_tests=true ;;
          --acceptance-only-fast|-aof) run_all_tests=false; run_acceptance_only_fast_group_1=true; run_acceptance_only_fast_group_2=true; run_acceptance_only_fast_group_3=true; run_acceptance_only_fast_group_4=true; run_acceptance_only_fast_group_5=true;;
          --acceptance-only-fast-group-1|-aof-g1) run_all_tests=false; run_acceptance_only_fast_group_1=true;;
          --acceptance-only-fast-group-2|-aof-g2) run_all_tests=false; run_acceptance_only_fast_group_2=true;;
          --acceptance-only-fast-group-3|-aof-g3) run_all_tests=false; run_acceptance_only_fast_group_3=true;;
          --acceptance-only-fast-group-4|-aof-g4) run_all_tests=false; run_acceptance_only_fast_group_4=true;;
          --acceptance-only-fast-group-5|-aof-g5) run_all_tests=false; run_acceptance_only_fast_group_5=true;;
          --acceptance-distributed-tasks) run_all_tests=false; run_acceptance_distributed_tasks=true;;
          --acceptance-only-python|-aop) run_all_tests=false; run_acceptance_only_python=true;;
          --acceptance-only-python-namespaces|-aopns) run_all_tests=false; run_acceptance_only_python_namespaces=true;;
          --acceptance-go-client|-ag) run_all_tests=false; run_acceptance_go_client=true;;
          --acceptance-go-client-only-fast|-agof) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_only_fast_group_1=true; run_acceptance_go_client_only_fast_group_2=true; run_acceptance_go_client_only_fast_group_3=true;;
          --acceptance-go-client-only-fast-group-1|-agof-g1) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_only_fast_group_1=true;;
          --acceptance-go-client-only-fast-group-2|-agof-g2) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_only_fast_group_2=true;;
          --acceptance-go-client-only-fast-group-3|-agof-g3) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_only_fast_group_3=true;;
          --acceptance-go-client-named-vectors-single-node|-agnvsn) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_named_vectors_single_node=true;;
          --acceptance-go-client-named-vectors-cluster|-agnvc) run_all_tests=false; run_acceptance_go_client=false; run_acceptance_go_client_named_vectors_cluster=true;;
          --acceptance-only-graphql|-aog) run_all_tests=false; run_acceptance_graphql_tests=true ;;
          --acceptance-only-authz|-aoa) run_all_tests=false; run_acceptance_only_authz=true;;
          --acceptance-only-mcp|-aom) run_all_tests=false; run_acceptance_only_mcp=true;;
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
          --acceptance-compaction-recovery|-acr) run_all_tests=false; run_acceptance_compaction_recovery=true;;
          --acceptance-compaction|-ac) run_all_tests=false; run_acceptance_compaction=true;;
          --acceptance-recovery|-ar) run_all_tests=false; run_acceptance_recovery=true;;
          --acceptance-reindex-multinode|-arm) run_all_tests=false; run_acceptance_reindex_multinode=true;;
          --acceptance-reindex-multinode-aj|-armaj) run_all_tests=false; run_acceptance_reindex_multinode_aj=true;;
          --acceptance-reindex-multinode-rm|-armrm) run_all_tests=false; run_acceptance_reindex_multinode_rm=true;;
          --acceptance-reindex-multinode-restart-a|-armra) run_all_tests=false; run_acceptance_reindex_multinode_restart_a=true;;
          --acceptance-reindex-multinode-restart-b|-armrb) run_all_tests=false; run_acceptance_reindex_multinode_restart_b=true;;
          --acceptance-reindex-multinode-scale|-arms) run_all_tests=false; run_acceptance_reindex_multinode_scale=true;;
          --acceptance-reindex-multinode-changetok|-armct) run_all_tests=false; run_acceptance_reindex_multinode_changetok=true;;
          --acceptance-reindex-singlenode-a|-arsa) run_all_tests=false; run_acceptance_reindex_singlenode_a=true;;
          --acceptance-reindex-singlenode-b|-arsb) run_all_tests=false; run_acceptance_reindex_singlenode_b=true;;
          --acceptance-reindex-concurrent|-arc) run_all_tests=false; run_acceptance_reindex_concurrent=true;;
          --acceptance-reindex-mt|-armt) run_all_tests=false; run_acceptance_reindex_mt=true;;
          --acceptance-reindex-backup|-arb) run_all_tests=false; run_acceptance_reindex_backup=true;;
          --benchmark-only|-b) run_all_tests=false; run_benchmark=true;;
          --cleanup) run_all_tests=false; run_cleanup=true;;
          --help|-h) printf '%s\n' \
              "Options:"\
              "--unit-only | -u"\
              "--unit-and-integration-only | -ui"\
              "--integration-only | -i"\
              "--acceptance-only | -a"\
              "--acceptance-only-fast | -aof"\
              "--acceptance-only-fast-group-1 | -aof-g1"\
              "--acceptance-only-fast-group-2 | -aof-g2"\
              "--acceptance-only-fast-group-3 | -aof-g3"\
              "--acceptance-only-fast-group-4 | -aof-g4"\
              "--acceptance-only-fast-group-5 | -aof-g5"\
              "--acceptance-only-python | -aop"\
              "--acceptance-only-python-namespaces | -aopns"\
              "--acceptance-go-client | -ag"\
              "--acceptance-go-client-only-fast | -agof"\
              "--acceptance-go-client-only-fast-group-1 | -agof-g1"\
              "--acceptance-go-client-only-fast-group-2 | -agof-g2"\
              "--acceptance-go-client-named-vectors-single-node | -agnvsn"\
              "--acceptance-go-client-named-vectors-cluster | -agnvc"\
              "--acceptance-only-graphql | -aog"\
              "--acceptance-only-replication| -aor"\
              "--acceptance-only-replica-replication-fast | -aorrf"\
              "--acceptance-only-replica-replication-slow | -aorrs"\
              "--acceptance-only-async-replication | -aoar"\
              "--acceptance-module-tests-only | --modules-only | -m"\
              "--acceptance-module-tests-only-backup | --modules-backup-only | -mob"\
              "--acceptance-module-tests-except-backup | --modules-except-backup | -meb"\
              "--acceptance-lsmkv | --lsmkv"\
              "--acceptance-compaction-recovery | -acr"\
              "--acceptance-compaction | -ac"\
              "--acceptance-recovery | -ar"\
              "--acceptance-reindex-multinode | -arm"\
              "--acceptance-reindex-multinode-aj | -armaj"\
              "--acceptance-reindex-multinode-rm | -armrm"\
              "--acceptance-reindex-multinode-restart-a | -armra"\
              "--acceptance-reindex-multinode-restart-b | -armrb"\
              "--acceptance-reindex-multinode-scale | -arms"\
              "--acceptance-reindex-multinode-changetok | -armct"\
              "--acceptance-reindex-singlenode-a | -arsa"\
              "--acceptance-reindex-singlenode-b | -arsb"\
              "--acceptance-reindex-concurrent | -arc"\
              "--acceptance-reindex-mt | -armt"\
              "--acceptance-reindex-backup | -arb"\
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

  if $run_acceptance_tests  || $run_acceptance_only_fast_group_1 || $run_acceptance_only_fast_group_2 || $run_acceptance_only_fast_group_3 || $run_acceptance_only_fast_group_4 || $run_acceptance_only_fast_group_5 || $run_acceptance_only_authz || $run_acceptance_only_mcp || $run_acceptance_go_client || $run_acceptance_graphql_tests || $run_acceptance_replication_tests || $run_acceptance_replica_replication_fast_tests || $run_acceptance_replica_replication_slow_tests || $run_acceptance_async_replication_tests || $run_acceptance_only_python || $run_all_tests || $run_benchmark || $run_acceptance_go_client_only_fast_group_1 || $run_acceptance_go_client_only_fast_group_2 || $run_acceptance_go_client_only_fast_group_3 || $run_acceptance_go_client_named_vectors_single_node || $run_acceptance_go_client_named_vectors_cluster || $only_acceptance || $run_acceptance_objects
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
    elif $run_acceptance_only_mcp
    then
      tools/test/run_ci_server.sh --with-mcp
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

    if $run_acceptance_tests || $run_acceptance_only_fast_group_1 || $run_acceptance_only_fast_group_2 || $run_acceptance_only_fast_group_3 || $run_acceptance_only_fast_group_4 || $run_acceptance_only_fast_group_5 || $run_acceptance_only_authz || $run_acceptance_only_mcp || $run_acceptance_go_client || $run_acceptance_graphql_tests || $run_acceptance_replication_tests || $run_acceptance_replica_replication_fast_tests || $run_acceptance_replica_replication_slow_tests || $run_acceptance_async_replication_tests || $run_acceptance_go_client_only_fast_group_1 || $run_acceptance_go_client_only_fast_group_2 || $run_acceptance_go_client_only_fast_group_3 || $run_acceptance_go_client_named_vectors_single_node || $run_acceptance_go_client_named_vectors_cluster || $run_all_tests || $only_acceptance || $run_acceptance_objects
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

  if $run_acceptance_only_python_namespaces
  then
    # Dedicated 3-node namespaces-enabled cluster on ports 8190/8191/8192
    # (HTTP) + 50190/50191/50192 (gRPC). Kept separate from the standard
    # docker-compose-test.yml flow because NAMESPACES_ENABLED forces
    # DISABLE_GRAPHQL=true and REPLICATION_MAXIMUM_FACTOR=1, which the rest
    # of the python suite isn't built to assume.
    echo_green "acceptance — python-namespaces: building weaviate/test-server image..."
    GIT_REVISION=$(git rev-parse --short HEAD)
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    docker compose -f docker-compose-namespaces-test.yml down --remove-orphans >/dev/null 2>&1 || true
    docker compose -f docker-compose-namespaces-test.yml build \
      --build-arg GIT_REVISION="$GIT_REVISION" \
      --build-arg GIT_BRANCH="$GIT_BRANCH" \
      --build-arg EXTRA_BUILD_ARGS="-race"
    echo_green "acceptance — python-namespaces: starting 3-node cluster..."
    docker compose -f docker-compose-namespaces-test.yml up -d

    # Poll each node's /v1/.well-known/ready. RAFT bootstrap waits on all
    # three nodes (REPLICATION_MAXIMUM_FACTOR=1 still requires the cluster
    # to be quorate at start), so give each a generous window.
    for port in 8190 8191 8192; do
      echo_green "acceptance — python-namespaces: waiting for node on :$port..."
      ready=false
      for _ in $(seq 1 90); do
        if curl -sf "http://localhost:$port/v1/.well-known/ready" >/dev/null; then
          ready=true
          break
        fi
        sleep 2
      done
      if ! $ready; then
        echo "python-namespaces: node on :$port did not become ready" >&2
        docker compose -f docker-compose-namespaces-test.yml logs --tail=200 || true
        docker compose -f docker-compose-namespaces-test.yml down --remove-orphans || true
        exit 1
      fi
    done

    echo_green "Run python namespace acceptance tests..."
    set +e
    ./test/acceptance_with_python/run.sh namespaces
    ns_exit=$?
    set -e

    docker compose -f docker-compose-namespaces-test.yml down --remove-orphans || true

    if [ $ns_exit -ne 0 ]; then
      echo "python-namespaces tests failed" >&2
      exit $ns_exit
    fi
    echo_green "Python namespace tests successful"
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

  if $run_acceptance_compaction_recovery || $run_acceptance_compaction || $run_acceptance_tests || $run_all_tests; then
    echo "running compaction acceptance tests"
    run_acceptance_compaction
  fi

  if $run_acceptance_compaction_recovery || $run_acceptance_recovery || $run_acceptance_tests || $run_all_tests; then
    echo "running recovery acceptance tests"
    run_acceptance_recovery
  fi

  # Dispatch the dedicated --acceptance-distributed-tasks shard at the top
  # level. Nesting inside run_acceptance_tests() silently dropped the
  # shard because that function is gated by the big-IF a few hundred
  # lines below, which does not include $run_acceptance_distributed_tasks.
  if $run_acceptance_distributed_tasks; then
    echo "running acceptance distributed_tasks"
    build_weaviate_test_image
    run_aof_group "distributed-tasks" test/acceptance/distributed_tasks
  fi

  if $run_acceptance_reindex_multinode; then
    echo "running reindex multinode acceptance tests (catch-all: everything not in -aj/-rm/-restart/-scale/-changetok sub-shards)"
    run_acceptance_reindex_multinode
  fi

  if $run_acceptance_reindex_multinode_aj; then
    echo "running reindex multinode adjacent-journeys acceptance tests"
    run_acceptance_reindex_multinode_aj
  fi

  if $run_acceptance_reindex_multinode_rm; then
    echo "running reindex multinode RestartMatrix acceptance tests"
    run_acceptance_reindex_multinode_rm
  fi

  if $run_acceptance_reindex_multinode_restart_a; then
    echo "running reindex multinode restart-themed acceptance tests (mid-reindex restarts + post-complete)"
    run_acceptance_reindex_multinode_restart_a
  fi

  if $run_acceptance_reindex_multinode_restart_b; then
    echo "running reindex multinode restart-themed acceptance tests (finalizing-window restarts + post-restart migration)"
    run_acceptance_reindex_multinode_restart_b
  fi

  if $run_acceptance_reindex_multinode_scale; then
    echo "running reindex multinode scale/orchestration acceptance tests"
    run_acceptance_reindex_multinode_scale
  fi

  if $run_acceptance_reindex_multinode_changetok; then
    echo "running reindex multinode change-tokenization acceptance tests"
    run_acceptance_reindex_multinode_changetok
  fi

  if $run_acceptance_reindex_singlenode_a; then
    echo "running reindex singlenode acceptance tests — sub-shard A (everything except PropertyStateMigrationMatrix)"
    run_acceptance_reindex_singlenode_a
  fi

  if $run_acceptance_reindex_singlenode_b; then
    echo "running reindex singlenode acceptance tests — sub-shard B (PropertyStateMigrationMatrix + PostRestartFinalize)"
    run_acceptance_reindex_singlenode_b
  fi

  if $run_acceptance_reindex_concurrent; then
    echo "running reindex concurrent acceptance tests"
    run_acceptance_reindex_concurrent
  fi

  if $run_acceptance_reindex_mt; then
    echo "running reindex multi-tenant acceptance tests"
    run_acceptance_reindex_mt
  fi

  if $run_acceptance_reindex_backup; then
    echo "running backup × runtime-reindex acceptance tests"
    run_acceptance_reindex_backup
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
  if $run_acceptance_only_fast_group_1 || \
     $run_acceptance_only_fast_group_2 || \
     $run_acceptance_only_fast_group_3 || \
     $run_acceptance_only_fast_group_4 || \
     $run_acceptance_only_fast_group_5 || \
     $run_acceptance_tests || \
     $run_all_tests; then
    echo "running acceptance fast only"

    if $run_acceptance_only_fast_group_1 || $run_acceptance_tests || $run_all_tests; then
      run_acceptance_only_fast_group 1
    fi
    if $run_acceptance_only_fast_group_2 || $run_acceptance_tests || $run_all_tests; then
      run_acceptance_only_fast_group 2
    fi
    if $run_acceptance_only_fast_group_3 || $run_acceptance_tests || $run_all_tests; then
      run_acceptance_only_fast_group 3
    fi
    if $run_acceptance_only_fast_group_4 || $run_acceptance_tests || $run_all_tests; then
      run_acceptance_only_fast_group 4
    fi
    if $run_acceptance_only_fast_group_5 || $run_acceptance_tests || $run_all_tests; then
      run_acceptance_only_fast_group 5
    fi
  fi
  # Catch-all for --acceptance-only / --all-tests. The dedicated
  # --acceptance-distributed-tasks shard is dispatched at the top level
  # (search for "distributed_tasks" above); $run_acceptance_distributed_tasks
  # is intentionally absent from this predicate.
  if $run_acceptance_tests || $run_all_tests; then
    echo "running acceptance distributed_tasks (via catch-all)"
    build_weaviate_test_image
    run_aof_group "distributed-tasks" test/acceptance/distributed_tasks
  fi
  if $run_acceptance_only_authz || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance authz"
    run_acceptance_only_authz "$@"
  fi
  if $run_acceptance_only_mcp || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance mcp"
    run_acceptance_only_mcp "$@"
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
  if $run_acceptance_go_client_only_fast_group_1 || $run_acceptance_go_client || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance go client only fast group 1"
    run_acceptance_go_client_only_fast_group 1
  fi
  if $run_acceptance_go_client_only_fast_group_2 || $run_acceptance_go_client || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance go client only fast group 2"
    run_acceptance_go_client_only_fast_group 2
  fi
  if $run_acceptance_go_client_only_fast_group_3 || $run_acceptance_go_client || $run_acceptance_tests || $run_all_tests; then
  echo "running acceptance go client only fast group 3"
    run_acceptance_go_client_only_fast_group 3
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

# get_fast_acceptance_packages returns a list of fast acceptance test packages.
# It excludes slow test categories (replication, graphql, authz, mcp, etc.) but includes stress tests.
# The returned paths are normalized to "test/acceptance/..." format.
function get_fast_acceptance_packages() {
  # fast acceptance tests minus slow acceptance tests
  go list ./... \
    | grep 'test/acceptance' \
    | grep -v 'test/acceptance/replication' \
    | grep -v 'test/acceptance/graphql_resolvers' \
    | grep -v 'test/acceptance_lsmkv' \
    | grep -v 'test/acceptance/authz' \
    | grep -v 'test/acceptance/mcp' \
    | grep -v 'test/acceptance/compaction' \
    | grep -v 'test/acceptance/recovery' \
    | grep -v 'test/acceptance/reindex_multinode' \
    | grep -v 'test/acceptance/reindex_singlenode' \
    | grep -v 'test/acceptance/reindex_concurrent' \
    | grep -v 'test/acceptance/reindex_mt' \
    | grep -v 'test/acceptance/reindex_backup' \
    | grep -v 'test/acceptance/distributed_tasks' \
    | sed 's|.*/test/acceptance/|test/acceptance/|'
}

# run_aof_group runs a group of acceptance test packages with appropriate test flags.
# Parameters:
#   $1: group_name - display name for the group (e.g., "1", "2")
#   $@: package_paths - list of package paths to run
#
# Optional env vars (read by this function):
#   AOF_GROUP_RUN  - if non-empty, passed as `-run "$AOF_GROUP_RUN"` to go test
#   AOF_GROUP_SKIP - if non-empty, passed as `-skip "$AOF_GROUP_SKIP"` to go test
# Use these to shard a heavy test package across multiple CI jobs.
#
# Stress tests automatically get different flags (no timeout, no race detector).
# Returns 1 if any test fails, 0 if all succeed.
function run_aof_group() {
  local group_name="$1"
  shift
  local -a package_paths=("$@")

  echo "Group $group_name packages: ${package_paths[*]}"
  if [[ -n "${AOF_GROUP_RUN:-}" ]]; then
    echo "  -run filter: $AOF_GROUP_RUN"
  fi
  if [[ -n "${AOF_GROUP_SKIP:-}" ]]; then
    echo "  -skip filter: $AOF_GROUP_SKIP"
  fi

  local -a extra_flags=()
  if [[ -n "${AOF_GROUP_RUN:-}" ]]; then
    extra_flags+=(-run "$AOF_GROUP_RUN")
  fi
  if [[ -n "${AOF_GROUP_SKIP:-}" ]]; then
    extra_flags+=(-skip "$AOF_GROUP_SKIP")
  fi

  local testFailed=0
  for path in "${package_paths[@]}"; do
    for pkg in $(go list "./$path" 2>/dev/null || true); do
      echo_green "Running $pkg"

      # Stress tests need different test configuration (no timeout, no race detector)
      if [[ "$pkg" == "test/acceptance/stress_tests" ]]; then
        if ! go test -count 1 "${extra_flags[@]}" "$pkg"; then
          echo "Test for $pkg failed" >&2
          testFailed=1
        fi
      else
        if ! go test -count 1 -timeout=20m -race "${extra_flags[@]}" "$pkg"; then
          echo "Test for $pkg failed" >&2
          testFailed=1
        fi
      fi
    done
  done

  [[ $testFailed -eq 1 ]] && return 1
  return 0
}

# get_aof_group returns the package list for the specified group number (1-4).
function get_aof_group() {
  case "$1" in
    1) echo "test/acceptance/multi_node test/acceptance/actions" ;;
    2) echo "test/acceptance/schema test/acceptance/cluster_api_auth test/acceptance/batch_request_endpoints" ;;
    3) echo "test/acceptance/authn test/acceptance/aliases test/acceptance/maintenance_mode test/acceptance/grpc test/acceptance/vector_distances" ;;
    4) echo "test/acceptance/alter_schema test/acceptance/namespace test/acceptance/namespace_limits test/acceptance/vector_index_restrictions" ;;
    *) echo "" ;;
  esac
}

# get_other_packages returns fast acceptance packages not included in groups 1-4.
# These packages form group 5 and include any newly added tests automatically.
# Returns normalized package paths, one per line.
function get_other_packages() {
  local -a AOF_GROUP1=()
  local -a AOF_GROUP2=()
  local -a AOF_GROUP3=()
  local -a AOF_GROUP4=()

  read -ra AOF_GROUP1 <<< "$(get_aof_group 1)"
  read -ra AOF_GROUP2 <<< "$(get_aof_group 2)"
  read -ra AOF_GROUP3 <<< "$(get_aof_group 3)"
  read -ra AOF_GROUP4 <<< "$(get_aof_group 4)"

  # All fast acceptance test packages, excluding those in groups 1-4
  local -a other_fast_packages=()
  while IFS= read -r pkg; do
    [[ -n $pkg ]] && other_fast_packages+=("$pkg")
  done < <(
    get_fast_acceptance_packages | grep -F -x -v -f <(printf '%s\n' "${AOF_GROUP1[@]}" "${AOF_GROUP2[@]}" "${AOF_GROUP3[@]}" "${AOF_GROUP4[@]}")
  )

  printf '%s\n' "${other_fast_packages[@]}"
}

# run_acceptance_only_fast_group runs a specific group of fast acceptance tests.
# Parameters:
#   $1: GROUP - group number to run (1-5)
# Groups 1-4 contain explicitly assigned packages for load balancing.
# Group 5 automatically contains all other fast acceptance packages.
function run_acceptance_only_fast_group() {
  build_weaviate_test_image
  local GROUP="$1"

  local -a AOF_GROUP1=()
  local -a AOF_GROUP2=()
  local -a AOF_GROUP3=()
  local -a AOF_GROUP4=()

  read -ra AOF_GROUP1 <<< "$(get_aof_group 1)"
  read -ra AOF_GROUP2 <<< "$(get_aof_group 2)"
  read -ra AOF_GROUP3 <<< "$(get_aof_group 3)"
  read -ra AOF_GROUP4 <<< "$(get_aof_group 4)"

  case "$GROUP" in
    1)
      echo_green "acceptance-only-fast — group 1/5"
      run_aof_group "1" "${AOF_GROUP1[@]}"
      ;;
    2)
      echo_green "acceptance-only-fast — group 2/5"
      run_aof_group "2" "${AOF_GROUP2[@]}"
      ;;
    3)
      echo_green "acceptance-only-fast — group 3/5"
      run_aof_group "3" "${AOF_GROUP3[@]}"
      ;;
    4)
      echo_green "acceptance-only-fast — group 4/5"
      run_aof_group "4" "${AOF_GROUP4[@]}"
      ;;
    5)
      echo_green "acceptance-only-fast — group 5/5 (others from fast set)"

      local -a other_fast_packages=()
      while IFS= read -r pkg; do
        [[ -n $pkg ]] && other_fast_packages+=("$pkg")
      done < <(get_other_packages)

      [[ ${#other_fast_packages[@]} -eq 0 ]] && { echo "Nothing to run for group 5."; return 0; }

      run_aof_group "5" "${other_fast_packages[@]}"
      ;;
    *) echo_red "Invalid group: $GROUP (must be 1..5)"; return 1 ;;
  esac
}

# build_weaviate_test_image builds the weaviate/test-server Docker image with
# race detector enabled. Sets TEST_WEAVIATE_IMAGE so testcontainers reuse it
# instead of building from source on every test function.
function build_weaviate_test_image() {
  if [[ -n "${TEST_WEAVIATE_IMAGE:-}" ]]; then
    return  # already built or provided externally
  fi
  echo_green "Building weaviate/test-server image..."
  GIT_REVISION=$(git rev-parse --short HEAD)
  GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
  docker compose -f docker-compose-test.yml build \
    --build-arg GIT_REVISION="$GIT_REVISION" \
    --build-arg GIT_BRANCH="$GIT_BRANCH" \
    --build-arg EXTRA_BUILD_ARGS="-race" \
    weaviate
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
}

function run_acceptance_compaction_recovery() {
  build_weaviate_test_image
  run_aof_group "compaction-recovery" \
    test/acceptance/compaction \
    test/acceptance/recovery
}

function run_acceptance_compaction() {
  build_weaviate_test_image
  run_aof_group "compaction" test/acceptance/compaction
}

function run_acceptance_recovery() {
  build_weaviate_test_image
  run_aof_group "recovery" test/acceptance/recovery
}

function run_acceptance_reindex_multinode() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-multinode (catch-all: everything not in -aj/-rm/-restart/-scale/-changetok sub-shards)"
  # The reindex_multinode package is partitioned across 6 CI shards to
  # keep each shard's wall-clock under ~10 min for fast PR feedback.
  # This "catch-all" shard runs only tests NOT claimed by a
  # more-specific sub-shard, so newly added tests get automatic CI
  # coverage here until the test author or a periodic cleanup sorts
  # them into the right sub-shard.
  #
  # Current sub-shards (see test/run.sh: run_acceptance_reindex_multinode_aj,
  # _rm, _restart, _scale, _changetok):
  #   -aj         : TestMultiNode_ChangeTokenization_AJ_*  (adjacent journeys)
  #   -rm         : TestMultiNode_RestartMatrix             (parametrised restart)
  #   -restart    : TestMultiNode_*Restart* / *Crash* (excl. AJ + Matrix)
  #   -scale      : TestMultiNode_HappyPath, _ConcurrentDifferent*,
  #                 _EnableRangeable_NoPartialCountsInFlight,
  #                 _RepeatedParallelMigrationJourney_*,
  #                 _PostRestartReapplyMigrations_*
  #   -changetok  : TestMultiNode_ChangeTokenization_* (non-AJ) +
  #                 TestMultiNode_BackToBackChangeTokenization_* +
  #                 TestLiveQueriesDuringChangeTokenization +
  #                 TestPartialResultsDuringChangeTokenization
  #
  # IMPORTANT: when adding a new sub-shard, also add its top-level test
  # prefixes to the SKIP regex below to prevent the catch-all from
  # double-running the same tests.
  AOF_GROUP_SKIP='TestMultiNode_ChangeTokenization_AJ_|TestMultiNode_RestartMatrix|TestMultiNode_(Rolling|Graceful|Crash|MajorityCrash|UngracefulStop|PostRestartMigration)|TestMultiNode_(HappyPath|QueryConsistencyDuringReindex|ConcurrentDifferentMigrations|EnableRangeable_NoPartialCountsInFlight|RepeatedParallelMigrationJourney|PostRestartReapplyMigrations)|TestMultiNode_ChangeTokenization_|TestMultiNode_BackToBackChangeTokenization|TestLiveQueriesDuringChangeTokenization|TestPartialResultsDuringChangeTokenization' \
    run_aof_group "reindex-multinode" test/acceptance/reindex_multinode
}

function run_acceptance_reindex_multinode_restart_a() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-multinode-restart-a (mid-reindex restarts + post-complete rolling)"
  # 7 tests:
  #   TestMultiNode_GracefulRestartDuringReindex
  #   TestMultiNode_GracefulLeaderRestartDuringReindex
  #   TestMultiNode_CrashDuringReindex
  #   TestMultiNode_MajorityCrashDuringReindex
  #   TestMultiNode_RollingRestartAfterComplete
  #   TestMultiNode_RollingRestartMidMigration
  #   TestMultiNode_RollingRestartBetweenMigrations
  #
  # The "restart-during-active-reindex" + "post-complete rolling"
  # bucket. Each test owns its own cluster lifecycle (restart timing
  # IS the journey), so these cannot be folded onto a shared cluster
  # the way the AJ suite can. The split below balances the wall-clock
  # against -restart-b instead.
  #
  # Regex caveat: alternation is by exact name. Earlier the filter
  # had `GracefulRestartDuringReindex` which did NOT match
  # `GracefulLeaderRestartDuringReindex` — a false-green CI ran for
  # multiple commits before this gap was caught. Same family caught
  # `RollingRestartMidMigration` + `RollingRestartBetweenMigrations`
  # un-claimed by any sub-shard; both are PASSING on main and now
  # land in this filter.
  AOF_GROUP_RUN='TestMultiNode_(GracefulRestartDuringReindex|GracefulLeaderRestartDuringReindex|CrashDuringReindex|MajorityCrashDuringReindex|RollingRestartAfterComplete|RollingRestartMidMigration|RollingRestartBetweenMigrations)' \
    run_aof_group "reindex-multinode-restart-a" test/acceptance/reindex_multinode
}

function run_acceptance_reindex_multinode_restart_b() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-multinode-restart-b (finalizing-window restarts + post-restart migration)"
  # 3 tests:
  #   TestMultiNode_RollingRestartDuringFinalizing_PerReplicaConsistency
  #   TestMultiNode_UngracefulStopDuringFinalizing_PerReplicaConsistency
  #   TestMultiNode_PostRestartMigration_NoStallPlateau
  #
  # The "narrow timing window" bucket — every test here is about a
  # specific moment in the migration state machine (FINALIZING window
  # races) or about the post-restart migration replay path. Same
  # rationale as -restart-a: per-test cluster lifecycle is structural,
  # not optional.
  AOF_GROUP_RUN='TestMultiNode_(RollingRestartDuringFinalizing|UngracefulStopDuringFinalizing|PostRestartMigration)' \
    run_aof_group "reindex-multinode-restart-b" test/acceptance/reindex_multinode
}

function run_acceptance_reindex_multinode_scale() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-multinode-scale"
  # Scale / orchestration tests. Locally measured wall-clock ≈ 283s
  # across 5 tests; +CI overhead → ~8-9 min total. The single longest
  # test in the whole package (PostRestartReapplyMigrations_ExactCounts
  # AcrossReplicas, 135s) lives here so it's amortised against the
  # smaller orchestration tests.
  AOF_GROUP_RUN='TestMultiNode_(HappyPath|QueryConsistencyDuringReindex|ConcurrentDifferentMigrations|EnableRangeable_NoPartialCountsInFlight|RepeatedParallelMigrationJourney|PostRestartReapplyMigrations)' \
    run_aof_group "reindex-multinode-scale" test/acceptance/reindex_multinode
}

function run_acceptance_reindex_multinode_changetok() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-multinode-changetok"
  # Change-tokenization tests (non-AJ; AJ has its own -aj shard) plus
  # the FINALIZING-window probe tests. Locally measured wall-clock
  # ≈ 171s across 7 tests; +CI overhead → ~7-8 min total.
  AOF_GROUP_RUN='TestMultiNode_ChangeTokenization_(RestartThenRoundTrip|MTRoundTrip|ConcurrentDifferentProps|RoundTrip)|TestMultiNode_BackToBackChangeTokenization|TestLiveQueriesDuringChangeTokenization|TestPartialResultsDuringChangeTokenization' \
    run_aof_group "reindex-multinode-changetok" test/acceptance/reindex_multinode
}

function run_acceptance_reindex_multinode_aj() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-multinode-aj"
  # AdjacentJourneys was split into multiple TestMultiNode_ChangeTokenization_AJ_*
  # top-level tests (see test/acceptance/reindex_multinode/round_trip_adjacent_test.go).
  # Each one spins up its own 3-node clusters per subtest, so dedicating
  # an independent CI shard gives the suite its own 20-min budget.
  AOF_GROUP_RUN='TestMultiNode_ChangeTokenization_AJ_' \
    run_aof_group "reindex-multinode-aj" test/acceptance/reindex_multinode
}

function run_acceptance_reindex_multinode_rm() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-multinode-rm"
  # TestMultiNode_RestartMatrix runs 7 sub-scenarios (R0..R5) each
  # involving a full 3-node cluster restart cycle. The total at ~5.5 min
  # is the longest single test in this package; combined with the
  # ~15-16 min of the rest of the package it pushes total runtime over
  # the 20-min go-test deadline. Running in its own shard parallelizes
  # the wall-clock against the other reindex-multinode shards. See
  # 0-weaviate-issues#214 post-fix run on PR #10694.
  AOF_GROUP_RUN='TestMultiNode_RestartMatrix' \
    run_aof_group "reindex-multinode-rm" test/acceptance/reindex_multinode
}

function run_acceptance_reindex_singlenode_a() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-singlenode-a (everything except PropertyStateMigrationMatrix)"
  # Profiled locally on M4 (race detector on): TestSingleNode_ReindexSuite
  # totals 265s, dominated by /PropertyStateMigrationMatrix at 128s
  # (~half the suite). Isolating PSMM to -singlenode-b gives this
  # sub-shard ~137s of suite work + the 4 standalone Test* funcs
  # (TestCancelThenRetry, TestRestartDuringSwap,
  # TestSingleNode_FinishedStatusRaceWithSchemaFlag,
  # TestTornResume_StandaloneSmoke), totalling ~3 min local → ~7-8 min CI.
  #
  # The -skip filter operates at the level of the subtest path: only
  # TestSingleNode_ReindexSuite/PropertyStateMigrationMatrix is skipped;
  # PostRestartFinalize still runs at the end of the suite here and
  # asserts against shardInfos populated by every non-PSMM subtest.
  AOF_GROUP_SKIP='^TestSingleNode_ReindexSuite$/^PropertyStateMigrationMatrix$' \
    run_aof_group "reindex-singlenode-a" test/acceptance/reindex_singlenode
}

function run_acceptance_reindex_singlenode_b() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-singlenode-b (PropertyStateMigrationMatrix only)"
  # The PSMM subtest alone takes 128s locally (~5 min CI). PostRestartFinalize
  # is deliberately NOT included here: that subtest hard-codes 6 post-restart
  # asserters (testBlockmaxPostRestart, testChangeTokenizationPostRestart, …)
  # which assume the corresponding migration subtest already ran on the same
  # container and populated its collection. In shard-b only PSMM has run, so
  # those collections don't exist on this fresh cluster — the first
  # post-restart asserter (change_tokenization_test.go:178 / RetokenizeTest)
  # gets a graphql 422 and the whole subtest fails. PostRestartFinalize is
  # therefore covered exclusively by shard-a, where all 19 other subtests
  # run first and populate the expected collections. PSMM is itself
  # finalize-aware (the FINALIZING-window matrix it asserts is in-flight,
  # not post-restart) so no coverage is lost for PSMM specifically.
  AOF_GROUP_RUN='^TestSingleNode_ReindexSuite$/^PropertyStateMigrationMatrix$' \
    run_aof_group "reindex-singlenode-b" test/acceptance/reindex_singlenode
}

function run_acceptance_reindex_concurrent() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-concurrent"
  # Concurrency tests (TestConcurrentReindex, TestParallelConflictMatrix,
  # TestParallelEnableFilterableAndRangeable). TestParallelConflictMatrix
  # historically runs the longest of the three. Split out of the
  # singlenode bundle for fast feedback.
  run_aof_group "reindex-concurrent" \
    test/acceptance/reindex_concurrent
}

function run_acceptance_reindex_mt() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-mt"
  # Multi-tenant reindex suite (single top-level test
  # TestMultiTenant_ReindexSuite with many subtests).  Split out of the
  # singlenode bundle so reindex_singlenode's wall-clock is no longer
  # gated on this suite's duration.
  run_aof_group "reindex-mt" \
    test/acceptance/reindex_mt
}

function run_acceptance_reindex_backup() {
  build_weaviate_test_image
  echo_green "acceptance — reindex-backup"
  run_aof_group "reindex-backup" \
    test/acceptance/reindex_backup
}

# get_fast_go_client_packages returns a list of fast go client test packages.
# It excludes named_vectors_tests but includes all other go client acceptance tests.
# The returned paths are normalized package paths.
function get_fast_go_client_packages() {
  cd 'test/acceptance_with_go_client'
  go list ./... | grep -v 'acceptance_tests_with_client/named_vectors_tests' | sed 's|.*/acceptance_tests_with_client/|acceptance_tests_with_client/|'
  cd -
}

# get_go_client_group returns the package patterns for the specified group number.
# Groups 1 and 2 contain explicitly assigned packages for load balancing.
# Group 3 will be handled as catch-all in the main function.
function get_go_client_group() {
  case "$1" in
    1) echo "acceptance_tests_with_client/multi_tenancy_tests acceptance_tests_with_client/filters_tests" ;;
    2) echo "acceptance_tests_with_client/usage" ;;
    *) echo "" ;;
  esac
}

# get_other_go_client_packages returns fast go client packages not included in
# groups 1 and 2. These packages form group 3 and include any newly added tests
# automatically. Returns normalized package paths, one per line.
function get_other_go_client_packages() {
  local -a ASSIGNED=()
  read -ra ASSIGNED <<< "$(get_go_client_group 1) $(get_go_client_group 2)"

  # All fast go client test packages, excluding those in groups 1 and 2
  local -a other_fast_packages=()
  while IFS= read -r pkg; do
    [[ -n $pkg ]] && other_fast_packages+=("$pkg")
  done < <(
    get_fast_go_client_packages | grep -F -x -v -f <(printf '%s\n' "${ASSIGNED[@]}")
  )

  printf '%s\n' "${other_fast_packages[@]}"
}

# run_go_client_group runs a group of go client test packages with appropriate test flags.
# Parameters:
#   $1: group_name - display name for the group (e.g., "1", "2")
#   $@: package_paths - list of package paths to run
# Returns 1 if any test fails, 0 if all succeed.
function run_go_client_group() {
  local group_name="$1"
  shift
  local -a package_paths=("$@")

  echo "Go Client Group $group_name packages: ${package_paths[*]}"

  # tests with go client are in a separate package with its own dependencies to isolate them
  cd 'test/acceptance_with_go_client'

  local testFailed=0
  for pattern in "${package_paths[@]}"; do
    for pkg in $(go list ./... | grep -v 'acceptance_tests_with_client/named_vectors_tests' | grep "${pattern}$"); do
      echo_green "Running $pkg"
      if ! go test -count 1 -race "$pkg"; then
        echo "Test for $pkg failed" >&2
        testFailed=1
      fi
    done
  done

  cd -

  [[ $testFailed -eq 1 ]] && return 1
  return 0
}

# run_acceptance_go_client_only_fast_group runs a specific group of go client tests.
# Parameters:
#   $1: GROUP - group number to run (1-3)
# Groups 1 and 2 contain explicitly assigned packages for load balancing.
# Group 3 automatically contains all other fast go client packages.
function run_acceptance_go_client_only_fast_group() {
  build_weaviate_test_image
  local GROUP="$1"

  case "$GROUP" in
    1)
      echo_green "acceptance-go-client-only-fast — group 1/3"
      local -a GROUP1=()
      read -ra GROUP1 <<< "$(get_go_client_group 1)"
      run_go_client_group "1" "${GROUP1[@]}"
      ;;
    2)
      echo_green "acceptance-go-client-only-fast — group 2/3"
      local -a GROUP2=()
      read -ra GROUP2 <<< "$(get_go_client_group 2)"
      run_go_client_group "2" "${GROUP2[@]}"
      ;;
    3)
      echo_green "acceptance-go-client-only-fast — group 3/3 (others from fast set)"

      local -a other_fast_packages=()
      while IFS= read -r pkg; do
        [[ -n $pkg ]] && other_fast_packages+=("$pkg")
      done < <(get_other_go_client_packages)

      [[ ${#other_fast_packages[@]} -eq 0 ]] && { echo "Nothing to run for group 3."; return 0; }

      run_go_client_group "3" "${other_fast_packages[@]}"
      ;;
    *) echo_red "Invalid group: $GROUP (must be 1, 2 or 3)"; return 1 ;;
  esac
}

function run_acceptance_go_client_named_vectors_single_node() {
  build_weaviate_test_image
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
  build_weaviate_test_image
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
  build_weaviate_test_image
  for pkg in $(go list ./... | grep 'test/acceptance/graphql_resolvers'); do
    if ! go test -timeout=15m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_only_authz() {
  build_weaviate_test_image
  for pkg in $(go list ./.../ | grep 'test/acceptance/authz'); do
    if ! go test -timeout=15m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_only_mcp() {
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
  for pkg in $(go list ./.../ | grep 'test/acceptance/mcp'); do
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
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication/replica_replication/slow'); do
    if ! go test -timeout=45m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_replication_tests() {
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication/read_repair'); do
    if ! go test -timeout=20m -count 1 -race "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_acceptance_async_replication_tests() {
  # Build once up front and reuse via TEST_WEAVIATE_IMAGE; otherwise each package
  # below rebuilds the image through testcontainers and the second package can
  # exceed the container-start deadline in CI.
  build_weaviate_test_image
  for pkg in $(go list ./.../ | grep 'test/acceptance/replication/async_replication'); do
    if ! go test -timeout=20m -count 1 -race "$pkg"; then
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
    if ! go test -count 1 -race -timeout 30m "$pkg"; then
      echo "Test for $pkg failed" >&2
      return 1
    fi
  done
}

function run_module_only_offload_tests() {
  for pkg in $(go list ./... |grep 'test/modules/offload'); do
    if ! go test -count 1 -race -timeout 30m -v "$pkg"; then
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
