#!/bin/bash

set -eou pipefail

function main() {
  # needed for test/docker package during replication tests
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
  # for now we need to run the tests sequentially, there seems to be some sort of issues with running them in parallel
    for pkg in $(go list ./... | grep 'test/acceptance' | grep -v 'test/acceptance/stress_tests' ); do
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

main "$@"