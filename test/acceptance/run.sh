#!/usr/bin/env bash

set -eou pipefail

function main() {
  # Jump to root directory
  cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

  echo "INFO: This script will surpress most output, unless a command ultimately fails"
  echo "      Then it will print the output of the failed command."

  echo_green "Prepare workspace..."
   
  # Remove data directory in case of previous runs
  rm -rf data
  echo "Done!"

  echo_green "Stop any running docker-compose containers..."
  surpress_on_success docker-compose down

  echo_green "Start up weaviate and backing dbs in docker-compose..."
  echo "This could take some time..."
  surpress_on_success tools/test/run_ci_server.sh

  echo_green "Import required schema and test fixtures..."
  # Note: It's not best practice to do this as part of the test script
  # It would be better if each test independently prepared (and also 
  # cleaned up) the test fixtures it needs, but one step at a time ;)
  surpress_on_success import_test_fixtures


  echo_green "Run acceptance tests..."
  run_acceptance_tests
}

function run_acceptance_tests() {
  for pkg in $(go list ./... | grep -v main); do
        if ! go test -race -coverprofile=$(echo $pkg | tr / -).cover $pkg; then
          echo "Test for $pkg failed" >&2
          return 1
        fi
    done
}

function import_test_fixtures() {
  # Load the test schema in weaviate.
  go run ./tools/schema_loader \
    -action-schema test/schema/test-action-schema.json \
    -thing-schema test/schema/test-thing-schema.json

  # Load the fixtures for the GraphQL acceptance tests
  go run ./tools/schema_loader \
    -action-schema test/acceptance/graphql_resolvers_local/fixtures/actions_schema.json \
    -thing-schema test/acceptance/graphql_resolvers_local/fixtures/things_schema.json

  go run ./tools/fixture_importer/ \
    -fixture-file test/acceptance/graphql_resolvers_local/fixtures/data.json
}

surpress_on_success() {
  out="$("${@}" 2>&1)" || { echo_red "FAILED!";  echo "$out"; return 1; }
  echo "Done!"
}

function echo_green() {
  green='\033[0;32m'
  nc='\033[0m' 
  echo -e "${green}${*}${nc}"
}

function echo_red() {
  green='\033[0;31m'
  nc='\033[0m' 
  echo -e "${green}${*}${nc}"
}

main "${@}"
