#!/usr/bin/env bash

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

success=0
fail=0

for _ in {1..50}; do
  if go test -count 1 ./test/acceptance/graphql_resolvers/...; then
    success=$((success+1))
  else
    fail=$((fail+1))
  fi
done

echo "Success: $success"
echo "Failure: $fail"
