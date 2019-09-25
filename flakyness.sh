#!/bin/bash

success=0
fail=0

for _ in {1..50}; do
  if ./test/integration/run.sh -run Orchestrate; then
    success=$((success+1))
  else
    fail=$((fail+1))
  fi
done

echo "Success: $success"
echo "Failure: $fail"
