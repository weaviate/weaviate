#!/usr/bin/env bash

set -euo pipefail

# Search for all tracked non-test .go files in the Git repository
all_files=$(git ls-files | grep -E '\.go$' | grep -vE 'test')

# Get all files with 'errgroup' in them. The only place where direct usage is allowed is in error_group_wrapper.go
# `|| true` because grep exits 1 when nothing matches, which under `set -e` would
# abort here and exit 1 with no output — indistinguishable from a real violation.
files=$(grep -l 'errgroup' ${all_files} || true)

found_error=false

for file in $files; do
    # Check if the file is not one of the permitted usages
    if [ "$file" != "entities/errors/error_group_wrapper.go" ]; then
        echo "Error: $file directly uses error groups. Please use entities/errors/error_group_wrapper.go instead."
        found_error=true
    fi
done

if [ "$found_error" = true ]; then
    exit 1
fi