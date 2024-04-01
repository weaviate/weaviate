#!/usr/bin/env bash

set -euo pipefail

# Get all files with 'errgroup' in them. The only place where direct usage is allowed is in error_group_wrapper.go
files=$(git ls-files | xargs grep -l 'errgroup')

found_error=false

for file in $files; do
    # Check if the file is not one of the permitted usages
    if [ "$file" != "entities/errors/error_group_wrapper.go" ] && [ "$file" != "tools/linter_error_groups.sh" ]; then
        echo "Error: $file directly uses error groups. Please use entities/errors/error_group_wrapper.go instead."
        found_error=true
    fi
done

if [ "$found_error" = true ]; then
    exit 1
fi