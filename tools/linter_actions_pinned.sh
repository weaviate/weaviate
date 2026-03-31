#!/usr/bin/env bash

set -euo pipefail

# Lint GitHub Actions workflow files to ensure all external actions are pinned
# to commit SHAs (not version tags or branches).
#
# Valid:   uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5
# Invalid: uses: actions/checkout@v5

found_error=false

for file in .github/workflows/*.yaml .github/workflows/*.yml; do
    [ -f "$file" ] || continue

    # Match 'uses:' lines with external actions (skip local actions starting with ./)
    while IFS= read -r line; do
        # Extract the action reference after 'uses:'
        ref=$(echo "$line" | sed -n 's/.*uses:[[:space:]]*//p' | sed 's/[[:space:]]*#.*//')

        # Skip local actions
        if [[ "$ref" == ./* ]]; then
            continue
        fi

        # Extract the part after @
        version=$(echo "$ref" | sed -n 's/.*@//p')

        # A valid SHA pin is 40 hex characters
        if ! echo "$version" | grep -qE '^[0-9a-f]{40}$'; then
            echo "Error: $file: action not pinned to commit SHA: $ref"
            found_error=true
        fi
    done < <(grep -n 'uses:' "$file" | grep -v '\./')
done

if [ "$found_error" = true ]; then
    echo ""
    echo "All external GitHub Actions must be pinned to a full commit SHA."
    echo "Example: uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5"
    exit 1
fi

echo "All GitHub Actions are properly pinned to commit SHAs."
