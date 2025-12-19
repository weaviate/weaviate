#!/bin/bash
# Environment Variables Audit Script
# This script finds all env vars in the codebase and outputs a CSV for review

echo "ENV_VAR_NAME,FIRST_FILE,FIRST_LINE"

# Find all os.Getenv and os.LookupEnv calls with proper pattern matching
grep -rnoE 'os\.(Getenv|LookupEnv)\("[^"]+"\)' --include="*.go" . | while IFS=: read -r file line match; do
  # Extract the env var name from the match
  env_var=$(echo "$match" | sed -E 's/os\.(Getenv|LookupEnv)\("([^"]+)"\)/\2/')

  if [ -n "$env_var" ]; then
    echo "$env_var,$file,$line"
  fi
done | sort -t, -k1,1 -u
