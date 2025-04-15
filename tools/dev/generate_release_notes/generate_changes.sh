#!/usr/bin/env bash
set -euo pipefail

CURRENT_VERSION=${1:-""}
PREVIOUS_VERSION=${2:-""}

if [ -z "$CURRENT_VERSION" ]; then
  echo "<current-version> <previous-version> are missing. Usage: ./generate_changes.sh <current-version> <previous-version>"
  exit 1
fi
if [ -z "$PREVIOUS_VERSION" ]; then
  echo "<previous-version> is missing. Usage: ./generate_changes.sh <current-version> <previous-version>"
  exit 1
fi

git --no-pager log $CURRENT_VERSION...$PREVIOUS_VERSION -P --grep="^(Merge pull request.*|.* \(\#[0-9]+\)$)" --pretty=format:'%s'
