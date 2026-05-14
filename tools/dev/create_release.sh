#!/usr/bin/env bash

set -euo pipefail

print_usage() {
  echo "Usage: $0 <version> [--branch=<base_branch>] [--remote=<remote_name>]"
  echo "Example:"
  echo "$0 v1.37.14"
  echo "$0 v1.37.14 --branch=stable/v1.37"
  echo "$0 v1.37.14 --branch=stable/v1.37 --remote=upstream"
}

if [ "$#" -lt 1 ]; then
  print_usage
  exit 1
fi

REQUIRED_TOOLS="jq git make go"
for tool in $REQUIRED_TOOLS; do
  if ! hash "$tool" 2>/dev/null; then
    echo "This script requires '$tool', but it is not installed."
    exit 1
  fi
done

RAW_VERSION="$1"
VERSION="${1#v}"
shift

BASE_BRANCH=""
REMOTE="origin"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --branch=*)
      BASE_BRANCH="${1#--branch=}"
      ;;
    --remote=*)
      REMOTE="${1#--remote=}"
      ;;
    *)
      echo "Unknown argument: '$1'"
      print_usage
      exit 1
      ;;
  esac
  shift
done

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]]; then
  echo "Invalid version: '$RAW_VERSION'. Expected format: v1.2.3 or 1.2.3 (optionally with a -suffix)."
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

echo "==> Starting release process for v$VERSION"

if [ -n "$(git status --porcelain)" ]; then
  echo "Error: working tree has uncommitted changes. Commit, stash, or clean them before running this script."
  git status --short
  exit 1
fi

if [ -n "$BASE_BRANCH" ]; then
  echo "==> Fetching latest changes from remote '$REMOTE'"
  if ! git fetch "$REMOTE"; then
    echo "Error: 'git fetch $REMOTE' failed."
    exit 1
  fi

  echo "==> Checking out base branch '$BASE_BRANCH'"
  if ! git checkout "$BASE_BRANCH"; then
    echo "Error: base branch '$BASE_BRANCH' does not exist or cannot be checked out."
    exit 1
  fi

  echo "==> Fast-forwarding '$BASE_BRANCH' to '$REMOTE/$BASE_BRANCH'"
  if ! git merge --ff-only "$REMOTE/$BASE_BRANCH"; then
    echo "Error: 'git merge --ff-only $REMOTE/$BASE_BRANCH' failed."
    exit 1
  fi

  echo "==> Creating release branch 'prepare-release-v$VERSION'"
  git checkout -b "prepare-release-v$VERSION"
fi

SCHEMA_FILE="openapi-specs/schema.json"

echo "==> Running 'make deps' to vendor Go dependencies"
make deps

CURRENT_VERSION="$(jq -r '.info.version' "$SCHEMA_FILE")"
echo "==> Updating info.version in $SCHEMA_FILE from $CURRENT_VERSION to $VERSION"
TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT
jq --arg v "$VERSION" '.info.version = $v' "$SCHEMA_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$SCHEMA_FILE"
trap - EXIT

echo "==> Running tools/prepare_release.sh"
tools/prepare_release.sh

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo ""
echo "Release v$VERSION is prepared."
echo "Push the prepare release branch with: git push $REMOTE $CURRENT_BRANCH and create prepare release pull request"

