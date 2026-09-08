#!/usr/bin/env bash

set -euo pipefail

VERSION="$(jq -r '.info.version' openapi-specs/schema.json)"
LANGUAGES="en nl de cs it"
IMAGE_BASE="semitechnologies/weaviate:"
REQUIRED_TOOLS="jq git"
MSG=""
PUBLISH_PROTOS=false

for arg in "$@"; do
    case "$arg" in
        --protos) PUBLISH_PROTOS=true ;;
        *) MSG="$arg" ;;
    esac
done

for tool in $REQUIRED_TOOLS; do
  if ! hash "$tool" 2>/dev/null; then
    echo "This script requires '$tool', but it is not installed."
    exit 1
  fi
done

if git rev-parse "v$VERSION" >/dev/null 2>&1; then
  echo "Cannot prepare release, a release for v$VERSION already exists"
  exit 1
fi

tools/gen-code-from-swagger.sh

git commit -a -m "prepare release v$VERSION"

git tag -a "v$VERSION" -m "release v$VERSION - $MSG"

if [[ "$PUBLISH_PROTOS" == true ]]; then
    git tag -a "grpc/generated/protocol/v$VERSION" -m "release grpc/generated/protocol v$VERSION"
fi

echo "You can use the following template for the release notes, copy/paste below the line"
echo "----------------------------"
VERSION="$VERSION" LANGUAGES="$LANGUAGES" go run ./tools/release_template
