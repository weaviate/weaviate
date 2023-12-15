#!/bin/bash 

set -euo pipefail

VERSION="$(jq -r '.info.version' openapi-specs/schema.json)"
LANGUAGES="en nl de cs it"
IMAGE_BASE="semitechnologies/weaviate:"
MSG=${1:-""}
REQUIRED_TOOLS="jq git"

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

echo "You can use the following template for the release notes, copy/paste below the line"
echo "----------------------------"
VERSION="$VERSION" LANGUAGES="$LANGUAGES" go run ./tools/release_template
