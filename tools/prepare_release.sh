#!/bin/bash 

set -euo pipefail

VERSION=$(jq -r '.info.version' openapi-specs/schema.json)
LANGUAGES="en nl de cs it"
IMAGE_BASE="semitechnologies/weaviate:"
MSG=${1:""}
REQUIRED_TOOLS="jq yaml2json json2yaml git"

for tool in $REQUIRED_TOOLS; do
  if ! hash "$tool" 2>/dev/null; then
    echo "This script requires '$tool', but it is not installed."
    exit 1
  fi
done

if git rev-parse "$VERSION" >/dev/null 2>&1; then
  echo "Cannot prepare relese, a release for $VERSION already exists"
  exit 1
fi

for lang in $LANGUAGES; do
  yaml2json "docker-compose/runtime/$lang/docker-compose.yml" | \
    jq ".services.weaviate.image = \"$IMAGE_BASE$VERSION\"" | json2yaml > "docker-compose/runtime/$lang/tmp.yml"
  mv "docker-compose/runtime/$lang/tmp.yml" "docker-compose/runtime/$lang/docker-compose.yml"
  echo "Successfully updated docker compose file for $lang"
done

git commit -a -m "prepare release $VERSION"

git tag -a "$VERSION" -m "release $VERSION - $MSG"

echo "You can use the following template for the release notes, copy/paste below the line"
echo "----------------------------"
cat <<EOM
Docker image/tag: \`semitechnologies/weaviate:$VERSION\`
See also: example docker compose files in [English](https://github.com/semi-technologies/weaviate/tree/$VERSION/docker-compose/runtime/en), [German](https://github.com/semi-technologies/weaviate/tree/$VERSION/docker-compose/runtime/de), [Dutch](https://github.com/semi-technologies/weaviate/tree/$VERSION/docker-compose/runtime/nl), [Italian](https://github.com/semi-technologies/weaviate/tree/$VERSION/docker-compose/runtime/it) and [Czech](https://github.com/semi-technologies/weaviate/tree/$VERSION/docker-compose/runtime/cs).

## Breaking Changes

## New Features

## Fixes

EOM



