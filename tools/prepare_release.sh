#!/bin/bash 

set -euo pipefail

VERSION=$(jq -r '.info.version' openapi-specs/schema.json)
LANGUAGES="en nl"
IMAGE_BASE="semitechnologies/weaviate:"
MSG=${1:""}

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
See also: example docker compose files in [english](https://github.com/semi-technologies/weaviate/tree/$VERSION/docker-compose/runtime/en) and [dutch](https://github.com/semi-technologies/weaviate/tree/$VERSION/docker-compose/runtime/nl).

## Breaking Changes

## New Features

## Fixes

EOM



