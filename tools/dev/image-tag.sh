#!/usr/bin/env bash

set -o pipefail
set -o errexit
set -o nounset

GIT_REVISION=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
STABLE_REVISION=$(jq -r '.info.version' < openapi-specs/schema.json)

# If it's a stable branch use the official `.info.version` from openapi-specs
# for any other branch use the SHA hash as image tag.
#
# e.g:
# stable: semitechnologies/weaviate:1.25.29
# other: semitechnologies/weaviate:1.25.29-cac50cfe2
#
# Gives ability to build immutable image at any commit in your feature branch. Easy to put it on dev cluster, no cache issues, etc.

if [[ "$GIT_BRANCH" == stable* ]]
then
	echo "${STABLE_REVISION}"
else
        echo "${STABLE_REVISION}-${GIT_REVISION}"
fi
