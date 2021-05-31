#!/bin/bash

DOCKER_REPO="semitechnologies/weaviate"

function release() {
  if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    # only run on non-pr builds, otherwise we have duplicates
    return 0
  fi

  tag_exact=
  tag_latest="${DOCKER_REPO}:latest"

  if [ "$TRAVIS_BRANCH" = "master" ]; then
    tag_exact="${DOCKER_REPO}:${weaviate_version}-$(echo "$TRAVIS_COMMIT" | cut -c1-7)"
  elif [ -n "$TRAVIS_TAG" ]; then
    weaviate_version="$(jq -r '.info.version' < openapi-specs/schema.json)"
        if [ "$TRAVIS_TAG" = "v$weaviate_version" ]; then
            echo "The release tag ($TRAVIS_TAG) and Weaviate version (v$weaviate_version) are not equal! Can't release."
            return 1
        fi
        tag_exact="${DOCKER_REPO}:${weaviate_version}"
  fi

  docker buildx build --platform=linux/amd64 \
     -t $tag_exact-amd64 \
     -t $tag_latest-amd64 \
     --target weaviate .

  docker buildx build --platform=linux/arm64 \
     -t $tag_exact-arm64 \
     -t $tag_latest-arm64 \
     --target weaviate .
}

release
