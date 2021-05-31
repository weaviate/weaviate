#!/bin/bash

DOCKER_REPO="semitechnologies/weaviate"

function release() {
  # for multi-platform build
  docker buildx create --use

  if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    # only run on non-pr builds, otherwise we have duplicates
    return 0
  fi

  tag_latest="${DOCKER_REPO}:latest"
  tag_exact=

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

  args=("--platform=linux/amd64,linux/arm64" "--target=weaviate" "-t=$tag_latest" "--push")
  if [ -n "$tag_exact" ]; then
    args+=("-t=$tag_exact")
  fi

  docker buildx build "${args[@]}" .
}

release
