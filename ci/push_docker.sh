#!/bin/bash

DOCKER_REPO="semitechnologies/weaviate"

function release() {
  # for multi-platform build
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  docker buildx create --use

  git_hash=$(echo "$GITHUB_SHA" | cut -c1-7)

  weaviate_version="$(jq -r '.info.version' < openapi-specs/schema.json)"

  args=("--build-arg=GITHASH=$git_hash" "--platform=linux/amd64,linux/arm64" "--target=weaviate" "-t=${PREVIEW_TAG}" "--push")

  docker buildx build "${args[@]}" .

}

release
