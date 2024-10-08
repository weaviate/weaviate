#!/bin/bash

DOCKER_REPO="semitechnologies/weaviate"

# Final `tag` is either
# 1. ${DOCKER_REPO}-${WEVIATE_VERSION} if on a stable branch
# 2. ${DOCKER_REPO}-${WEAVIATE_VERSION}-${GIT_BRANCH}-${GIT_REVISION} if on any other branch

function release() {
  # for multi-platform build
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  docker buildx create --use

  tag_latest="${DOCKER_REPO}:latest"
  tag_exact=

  git_revision=$(echo "$GITHUB_SHA" | cut -c1-7)
  git_branch=$(git branch) # we clone only specific branch, so $(git branch) will have single value always.
  build_user="ci"
  build_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  weaviate_version="$(jq -r '.info.version' < openapi-specs/schema.json)"

  if [  "$GITHUB_REF_TYPE" == "tag" ]; then
      if [ "$GITHUB_REF_NAME" != "v$weaviate_version" ]; then
          echo "The release tag ($GITHUB_REF_NAME) and Weaviate version (v$weaviate_version) are not equal! Can't release."
          return 1
      fi
      tag_exact="${DOCKER_REPO}:${weaviate_version}"
  else
      tag_exact="${DOCKER_REPO}:${weaviate_version}-${git_branch}-${git_hash}"
  fi

  args=("--build-arg=GIT_REVISION=$git_revision" "--build-arg=GIT_BRANCH=$git_branch" "--build-arg=BUILD_USER=$build_user" "--build-arg=BUILD_DATE=$build_date" "--platform=linux/amd64,linux/arm64" "--target=weaviate" "--push")
  args+=("-t=$tag_exact")
  args+=("-t=$tag_latest")

  docker buildx build "${args[@]}" .

}

release
