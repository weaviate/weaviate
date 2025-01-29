#!/bin/bash

DOCKER_REPO="semitechnologies/weaviate"

function release() {
  # for multi-platform build
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  docker buildx create --use

  tag_latest="${DOCKER_REPO}:latest"
  tag_exact=
  tag_preview=
  tag_preview_semver=

  git_hash=$(echo "$GITHUB_SHA" | cut -c1-7)

  weaviate_version="$(jq -r '.info.version' < openapi-specs/schema.json)"
  if [  "$GITHUB_REF_TYPE" == "tag" ]; then
        if [ "$GITHUB_REF_NAME" != "v$weaviate_version" ]; then
            echo "The release tag ($GITHUB_REF_NAME) and Weaviate version (v$weaviate_version) are not equal! Can't release."
            return 1
        fi
        tag_exact="${DOCKER_REPO}:${weaviate_version}"
  else
    tag_preview_semver="${DOCKER_REPO}:${weaviate_version}-${git_hash}"
    pr_title="$(echo -n "$PR_TITLE" | tr '[:upper:]' '[:lower:]' | tr -c -s '[:alnum:]' '-' | sed 's/-$//g')"
    if [ "$pr_title" == "" ]; then
      prefix="$(echo -n $GITHUB_REF_NAME | sed 's/\//-/g')"
      tag_preview="${DOCKER_REPO}:${prefix}-${git_hash}"
      weaviate_version="${prefix}-${git_hash}"
    else
      tag_preview="${DOCKER_REPO}:preview-${pr_title}-${git_hash}"
      weaviate_version="preview-${pr_title}-${git_hash}"
    fi
  fi

  args=("--build-arg=GITHASH=$git_hash" "--build-arg=DOCKER_IMAGE_TAG=$weaviate_version" "--platform=linux/amd64,linux/arm64" "--target=weaviate" "--push")
  if [ -n "$tag_exact" ]; then
    # exact tag on main
    args+=("-t=$tag_exact")
    args+=("-t=$tag_latest")
  fi
  if [ -n "$tag_preview" ]; then
    # preview tag on PR builds
    args+=("-t=$tag_preview")
    args+=("-t=$tag_preview_semver")
  fi

  docker buildx build "${args[@]}" . || exit 1

  if [ -n "$tag_preview" ]; then
    echo "PREVIEW_TAG=$tag_preview" >> "$GITHUB_OUTPUT"
    echo "PREVIEW_SEMVER_TAG=$tag_preview_semver" >> "$GITHUB_OUTPUT"
  elif [ -n "$tag_exact" ]; then
    echo "PREVIEW_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
    echo "PREVIEW_SEMVER_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
  fi
}

release
