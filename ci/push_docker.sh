#!/usr/bin/env bash

set -euo pipefail

DOCKER_REPO_WEAVIATE="semitechnologies/weaviate"
DOCKER_REPO_SERVERLESS="semitechnologies/weaviate-experimental"
target_weaviate="weaviate"
target_weaviate_experimental="weaviate_experimental"

function release() {
  # default target is weaviate
  DOCKER_REPO=$DOCKER_REPO_WEAVIATE
  target=$target_weaviate
  if [ $# -eq 1 ] && [ -n "$1" ]; then
    if [ "$1" == "$target_weaviate_experimental" ]; then
      DOCKER_REPO=$DOCKER_REPO_SERVERLESS
      target=$target_weaviate_experimental
    elif [ "$1" != "$target_weaviate" ]; then
      echo "release script wrong parameter: $1, possible values are: $target_weaviate, $target_weaviate_experimental."
      return 1
    fi
  fi

  # for multi-platform build
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  docker buildx create --use

  tag_latest="${DOCKER_REPO}:latest"
  tag_exact=
  tag_preview=
  tag_preview_semver=

  git_revision=$(echo "$GITHUB_SHA" | cut -c1-7)
  git_branch="$GITHUB_HEAD_REF"
  build_user="ci"
  build_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  weaviate_version="$(jq -r '.info.version' < openapi-specs/schema.json)"
  if [ "$GITHUB_REF_TYPE" == "tag" ]; then
      if [ "$GITHUB_REF_NAME" != "v$weaviate_version" ]; then
          echo "The release tag ($GITHUB_REF_NAME) and Weaviate version (v$weaviate_version) are not equal! Can't release."
          return 1
      fi
      tag_exact="${DOCKER_REPO}:${weaviate_version}"
      git_branch="$GITHUB_REF_NAME"
  else
    tag_preview_semver="${DOCKER_REPO}:${weaviate_version}-${git_revision}"
    pr_title="$(echo -n "$PR_TITLE" | tr '[:upper:]' '[:lower:]' | tr -c -s '[:alnum:]' '-' | sed 's/-$//g')"
    if [ "$pr_title" == "" ]; then
      git_branch="$GITHUB_REF_NAME"
      branch_name="$(echo -n $GITHUB_REF_NAME | sed 's/\//-/g')"
      tag_preview="${DOCKER_REPO}:${branch_name}-${git_revision}"
      weaviate_version="${branch_name}-${git_revision}"
      git_branch="$GITHUB_HEAD_REF"
    else
      tag_preview="${DOCKER_REPO}:preview-${pr_title}-${git_revision}"
      weaviate_version="preview-${pr_title}-${git_revision}"
    fi
  fi

  args=("--build-arg=GIT_REVISION=$git_revision" "--build-arg=GIT_BRANCH=$git_branch" "--build-arg=BUILD_USER=$build_user" "--build-arg=BUILD_DATE=$build_date" "--platform=linux/amd64,linux/arm64" "--push")

  # build weaviate or experimental image
  args+=("--target=$target")

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

  # build weaviate image
  docker buildx build "${args[@]}" .

  if [ -n "$tag_preview" ]; then
    echo "PREVIEW_TAG=$tag_preview" >> "$GITHUB_OUTPUT"
    echo "PREVIEW_SEMVER_TAG=$tag_preview_semver" >> "$GITHUB_OUTPUT"
  elif [ -n "$tag_exact" ]; then
    echo "PREVIEW_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
  fi
}

release "$@"
