#!/usr/bin/env bash

set -euo pipefail

DOCKER_REPO_WEAVIATE="semitechnologies/weaviate"

only_build_amd64=false
only_build_arm64=false
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --amd64-only) only_build_amd64=true;;
    --arm64-only) only_build_arm64=true;;
    --help|-h) printf '%s\n' \
      "Options:"\
      "--amd64-only"\
      "--arm64-only"\
      "--help | -h"; exit 1;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done

function release() {
  DOCKER_REPO=$DOCKER_REPO_WEAVIATE

  # for multi-platform build
  if [ "$only_build_amd64" == "false" ] && [ "$only_build_arm64" == "false" ]; then
    docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  fi

  docker buildx create --use

  # nightly tag was added to be pushed on merges to main branch, latest tag is used to get latest released version
  tag_latest="${DOCKER_REPO}:latest"
  tag_exact=
  tag_preview=
  tag_preview_semver=
  tag_nightly=

  git_revision=$(echo "$GITHUB_SHA" | cut -c1-7)
  git_branch="$GITHUB_HEAD_REF"
  build_user="ci"
  build_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Determine architecture and platform
  arch=""
  if $only_build_amd64; then
    build_platform="linux/amd64"
    arch="amd64"
  elif $only_build_arm64; then
    build_platform="linux/arm64"
    arch="arm64"
  else
    build_platform="linux/amd64,linux/arm64"
  fi

  weaviate_version="$(jq -r '.info.version' < openapi-specs/schema.json)"
  if [ "$GITHUB_REF_TYPE" == "tag" ]; then
      if [ "$GITHUB_REF_NAME" != "v$weaviate_version" ]; then
          echo "The release tag ($GITHUB_REF_NAME) and Weaviate version (v$weaviate_version) are not equal! Can't release."
          return 1
      fi
      tag_exact="${DOCKER_REPO}:${weaviate_version}"
      git_branch="$GITHUB_REF_NAME"
  else
    if [ -n "$arch" ]; then
      tag_preview_semver="${DOCKER_REPO}:${weaviate_version}-${git_revision}.${arch}"
    else
      tag_preview_semver="${DOCKER_REPO}:${weaviate_version}-${git_revision}"
    fi
    pr_title="$(echo -n "$PR_TITLE" | tr '[:upper:]' '[:lower:]' | tr -c -s '[:alnum:]' '-' | sed 's/-$//g')"
    if [ "$pr_title" == "" ]; then
      git_branch="$GITHUB_REF_NAME"
      branch_name="$(echo -n $GITHUB_REF_NAME | sed 's/\//-/g')"
      tag_preview="${DOCKER_REPO}:${branch_name}-${git_revision}"
      weaviate_version="${branch_name}-${git_revision}"
      git_branch="$GITHUB_HEAD_REF"
      if [ "$branch_name" == "main" ]; then
        tag_nightly="${DOCKER_REPO}:nightly"
      fi
    else
      if [ -n "$arch" ]; then
        tag_preview="${DOCKER_REPO}:preview-${pr_title}-${git_revision}.${arch}"
      else
        tag_preview="${DOCKER_REPO}:preview-${pr_title}-${git_revision}"
      fi
      weaviate_version="preview-${pr_title}-${git_revision}"
    fi
  fi

  args=("--build-arg=GIT_REVISION=$git_revision"
        "--build-arg=GIT_BRANCH=$git_branch"
        "--build-arg=BUILD_USER=$build_user"
        "--build-arg=BUILD_DATE=$build_date"
        "--build-arg=CGO_ENABLED=0" # Force-disable CGO for cross-compilation - Fixes segmentation faults on arm64 (https://docs.docker.com/docker-hub/image-library/trusted-content/#alpine-images)
        "--platform=$build_platform"
        "--target=weaviate"
        "--push")

  if [ -n "$tag_exact" ]; then
    # exact tag on main
    args+=("-t=$tag_exact")
    args+=("-t=$tag_latest")
  fi
  if [ -n "$tag_preview" ]; then
    # preview tag on PR builds
    args+=("-t=$tag_preview")
    args+=("-t=$tag_preview_semver")
    if [ -n "$tag_nightly" ]; then
      args+=("-t=$tag_nightly")
    fi
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
