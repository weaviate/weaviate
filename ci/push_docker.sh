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

# Prints the highest vX.Y.Z tag on origin, without the "v".
# Reads origin because CI checkouts are shallow and have no local tags.
function highest_stable_release() {
  git ls-remote --tags --refs origin 'v*' \
    | sed -n 's|.*refs/tags/v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)$|\1|p' \
    | sort -V | tail -n 1
}

# Points <repo>:latest at the highest stable release whose image is published.
# Release builds run concurrently, so each one calls this after pushing its own
# tag. Whichever finishes last leaves latest on the highest published release,
# regardless of finishing order. Failures only warn: the next release retries.
function promote_latest() {
  local docker_repo="$1"
  local highest

  if ! highest="$(highest_stable_release)" || [ -z "$highest" ]; then
    echo "::warning::Not updating latest: could not list release tags on origin."
    return 0
  fi

  if ! docker buildx imagetools inspect "${docker_repo}:${highest}" >/dev/null 2>&1; then
    echo "Not updating latest: ${docker_repo}:${highest} is not published yet; its own build will update latest."
    return 0
  fi

  echo "Pointing ${docker_repo}:latest at ${docker_repo}:${highest}."
  if ! docker buildx imagetools create -t "${docker_repo}:latest" "${docker_repo}:${highest}"; then
    echo "::warning::Failed to point ${docker_repo}:latest at ${docker_repo}:${highest}."
  fi
}

function release() {
  DOCKER_REPO=$DOCKER_REPO_WEAVIATE

  # for multi-platform build
  if [ "$only_build_amd64" == "false" ] && [ "$only_build_arm64" == "false" ]; then
    docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  fi

  # Reuse the existing builder, if any. The build sets no remote cache, so only
  # this builder's local cache applies. In the Multi-Arch-Docker-Build job it holds
  # the scanned image, and the amd64 image reuses its `apk upgrade` layer.
  docker buildx inspect >/dev/null 2>&1 || docker buildx create --use

  # nightly tag was added to be pushed on merges to main branch; latest is set
  # by promote_latest after release builds
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

  if [ -n "$tag_exact" ]; then
    promote_latest "$DOCKER_REPO"
  fi

  if [ -n "$tag_preview" ]; then
    echo "PREVIEW_TAG=$tag_preview" >> "$GITHUB_OUTPUT"
    echo "PREVIEW_SEMVER_TAG=$tag_preview_semver" >> "$GITHUB_OUTPUT"
  elif [ -n "$tag_exact" ]; then
    echo "PREVIEW_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
    echo "PREVIEW_SEMVER_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
  fi
}

release
