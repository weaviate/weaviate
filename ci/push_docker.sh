#!/bin/bash

DOCKER_REPO="semitechnologies/weaviate"

function release() {
  # for multi-platform build
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  docker buildx create --use

  tag_latest="${DOCKER_REPO}:latest"
  tag_exact=
  tag_preview=

  git_revision=$(echo "$GITHUB_SHA" | cut -c1-7)
  git_branch=$(git branch)
  build_user="ci"
  build_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  prefix="preview"
  if [ "$git_branch" == "main" ] || [ "$git_branch" == "stable/v*" ]; then
    prefix="$(echo $git_branch | sed 's/\//-/g')"
  fi


  weaviate_version="$(jq -r '.info.version' < openapi-specs/schema.json)"
  if [ "$GITHUB_REF_NAME" == "main" ]; then
    tag_exact="${DOCKER_REPO}:${weaviate_version}-${git_hash}"
  elif [  "$GITHUB_REF_TYPE" == "tag" ]; then
        if [ "$GITHUB_REF_NAME" != "v$weaviate_version" ]; then
            echo "The release tag ($GITHUB_REF_NAME) and Weaviate version (v$weaviate_version) are not equal! Can't release."
            return 1
        fi
        tag_exact="${DOCKER_REPO}:${weaviate_version}"
  else
    pr_title="$(echo -n "$PR_TITLE" | tr '[:upper:]' '[:lower:]' | tr -c -s '[:alnum:]' '-' | sed 's/-$//g')"
    if [ "$pr_title" == "" ]; then
      tag_preview="${DOCKER_REPO}:${prefix}-${git_hash}"
      weaviate_version="${prefix}-${git_hash}"
    else
      tag_preview="${DOCKER_REPO}:${prefix}-${pr_title}-${git_hash}"
      weaviate_version="${prefix}-${pr_title}-${git_hash}"
    fi
  fi

  args=("--build-arg=GIT_REVISION=$git_revision" "--build-arg=GIT_BRANCH=$git_branch" "--build-arg=VERSION=$weaviate_version" "--build-arg=BUILD_USER=$build_user" "--build-arg=BUILD_DATE=$build_date" "--platform=linux/amd64,linux/arm64" "--target=weaviate" "--push")
  if [ -n "$tag_exact" ]; then
    # exact tag on main
    args+=("-t=$tag_exact")
    args+=("-t=$tag_latest")
  fi
  if [ -n "$tag_preview" ]; then
    # preview tag on PR builds
    args+=("-t=$tag_preview")
  fi

  docker buildx build "${args[@]}" .

  if [ -n "$tag_preview" ]; then
    echo "PREVIEW_TAG=$tag_preview" >> "$GITHUB_OUTPUT"
  elif [ -n "$tag_exact" ]; then
    echo "PREVIEW_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
  fi
}

release
