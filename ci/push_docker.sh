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

  # Only set up QEMU if we're doing a multi-arch build or cross-architecture build
  need_qemu=false
  
  # Determine if we need QEMU based on current architecture and build target
  current_arch=$(uname -m)
  if [[ "$current_arch" == "x86_64" && "$only_build_arm64" == "true" ]]; then
    need_qemu=true
  elif [[ "$current_arch" == "aarch64" && "$only_build_amd64" == "true" ]]; then
    need_qemu=true
  elif [[ "$only_build_amd64" == "false" && "$only_build_arm64" == "false" ]]; then
    # Multi-arch build always needs QEMU
    need_qemu=true
  fi
  
  if [[ "$need_qemu" == "true" ]]; then
    echo "Setting up QEMU for cross-architecture builds"
    docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
  else
    echo "Native build detected, skipping QEMU setup"
  fi

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
      if [ "$branch_name" == "main" ]; then
        tag_nightly="${DOCKER_REPO}:nightly"
      fi
    else
      tag_preview="${DOCKER_REPO}:preview-${pr_title}-${git_revision}"
      weaviate_version="preview-${pr_title}-${git_revision}"
    fi
  fi

  # Create list of final tags
  final_tags=()
  if [ -n "$tag_exact" ]; then
    final_tags+=("$tag_exact")
    final_tags+=("$tag_latest")
  fi
  if [ -n "$tag_preview" ]; then
    final_tags+=("$tag_preview")
  fi
  if [ -n "$tag_preview_semver" ]; then
    final_tags+=("$tag_preview_semver")
  fi
  if [ -n "$tag_nightly" ]; then
    final_tags+=("$tag_nightly")
  fi

  # Determine architecture and platform
  if $only_build_amd64; then
    build_platform="linux/amd64"
    arch="amd64"
  elif $only_build_arm64; then
    build_platform="linux/arm64"
    arch="arm64"
  else
    build_platform="linux/amd64,linux/arm64"
  fi

  # For multi-architecture builds, use standard buildx approach with all tags
  if [ "$only_build_amd64" == "false" ] && [ "$only_build_arm64" == "false" ]; then
    # Always create buildx builder
    docker buildx create --use
    
    echo "Building multi-architecture image for $build_platform"
    args=("--build-arg=GIT_REVISION=$git_revision" "--build-arg=GIT_BRANCH=$git_branch" "--build-arg=BUILD_USER=$build_user" "--build-arg=BUILD_DATE=$build_date" "--platform=$build_platform" "--target=weaviate" "--push")
    
    for tag in "${final_tags[@]}"; do
      args+=("-t=$tag")
    done

    docker buildx build "${args[@]}" . || exit 1
  else
    # For single architecture builds, just create a temp tag
    echo "Building single-architecture image for $build_platform"
    
    # Create a dedicated builder for this architecture with a clean cache
    builder_name="$arch-builder-$(date +%s)"
    docker buildx create --name="$builder_name" --use
    
    # Create a single temporary tag for this architecture
    temp_tag="${DOCKER_REPO}:temp-${git_revision}-${arch}"
    
    # Build once with the temporary tag
    echo "Building $arch architecture with temporary tag: $temp_tag"
    docker buildx build \
      --build-arg="GIT_REVISION=$git_revision" \
      --build-arg="GIT_BRANCH=$git_branch" \
      --build-arg="BUILD_USER=$build_user" \
      --build-arg="BUILD_DATE=$build_date" \
      --platform="$build_platform" \
      --target=weaviate \
      --provenance=false \
      -t="$temp_tag" \
      --push \
      . || exit 1
      
    # Clean up builder
    docker buildx rm "$builder_name" || true
    
    # Output the temp tag for the create_manifests.sh script
    echo "TEMP_TAG=$temp_tag" >> "$GITHUB_OUTPUT"
  fi

  # Set output variables for GitHub Actions
  if [ -n "$tag_preview" ]; then
    echo "PREVIEW_TAG=$tag_preview" >> "$GITHUB_OUTPUT"
    echo "PREVIEW_SEMVER_TAG=$tag_preview_semver" >> "$GITHUB_OUTPUT"
  elif [ -n "$tag_exact" ]; then
    echo "PREVIEW_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
    echo "PREVIEW_SEMVER_TAG=$tag_exact" >> "$GITHUB_OUTPUT"
  fi
  
  # Create a file with all final tags for the create_manifests.sh script
  printf '%s\n' "${final_tags[@]}" > final_tags.txt
}

release

