#!/usr/bin/env bash

set -euo pipefail

# Get the AMD64 and ARM64 temp tags from inputs
amd64_temp_tag=${AMD64_TEMP_TAG:-""}
arm64_temp_tag=${ARM64_TEMP_TAG:-""}

if [ -z "$amd64_temp_tag" ] || [ -z "$arm64_temp_tag" ]; then
  echo "Error: Both AMD64_TEMP_TAG and ARM64_TEMP_TAG must be provided"
  exit 1
fi

# Enable experimental features for manifest command
export DOCKER_CLI_EXPERIMENTAL=enabled

# Read final tags from file if provided, otherwise use empty array
final_tags=()
if [ -f "final_tags.txt" ]; then
  mapfile -t final_tags < final_tags.txt
elif [ -n "${FINAL_TAGS:-}" ]; then
  # Split FINAL_TAGS by comma if provided as environment variable
  IFS=',' read -ra final_tags <<< "$FINAL_TAGS"
else
  echo "Error: No final tags provided. Either final_tags.txt must exist or FINAL_TAGS environment variable must be set"
  exit 1
fi

if [ ${#final_tags[@]} -eq 0 ]; then
  echo "Error: No final tags found"
  exit 1
fi

echo "Creating manifests for the following tags:"
printf '  %s\n' "${final_tags[@]}"
echo "Using temporary images:"
echo "  AMD64: $amd64_temp_tag"
echo "  ARM64: $arm64_temp_tag"

# Verify both temp tags exist
echo "Verifying temporary tags exist..."
if ! docker manifest inspect "$amd64_temp_tag" > /dev/null 2>&1; then
  echo "Error: AMD64 temporary tag $amd64_temp_tag not found"
  exit 1
fi

if ! docker manifest inspect "$arm64_temp_tag" > /dev/null 2>&1; then
  echo "Error: ARM64 temporary tag $arm64_temp_tag not found"
  exit 1
fi

echo "Both temporary tags verified."

# Create and push manifest for each final tag
for tag in "${final_tags[@]}"; do
  echo "Creating manifest for $tag"
  
  # Create the manifest with both architectures
  docker manifest create "$tag" "$amd64_temp_tag" "$arm64_temp_tag" --amend
  
  # Add architecture annotations
  docker manifest annotate "$tag" "$amd64_temp_tag" --arch amd64 --os linux
  docker manifest annotate "$tag" "$arm64_temp_tag" --arch arm64 --os linux
  
  # Push the manifest with retries
  for i in {1..3}; do
    if docker manifest push --purge "$tag"; then
      echo "Successfully pushed manifest for $tag"
      break
    fi
    
    if [ $i -eq 3 ]; then
      echo "Failed to push manifest for $tag after 3 attempts"
      exit 1
    fi
    
    echo "Retrying manifest push (attempt $i)..."
    sleep 5
  done
done

echo "All manifests successfully created and pushed."

# Optionally cleanup temp tags
if [[ "${CLEANUP_TEMP_TAGS:-false}" == "true" ]]; then
  echo "Cleaning up temporary tags..."
  docker buildx imagetools rm "$amd64_temp_tag" > /dev/null 2>&1 || echo "Warning: Failed to remove $amd64_temp_tag"
  docker buildx imagetools rm "$arm64_temp_tag" > /dev/null 2>&1 || echo "Warning: Failed to remove $arm64_temp_tag" 
  echo "Cleanup complete."
fi 