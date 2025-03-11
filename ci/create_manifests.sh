#!/usr/bin/env bash

set -euo pipefail

# Load both tarballs
echo "Loading AMD64 image..."
docker load -i amd64.tar
echo "Loading ARM64 image..."
docker load -i arm64.tar

# Read tags from file
mapfile -t TAGS < final_tags.txt

# For each tag, create and push a manifest
for tag in "${TAGS[@]}"; do
  echo "Creating manifest for $tag..."
  
  # Create manifest using buildx imagetools
  docker buildx imagetools create \
    --tag "$tag" \
    "semitechnologies/weaviate:amd64-local" \
    "semitechnologies/weaviate:arm64-local"
    
  echo "Successfully pushed manifest for $tag"
done

# Clean up local images
docker image rm "semitechnologies/weaviate:amd64-local" "semitechnologies/weaviate:arm64-local"
