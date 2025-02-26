#!/bin/bash

DOCKER_BUILDKIT=1 docker build \
  -f Dockerfile.cuda \
  --no-cache \
  --progress=plain \
  --target weaviate_cuvs \
  -t weaviate-cuvs:latest .