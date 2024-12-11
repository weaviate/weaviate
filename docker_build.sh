#!/bin/bash

DOCKER_BUILDKIT=1 docker build \
  --no-cache \
  --progress=plain \
  --target weaviate_cuvs \
  --build-context libs=/home/ajit/miniforge3/envs/cuvs \
  --build-context app=/home/ajit/weaviate \
  -t weaviate-cuvs:latest .