#!/bin/bash

# This script is for running the docker build for GSI's weaivate container
# Note:  We may end up removing this and re-factoring the docker build if/when we merge with main weaviate

set -e
set -x

cd .. && DOCKER_BUILDKIT=0 docker build --no-cache -f gsi/docker/Dockerfile.gemini -t gsi/weaviate .
