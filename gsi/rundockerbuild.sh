#!/bin/bash

# This script is for running the docker build for GSI's weaivate container
# Note:  We may end up removing this and re-factoring the docker build if/when we merge with main weaviate

set -e
set -x

NO_CACHE_FLAG="" #--no-cache
export NO_CACHE_FLAG

cd .. && DOCKER_BUILDKIT=0 docker build $NO_CACHE_FLAG -f gsi/docker/Dockerfile.gemini -t gsi/weaviate .
