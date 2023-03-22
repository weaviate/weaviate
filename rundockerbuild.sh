#!/bin/bash

DOCKER_BUILDKIT=0 docker build -f Dockerfile -t gsi/weaviate .
