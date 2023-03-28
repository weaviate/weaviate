#!/bin/bash

# This will build a t2v container called "gsi/bench2v" 

export MODEL_NAME=distilroberta-base
DOCKER_BUILDKIT=0 docker build --build-arg MODEL_NAME=${MODEL_NAME} -f gsi-bench2v.Dockerfile -t gsi/bench2v .
