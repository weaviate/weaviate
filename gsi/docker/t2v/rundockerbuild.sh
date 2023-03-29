#!/bin/bash

# This will build a t2v container called "gsi/t2v" locally which embeds the distilroberta-base model

export MODEL_NAME=distilroberta-base
DOCKER_BUILDKIT=0 docker build --build-arg MODEL_NAME=${MODEL_NAME} -f gsi-t2v.distilroberta-base.Dockerfile -t gsi/t2v .
