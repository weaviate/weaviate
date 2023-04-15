#!/bin/bash

# This will build a t2v-like container for benchmarking called "gsi/bench2v" 

DOCKER_BUILDKIT=0 docker build -f gsi-bench2v.Dockerfile -t gsi/bench2v .
