#!/bin/bash

# This script will help you build the weaivate executable locally (ie, without docker ).
# I don't recommend that you run this unless you know what you are doing with golang.
# Note that we may likely remove this script as a cleanup activity if/when we merge with main weaviate.

set -e
set -x

# You need to create this file with values for the env vars below
if [ ! -f .env ]; then
    echo "Could not find a file called .env"
    exit 1
fi
source .env

# Standard Weaviate env vars
export OPENAI_APIKEY
export QUERY_DEFAULTS_LIMIT
export AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED
export PERSISTENCE_DATA_PATH
export DEFAULT_VECTORIZER_MODULE
export CLUSTER_HOSTNAME
export ENABLE_MODULES
export TRANSFORMERS_INFERENCE_API

# New Weaviate env vars
export MODULES_PATH
export DEFAULT_VECTOR_INDEX_TYPE

# Gemini Plugin env vars
export GEMINI_ALLOCATION_ID
export GEMINI_DATA_DIRECTORY
export GEMINI_FVS_SERVER
export GEMINI_DEBUG
export GEMINI_MIN_RECORDS_CHECK
export GOPATH
export PATH

# Golang env vars
GOPATH="$HOME/go"
GOROOT=/usr/local/go
export PATH=$GOPATH:$GOROOT/bin:$PATH

# You can choose to start 'fresh' or not
rm -fr /var/lib/weaviate/*

# DLV is a Golang debugger that will need to have installed locally
which dlv

cd .. && dlv debug cmd/weaviate-server/main.go --build-flags -modfile=gsi/go.mod -- --host=0.0.0.0 --port=8081 --scheme=http
