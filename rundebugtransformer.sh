#!/bin/bash

source .env.transformer
export OPENAI_APIKEY
export QUERY_DEFAULTS_LIMIT
export AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED
export PERSISTENCE_DATA_PATH
export DEFAULT_VECTORIZER_MODULE
export CLUSTER_HOSTNAME
export ENABLE_MODULES
export TRANSFORMERS_INFERENCE_API
export GEMINI_ALLOCATION_ID
export GEMINI_DATA_DIRECTORY
export GEMINI_VERBOSE
export GOPATH
export PATH

which dlv

dlv debug cmd/weaviate-server/main.go -- --host=0.0.0.0 --port=8081 --scheme=http
