#!/bin/bash

export OPENAI_APIKEY=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
export QUERY_DEFAULTS_LIMIT=25
export PERSISTENCE_DATA_PATH=true
export PERSISTENCE_DATA_PATH=/var/lib/weaviate
export DEFAULT_VECTORIZER_MODULE=text2vec-openai
export CLUSTER_HOSTNAME=node1
export ENABLE_MODULES=text2vec-openai
export GOPATH="$HOME/go"
export PATH="$GOPATH/bin:$PATH"

which dlv

dlv debug cmd/weaviate-server/main.go -- --host==0.0.0.0 --port=8081 --scheme=http
