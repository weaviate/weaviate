#!/usr/bin/env bash

CONFIG=${1:-local-development}

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

export GO111MODULE=on
export LOG_LEVEL=${LOG_LEVEL:-"debug"}
export LOG_FORMAT=${LOG_FORMAT:-"text"}
export PROMETHEUS_MONITORING_ENABLED=${PROMETHEUS_MONITORING_ENABLED:-"true"}
export ENABLE_EXPERIMENTAL_BM25=${ENABLE_EXPERIMENTAL_BM25:-"true"}
export GO_BLOCK_PROFILE_RATE=${GO_BLOCK_PROFILE_RATE:-"20"}
export GO_MUTEX_PROFILE_FRACTION=${GO_MUTEX_PROFILE_FRACTION:-"20"}
export PERSISTENCE_DATA_PATH=${PERSISTENCE_DATA_PATH:-"./data"}
export ORIGIN=${ORIGIN:-"http://localhost:8080"}
export QUERY_DEFAULTS_LIMIT=${QUERY_DEFAULTS_LIMIT:-"20"}
export QUERY_MAXIMUM_RESULTS=${QUERY_MAXIMUM_RESULTS:-"10000"}

case $CONFIG in
  debug)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary" \
      CLUSTER_HOSTNAME="node1" \
      dlv debug ./cmd/weaviate-server -- \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
  ;;

  local-development)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      STORAGE_FS_SNAPSHOTS_PATH="${PWD}/snapshots" \
      ENABLE_MODULES="text2vec-contextionary,storage-filesystem" \
      CLUSTER_HOSTNAME="node1" \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  second-node)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-node2" \
      STORAGE_FS_SNAPSHOTS_PATH="${PWD}/snapshots-node2" \
      CLUSTER_HOSTNAME="node2" \
      CLUSTER_GOSSIP_BIND_PORT="7102" \
      CLUSTER_DATA_BIND_PORT="7103" \
      CLUSTER_JOIN="localhost:7100" \
      CONTEXTIONARY_URL=localhost:9999 \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary,storage-filesystem" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8081 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-transformers)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-transformers \
      TRANSFORMERS_INFERENCE_API="http://localhost:8000" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-transformers" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-transformers-passage-query)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-transformers \
      TRANSFORMERS_PASSAGE_INFERENCE_API="http://localhost:8006" \
      TRANSFORMERS_QUERY_INFERENCE_API="http://localhost:8007" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-transformers" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;  
  local-qna)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-contextionary,qna-transformers" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-image)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      IMAGE_INFERENCE_API="http://localhost:8002" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-contextionary,img2vec-neural" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-ner)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      NER_INFERENCE_API="http://localhost:8003" \
      ENABLE_MODULES="text2vec-contextionary,ner-transformers" \
      CLUSTER_HOSTNAME="node1" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-spellcheck)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      SPELLCHECK_INFERENCE_API="http://localhost:8004" \
      ENABLE_MODULES="text2vec-contextionary,text-spellcheck" \
      CLUSTER_HOSTNAME="node1" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-clip)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=multi2vec-clip \
      CLIP_INFERENCE_API="http://localhost:8005" \
      ENABLE_MODULES="multi2vec-clip" \
      CLUSTER_HOSTNAME="node1" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-oidc)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=false \
      AUTHENTICATION_OIDC_ENABLED=true \
      AUTHENTICATION_OIDC_ISSUER=http://localhost:9090/auth/realms/weaviate \
      AUTHENTICATION_OIDC_USERNAME_CLAIM=email \
      AUTHENTICATION_OIDC_GROUPS_CLAIM=groups \
      AUTHENTICATION_OIDC_CLIENT_ID=demo \
      AUTHORIZATION_ADMINLIST_ENABLED=true \
      AUTHORIZATION_ADMINLIST_USERS=john@doe.com \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      CLUSTER_HOSTNAME="node1" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080
    ;;

  local-multi-text)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      TRANSFORMERS_INFERENCE_API=http://localhost:8000 \
      CLIP_INFERENCE_API=http://localhost:8005 \
      ENABLE_MODULES=text2vec-contextionary,text2vec-transformers,multi2vec-clip \
      CLUSTER_HOSTNAME="node1" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080
    ;;

  local-openai)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-openai \
      ENABLE_MODULES="text2vec-openai" \
      CLUSTER_HOSTNAME="node1" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-huggingface)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-huggingface \
      ENABLE_MODULES="text2vec-huggingface" \
      CLUSTER_HOSTNAME="node1" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-no-modules)
      CLUSTER_HOSTNAME="node1" \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=none \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=3600s \
        --write-timeout=3600s
    ;;

  local-s3)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      STORAGE_S3_ENDPOINT="localhost:9000" \
      STORAGE_S3_BUCKET="weaviate-snapshots" \
      AWS_ACCESS_KEY_ID="aws_access_key" \
      AWS_SECRET_KEY="aws_secret_key" \
      ENABLE_MODULES="text2vec-contextionary,storage-aws-s3" \
      CLUSTER_HOSTNAME="node1" \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-gcs)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      GOOGLE_CLOUD_PROJECT=project-id \
      STORAGE_EMULATOR_HOST=localhost:9090 \
      STORAGE_GCS_ENDPOINT=localhost:9090 \
      STORAGE_GCS_BUCKET=weaviate-snapshots \
      ENABLE_MODULES="text2vec-contextionary,storage-gcs" \
      CLUSTER_HOSTNAME="node1" \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
      ;;

  *) 
    echo "Invalid config" 2>&1
    exit 1
    ;;
esac
