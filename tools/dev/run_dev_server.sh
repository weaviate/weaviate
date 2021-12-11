#!/usr/bin/env bash

CONFIG=${1:-local-development}

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

export GO111MODULE=on
export DEVELOPMENT_UI=on
export LOG_LEVEL=debug
export LOG_FORMAT=text

case $CONFIG in
  debug)
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      PERSISTENCE_DATA_PATH="./data" \
      ENABLE_MODULES="text2vec-contextionary" \
      dlv debug ./cmd/weaviate-server -- \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
  ;;

  local-development)
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      PERSISTENCE_DATA_PATH="./data" \
      ENABLE_MODULES="text2vec-contextionary" \
      CLUSTER_HOSTNAME="node1" \
      CLUSTER_GOSSIP_BIND_PORT="7010" \
      CLUSTER_DATA_BIND_PORT="7011" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  second-node)
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="./data-node2" \
      CLUSTER_HOSTNAME="node2" \
      CLUSTER_GOSSIP_BIND_PORT="7002" \
      CLUSTER_DATA_BIND_PORT="7003" \
      CLUSTER_JOIN="localhost:7000" \
      CONTEXTIONARY_URL=localhost:9999 \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8081 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-transformers)
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-transformers \
      PERSISTENCE_DATA_PATH="./data" \
      TRANSFORMERS_INFERENCE_API="http://localhost:8000" \
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
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      PERSISTENCE_DATA_PATH="./data" \
      QNA_INFERENCE_API="http://localhost:8001" \
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
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      PERSISTENCE_DATA_PATH="./data" \
      IMAGE_INFERENCE_API="http://localhost:8002" \
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
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      PERSISTENCE_DATA_PATH="./data" \
      NER_INFERENCE_API="http://localhost:8003" \
      ENABLE_MODULES="text2vec-contextionary,ner-transformers" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-spellcheck)
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      PERSISTENCE_DATA_PATH="./data" \
      SPELLCHECK_INFERENCE_API="http://localhost:8004" \
      ENABLE_MODULES="text2vec-contextionary,text-spellcheck" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-clip)
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=multi2vec-clip \
      PERSISTENCE_DATA_PATH="./data" \
      CLIP_INFERENCE_API="http://localhost:8005" \
      ENABLE_MODULES="multi2vec-clip" \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-oidc)
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      PERSISTENCE_DATA_PATH="./data" \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=false \
      AUTHENTICATION_OIDC_ENABLED=true \
      AUTHENTICATION_OIDC_ISSUER=http://localhost:9090/auth/realms/weaviate \
      AUTHENTICATION_OIDC_USERNAME_CLAIM=email \
      AUTHENTICATION_OIDC_GROUPS_CLAIM=groups \
      AUTHENTICATION_OIDC_CLIENT_ID=demo \
      AUTHORIZATION_ADMINLIST_ENABLED=true \
      AUTHORIZATION_ADMINLIST_USERS=john@doe.com \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080
    ;;

  local-multi-text)
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      PERSISTENCE_DATA_PATH="./data" \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      TRANSFORMERS_INFERENCE_API=http://localhost:8000 \
      CLIP_INFERENCE_API=http://localhost:8005 \
      ENABLE_MODULES=text2vec-contextionary,text2vec-transformers,multi2vec-clip \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080
    ;;

  *) 
    echo "Invalid config" 2>&1
    exit 1
    ;;
esac

