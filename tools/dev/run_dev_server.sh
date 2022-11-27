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
export TRACK_VECTOR_DIMENSIONS=true
export CLUSTER_HOSTNAME=${CLUSTER_HOSTNAME:-"node1"}

function go_run() {
  GIT_HASH=$(git rev-parse --short HEAD)
  go run -ldflags "-X github.com/semi-technologies/weaviate/usecases/config.GitHash=$GIT_HASH" $@
}

case $CONFIG in
  debug)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
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
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups" \
      ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  second-node)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-node2" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-node2" \
      CLUSTER_HOSTNAME="node2" \
      CLUSTER_GOSSIP_BIND_PORT="7102" \
      CLUSTER_DATA_BIND_PORT="7103" \
      CLUSTER_JOIN="localhost:7100" \
      CONTEXTIONARY_URL=localhost:9999 \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8081 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

    third-node)
        CONTEXTIONARY_URL=localhost:9999 \
        AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
        PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-node3" \
        CLUSTER_HOSTNAME="node3" \
        CLUSTER_GOSSIP_BIND_PORT="7104" \
        CLUSTER_DATA_BIND_PORT="7105" \
        CLUSTER_JOIN="localhost:7100" \
        CONTEXTIONARY_URL=localhost:9999 \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        ENABLE_MODULES="text2vec-contextionary" \
        go_run ./cmd/weaviate-server \
          --scheme http \
          --host "127.0.0.1" \
          --port 8082 \
          --read-timeout=600s \
          --write-timeout=600s
      ;;

    fourth-node)
        CONTEXTIONARY_URL=localhost:9999 \
        AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
        PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-node4" \
        CLUSTER_HOSTNAME="node4" \
        CLUSTER_GOSSIP_BIND_PORT="7106" \
        CLUSTER_DATA_BIND_PORT="7107" \
        CLUSTER_JOIN="localhost:7100" \
        CONTEXTIONARY_URL=localhost:9999 \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        ENABLE_MODULES="text2vec-contextionary" \
        go_run ./cmd/weaviate-server \
          --scheme http \
          --host "127.0.0.1" \
          --port 8083 \
          --read-timeout=600s \
          --write-timeout=600s
      ;;

  local-transformers)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-transformers \
      TRANSFORMERS_INFERENCE_API="http://localhost:8000" \
      ENABLE_MODULES="text2vec-transformers" \
      go_run ./cmd/weaviate-server \
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
      ENABLE_MODULES="text2vec-transformers" \
      go_run ./cmd/weaviate-server \
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
      ENABLE_MODULES="text2vec-contextionary,qna-transformers" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-sum)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      SUM_INFERENCE_API="http://localhost:8008" \
      ENABLE_MODULES="text2vec-contextionary,sum-transformers" \
      go_run ./cmd/weaviate-server \
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
      ENABLE_MODULES="text2vec-contextionary,img2vec-neural" \
      go_run ./cmd/weaviate-server \
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
      go_run ./cmd/weaviate-server \
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
      go_run ./cmd/weaviate-server \
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
      go_run ./cmd/weaviate-server \
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
      go_run ./cmd/weaviate-server \
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
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080
    ;;

  local-openai)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-openai \
      ENABLE_MODULES="text2vec-openai" \
      go_run ./cmd/weaviate-server \
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
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-no-modules)
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=none \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=3600s \
        --write-timeout=3600s
    ;;

  local-centroid)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      ENABLE_MODULES="ref2vec-centroid" \
      go_run ./cmd/weaviate-server \
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
      BACKUP_S3_ENDPOINT="localhost:9000" \
      BACKUP_S3_BUCKET="weaviate-backups" \
      AWS_ACCESS_KEY_ID="aws_access_key" \
      AWS_SECRET_KEY="aws_secret_key" \
      ENABLE_MODULES="text2vec-contextionary,backup-s3" \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      go_run ./cmd/weaviate-server \
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
      BACKUP_GCS_ENDPOINT=localhost:9090 \
      BACKUP_GCS_BUCKET=weaviate-backups \
      ENABLE_MODULES="text2vec-contextionary,backup-gcs" \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
      ;;

  local-cohere)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-cohere \
      ENABLE_MODULES="text2vec-cohere" \
      go_run ./cmd/weaviate-server \
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
