#!/bin/bash

CONFIG=${1:-local-development}

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

export GO111MODULE=on
export LOG_LEVEL=${LOG_LEVEL:-"debug"}
export LOG_FORMAT=${LOG_FORMAT:-"text"}
export PROMETHEUS_MONITORING_ENABLED=${PROMETHEUS_MONITORING_ENABLED:-"true"}
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
  go run -ldflags "-X github.com/weaviate/weaviate/usecases/config.GitHash=$GIT_HASH" "$@"
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
      GRPC_PORT=50052 \
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="./data-node2" \
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
        GRPC_PORT=50053 \
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
        GRPC_PORT=50054 \
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
  local-bind)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=multi2vec-bind \
      BIND_INFERENCE_API="http://localhost:8011" \
      ENABLE_MODULES="multi2vec-bind" \
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

  local-apikey)
      AUTHENTICATION_APIKEY_ENABLED=true \
      AUTHENTICATION_APIKEY_ALLOWED_KEYS=my-secret-key \
      AUTHENTICATION_APIKEY_USERS=john@doe.com \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=false \
      AUTHORIZATION_ADMINLIST_ENABLED=true \
      AUTHORIZATION_ADMINLIST_USERS=john@doe.com \
      DEFAULT_VECTORIZER_MODULE=none \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080
    ;;

  local-wcs-oidc-and-apikey)
      AUTHENTICATION_APIKEY_ENABLED=true \
      AUTHENTICATION_APIKEY_ALLOWED_KEYS=my-secret-key,my-secret-read-only-key \
      AUTHENTICATION_APIKEY_USERS=etienne@semi.technology,etienne+read-only@semi.technology \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=false \
      AUTHENTICATION_OIDC_ENABLED=true \
      AUTHENTICATION_OIDC_ISSUER=https://auth.wcs.api.weaviate.io/auth/realms/SeMI\
      AUTHENTICATION_OIDC_USERNAME_CLAIM=email \
      AUTHENTICATION_OIDC_GROUPS_CLAIM=groups \
      AUTHENTICATION_OIDC_CLIENT_ID=wcs \
      AUTHORIZATION_ADMINLIST_ENABLED=true \
      AUTHORIZATION_ADMINLIST_USERS=etienne@semi.technology \
      AUTHORIZATION_ADMINLIST_READONLY_USERS=etienne+read-only@semi.technology \
      DEFAULT_VECTORIZER_MODULE=none \
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

  local-qna-openai)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-contextionary,qna-openai" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-generative-openai)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-contextionary,generative-openai" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-all-openai)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-contextionary,qna-openai,generative-openai,text2vec-openai" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-all-palm)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-contextionary,generative-palm,text2vec-palm" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-all-openai-cohere-palm)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="node1" \
      ENABLE_MODULES="text2vec-contextionary,generative-palm,text2vec-palm,qna-openai,generative-openai,text2vec-openai,generative-cohere,text2vec-cohere,reranker-cohere" \
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
      BACKUP_S3_USE_SSL="false" \
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

    local-gcs-2)
        PERSISTENCE_DATA_PATH="./data-node2" \
        CONTEXTIONARY_URL=localhost:9999 \
        AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        GOOGLE_CLOUD_PROJECT=project-id \
        STORAGE_EMULATOR_HOST=localhost:9090 \
        BACKUP_GCS_ENDPOINT=localhost:9090 \
        BACKUP_GCS_BUCKET=weaviate-backups \
        ENABLE_MODULES="text2vec-contextionary,backup-gcs" \
        CLUSTER_HOSTNAME="node2" \
        CLUSTER_GOSSIP_BIND_PORT="7102" \
        CLUSTER_DATA_BIND_PORT="7103" \
        CLUSTER_JOIN="localhost:7100" \
        GRPC_PORT=50052 \
        go_run ./cmd/weaviate-server \
          --scheme http \
          --host "127.0.0.1" \
          --port 8081 \
          --read-timeout=600s \
          --write-timeout=600s
        ;;

    local-gcs-3)
        PERSISTENCE_DATA_PATH="./data-node3" \
        CONTEXTIONARY_URL=localhost:9999 \
        AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        GOOGLE_CLOUD_PROJECT=project-id \
        STORAGE_EMULATOR_HOST=localhost:9090 \
        BACKUP_GCS_ENDPOINT=localhost:9090 \
        BACKUP_GCS_BUCKET=weaviate-backups \
        ENABLE_MODULES="text2vec-contextionary,backup-gcs" \
        CLUSTER_HOSTNAME="node3" \
        CLUSTER_GOSSIP_BIND_PORT="7104" \
        CLUSTER_DATA_BIND_PORT="7105" \
        CLUSTER_JOIN="localhost:7100" \
        GRPC_PORT=50053 \
        go_run ./cmd/weaviate-server \
          --scheme http \
          --host "127.0.0.1" \
          --port 8082 \
          --read-timeout=600s \
          --write-timeout=600s
        ;;

  local-azure)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      BACKUP_AZURE_CONTAINER=weaviate-container \
      AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;" \
      ENABLE_MODULES="text2vec-contextionary,backup-azure" \
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

  local-all-cohere)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-cohere \
      ENABLE_MODULES="text2vec-cohere,reranker-cohere,generative-cohere" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-reranker-transformers)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      RERANKER_INFERENCE_API="http://localhost:8009" \
      ENABLE_MODULES="text2vec-contextionary,reranker-transformers" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-gpt4all)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-gpt4all \
      GPT4ALL_INFERENCE_API="http://localhost:8010" \
      ENABLE_MODULES="text2vec-gpt4all" \
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
