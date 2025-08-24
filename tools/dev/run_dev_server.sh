#!/usr/bin/env bash

CONFIG=${1:-local-development}

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

export GO111MODULE=on
export LOG_LEVEL=${LOG_LEVEL:-"debug"}
export LOG_FORMAT=${LOG_FORMAT:-"text"}
export PROMETHEUS_MONITORING_ENABLED=${PROMETHEUS_MONITORING_ENABLED:-"true"}
export PROMETHEUS_MONITORING_PORT=${PROMETHEUS_MONITORING_PORT:-"2112"}
export GO_BLOCK_PROFILE_RATE=${GO_BLOCK_PROFILE_RATE:-"20"}
export GO_MUTEX_PROFILE_FRACTION=${GO_MUTEX_PROFILE_FRACTION:-"20"}
export PERSISTENCE_DATA_PATH=${PERSISTENCE_DATA_PATH:-"./data"}
export ORIGIN=${ORIGIN:-"http://localhost:8080"}
export QUERY_DEFAULTS_LIMIT=${QUERY_DEFAULTS_LIMIT:-"20"}
export QUERY_MAXIMUM_RESULTS=${QUERY_MAXIMUM_RESULTS:-"10000"}
export TRACK_VECTOR_DIMENSIONS=true
export CLUSTER_HOSTNAME=${CLUSTER_HOSTNAME:-"weaviate-0"}
export GPT4ALL_INFERENCE_API="http://localhost:8010"
export DISABLE_TELEMETRY=true # disable telemetry for local development

# inject build info into binaries.
GIT_REVISION=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

VPREFIX="github.com/weaviate/weaviate/usecases/build"

BUILD_TAGS="-X ${VPREFIX}.Branch=${GIT_BRANCH} -X ${VPREFIX}.Revision=${GIT_REVISION} -X ${VPREFIX}.BuildUser=$(whoami)@$(hostname) -X ${VPREFIX}.BuildDate=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"


function go_run() {
   go run -ldflags "${BUILD_TAGS}" "$@"
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

  local-single-node)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="./data-weaviate-0" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-0" \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
      PROMETHEUS_MONITORING_PORT="2112" \
      PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      RAFT_BOOTSTRAP_EXPECT=1 \
      RUNTIME_OVERRIDES_ENABLED=true \
      RUNTIME_OVERRIDES_PATH="${PWD}/tools/dev/config.runtime-overrides.yaml" \
      RUNTIME_OVERRIDES_LOAD_INTERVAL=30s \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-single-node-rbac)
    AUTHENTICATION_APIKEY_ENABLED=true \
    AUTHORIZATION_RBAC_ENABLED=true \
    AUTHENTICATION_APIKEY_ALLOWED_KEYS='jane-secret-key,ian-secret-key,jp-secret-key' \
    AUTHENTICATION_APIKEY_USERS='jane@doe.com,ian-smith,jp-hwang' \
    AUTHORIZATION_RBAC_ROOT_USERS='jp-hwang' \
    PERSISTENCE_DATA_PATH="./data-weaviate-0" \
    BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-0" \
    ENABLE_MODULES="backup-filesystem" \
    CLUSTER_IN_LOCALHOST=true \
    CLUSTER_GOSSIP_BIND_PORT="7100" \
    CLUSTER_DATA_BIND_PORT="7101" \
    RAFT_BOOTSTRAP_EXPECT=1 \
    go_run ./cmd/weaviate-server \
      --scheme http \
      --host "127.0.0.1" \
      --port 8080 \
      --read-timeout=600s \
      --write-timeout=600s
  ;;

  local-first-rbac)
    CONTEXTIONARY_URL=localhost:9999 \
    AUTHENTICATION_APIKEY_ENABLED=true \
    AUTHORIZATION_RBAC_ENABLED=true \
    AUTHENTICATION_APIKEY_ALLOWED_KEYS='jane-secret-key,ian-secret-key,jp-secret-key' \
    AUTHENTICATION_APIKEY_USERS='jane@doe.com,ian-smith,jp-hwang' \
    AUTHORIZATION_RBAC_ROOT_USERS='jp-hwang' \
    PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-weaviate-0" \
    BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-0" \
    DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
    ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
    PROMETHEUS_MONITORING_PORT="2112" \
    CLUSTER_IN_LOCALHOST=true \
    CLUSTER_GOSSIP_BIND_PORT="7100" \
    CLUSTER_DATA_BIND_PORT="7101" \
    RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
    RAFT_BOOTSTRAP_EXPECT=3 \
    go_run ./cmd/weaviate-server \
      --scheme http \
      --host "127.0.0.1" \
      --port 8080 \
      --read-timeout=600s \
      --write-timeout=600s
  ;;

  local-second-rbac)
    GRPC_PORT=50052 \
    CONTEXTIONARY_URL=localhost:9999 \
    AUTHENTICATION_APIKEY_ENABLED=true \
    AUTHORIZATION_RBAC_ENABLED=true \
    AUTHENTICATION_APIKEY_ALLOWED_KEYS='jane-secret-key,ian-secret-key,jp-secret-key' \
    AUTHENTICATION_APIKEY_USERS='jane@doe.com,ian-smith,jp-hwang' \
    AUTHORIZATION_RBAC_ROOT_USERS='jp-hwang' \
    PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-weaviate-1" \
    BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-1" \
    CLUSTER_HOSTNAME="weaviate-1" \
    CLUSTER_IN_LOCALHOST=true \
    CLUSTER_GOSSIP_BIND_PORT="7102" \
    CLUSTER_DATA_BIND_PORT="7103" \
    CLUSTER_JOIN="localhost:7100" \
    PROMETHEUS_MONITORING_PORT="2113" \
    RAFT_PORT="8302" \
    RAFT_INTERNAL_RPC_PORT="8303" \
    RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
    RAFT_BOOTSTRAP_EXPECT=3 \
    DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
    ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
    go_run ./cmd/weaviate-server \
      --scheme http \
      --host "127.0.0.1" \
      --port 8081 \
      --read-timeout=600s \
      --write-timeout=600s
  ;;

  local-third-rbac)
    GRPC_PORT=50053 \
    CONTEXTIONARY_URL=localhost:9999 \
    AUTHENTICATION_APIKEY_ENABLED=true \
    AUTHORIZATION_RBAC_ENABLED=true \
    AUTHENTICATION_APIKEY_ALLOWED_KEYS='jane-secret-key,ian-secret-key,jp-secret-key' \
    AUTHENTICATION_APIKEY_USERS='jane@doe.com,ian-smith,jp-hwang' \
    AUTHORIZATION_RBAC_ROOT_USERS='jp-hwang' \
    BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-2" \
    PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-weaviate-2" \
    CLUSTER_HOSTNAME="weaviate-2" \
    CLUSTER_IN_LOCALHOST=true \
    CLUSTER_GOSSIP_BIND_PORT="7104" \
    CLUSTER_DATA_BIND_PORT="7105" \
    CLUSTER_JOIN="localhost:7100" \
    PROMETHEUS_MONITORING_PORT="2114" \
    RAFT_PORT="8304" \
    RAFT_INTERNAL_RPC_PORT="8305" \
    RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
    RAFT_BOOTSTRAP_EXPECT=3 \
    DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
    ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
    go_run ./cmd/weaviate-server \
      --scheme http \
      --host "127.0.0.1" \
      --port 8082 \
      --read-timeout=600s \
      --write-timeout=600s
  ;;

  local-development)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-weaviate-0" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-0" \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
      PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      PROMETHEUS_MONITORING_ENABLED=true \
      PROMETHEUS_MONITORING_PORT="${PROMETHEUS_MONITORING_PORT}" \
      RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
      RAFT_BOOTSTRAP_EXPECT=3 \
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
      PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-weaviate-1" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-1" \
      CLUSTER_HOSTNAME="weaviate-1" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7102" \
      CLUSTER_DATA_BIND_PORT="7103" \
      CLUSTER_JOIN="localhost:7100" \
      PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
      PROMETHEUS_MONITORING_PORT="$((PROMETHEUS_MONITORING_PORT + 1))" \
      PROMETHEUS_MONITORING_ENABLED=true \
      RAFT_PORT="8302" \
      RAFT_INTERNAL_RPC_PORT="8303" \
      RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
      RAFT_BOOTSTRAP_EXPECT=3 \
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
        BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-2" \
        PERSISTENCE_DATA_PATH="${PERSISTENCE_DATA_PATH}-weaviate-2" \
        CLUSTER_HOSTNAME="weaviate-2" \
        CLUSTER_IN_LOCALHOST=true \
        CLUSTER_GOSSIP_BIND_PORT="7104" \
        CLUSTER_DATA_BIND_PORT="7105" \
        CLUSTER_JOIN="localhost:7100" \
        PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
        PROMETHEUS_MONITORING_PORT="$((PROMETHEUS_MONITORING_PORT + 2))" \
        PROMETHEUS_MONITORING_ENABLED=true \
        RAFT_PORT="8304" \
        RAFT_INTERNAL_RPC_PORT="8305" \
        RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
        RAFT_BOOTSTRAP_EXPECT=3 \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        ENABLE_MODULES="text2vec-contextionary,backup-filesystem" \
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
        PERSISTENCE_DATA_PATH="./data-weaviate-4" \
        CLUSTER_HOSTNAME="weaviate-4" \
        CLUSTER_IN_LOCALHOST=true \
        CLUSTER_GOSSIP_BIND_PORT="7106" \
        CLUSTER_DATA_BIND_PORT="7107" \
        CLUSTER_JOIN="localhost:7100" \
        PROMETHEUS_MONITORING_PORT="2115" \
        PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
        RAFT_PORT="8306" \
        RAFT_INTERNAL_RPC_PORT="8307" \
	RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
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
      RUNTIME_OVERRIDES_ENABLED=true \
      RUNTIME_OVERRIDES_PATH="${PWD}/tools/dev/config.runtime-overrides.yaml" \
      RUNTIME_OVERRIDES_LOAD_INTERVAL=30s \
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

  local-ollama)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-ollama \
      ENABLE_MODULES="text2vec-ollama" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
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
      CLUSTER_HOSTNAME="weaviate-0" \
      CLUSTER_IN_LOCALHOST=true \
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
      CLUSTER_HOSTNAME="weaviate-0" \
      CLUSTER_IN_LOCALHOST=true \
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
      CLUSTER_HOSTNAME="weaviate-0" \
      CLUSTER_IN_LOCALHOST=true \
      ENABLE_MODULES="text2vec-contextionary,qna-openai,generative-openai,text2vec-openai" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-all-google)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="weaviate-0" \
      CLUSTER_IN_LOCALHOST=true \
      ENABLE_MODULES="text2vec-contextionary,generative-google,text2vec-google" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-all-openai-cohere-google)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="weaviate-0" \
      CLUSTER_IN_LOCALHOST=true \
      ENABLE_MODULES="text2vec-contextionary,generative-google,text2vec-google,qna-openai,generative-openai,text2vec-openai,generative-cohere,text2vec-cohere,reranker-cohere" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-all-openai-voyageai-google)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      QNA_INFERENCE_API="http://localhost:8001" \
      CLUSTER_HOSTNAME="weaviate-0" \
      ENABLE_MODULES="text2vec-contextionary,generative-google,text2vec-google,qna-openai,generative-openai,text2vec-openai,text2vec-voyageai,reranker-voyageai,multi2vec-voyageai" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-huggingface)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      CLUSTER_IN_LOCALHOST=true \
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
      CLUSTER_IN_LOCALHOST=true \
      DEFAULT_VECTORIZER_MODULE=none \
      RUNTIME_OVERRIDES_ENABLED=true \
      RUNTIME_OVERRIDES_PATH="${PWD}/tools/dev/config.runtime-overrides.yaml" \
      RUNTIME_OVERRIDES_LOAD_INTERVAL=5s \
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
      CLUSTER_IN_LOCALHOST=true \
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
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-minio)
    docker run \
    -p 9000:9000 \
    -p 9001:9001 \
    --user $(id -u):$(id -g) \
    --name minio1 \
    -e "MINIO_ROOT_USER=aws_access_key" \
    -e "MINIO_ROOT_PASSWORD=aws_secret_key" \
    -v ${HOME}/minio/data:/data \
    quay.io/minio/minio server /data --console-address ":9001"
    ;;

  local-node-with-offload)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      PERSISTENCE_DATA_PATH="./data-weaviate-0" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-0" \
      ENABLE_MODULES="backup-s3,offload-s3" \
      BACKUP_S3_BUCKET="weaviate-backups" \
      BACKUP_S3_USE_SSL="false" \
      BACKUP_S3_ENDPOINT="localhost:9000" \
      ENABLE_MODULES="backup-filesystem,text2vec-contextionary,offload-s3" \
      PROMETHEUS_MONITORING_PORT="2112" \
      PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      RAFT_BOOTSTRAP_EXPECT=1 \
      OFFLOAD_S3_BUCKET_AUTO_CREATE=true \
      OFFLOAD_S3_ENDPOINT="http://localhost:9000"\
      OFFLOAD_S3_BUCKET_AUTO_CREATE=true \
      AWS_ACCESS_KEY_ID="aws_access_key"\
      AWS_SECRET_KEY="aws_secret_key"\
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

    local-single-offload-node)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="./data-weaviate-0" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-0" \
      ENABLE_MODULES="backup-s3,offload-s3" \
      BACKUP_S3_BUCKET="weaviate-backups" \
      BACKUP_S3_USE_SSL="false" \
      BACKUP_S3_ENDPOINT="localhost:9000" \
      PROMETHEUS_MONITORING_PORT="2112" \
      PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      RAFT_BOOTSTRAP_EXPECT=1 \
      OFFLOAD_S3_ENDPOINT="http://localhost:9000"\
      OFFLOAD_S3_BUCKET_AUTO_CREATE=true \
      AWS_ACCESS_KEY_ID="aws_access_key"\
      AWS_SECRET_KEY="aws_secret_key"\
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-offload-s3)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="./${PERSISTENCE_DATA_PATH}-weaviate-0" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-0" \
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary,backup-s3,offload-s3" \
      BACKUP_S3_BUCKET="weaviate-backups" \
      BACKUP_S3_USE_SSL="false" \
      BACKUP_S3_ENDPOINT="localhost:9000" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
      RAFT_BOOTSTRAP_EXPECT=3 \
      OFFLOAD_S3_BUCKET_AUTO_CREATE=true \
      OFFLOAD_S3_ENDPOINT="http://localhost:9000"\
      AWS_ACCESS_KEY_ID="aws_access_key"\
      AWS_SECRET_KEY="aws_secret_key"\
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  second-offload-s3)
      GRPC_PORT=50052 \
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      PERSISTENCE_DATA_PATH="./${PERSISTENCE_DATA_PATH}-weaviate-1" \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-1" \
      BACKUP_S3_BUCKET="weaviate-backups" \
      BACKUP_S3_USE_SSL="false" \
      BACKUP_S3_ENDPOINT="localhost:9000" \
      CLUSTER_HOSTNAME="weaviate-1" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7102" \
      CLUSTER_DATA_BIND_PORT="7103" \
      CLUSTER_JOIN="localhost:7100" \
      PROMETHEUS_MONITORING_PORT="2113" \
      PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
      RAFT_PORT="8302" \
      RAFT_INTERNAL_RPC_PORT="8303" \
      RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
      RAFT_BOOTSTRAP_EXPECT=3 \
      OFFLOAD_S3_BUCKET_AUTO_CREATE=true \
      OFFLOAD_S3_ENDPOINT="http://localhost:9000"\
      AWS_ACCESS_KEY_ID="aws_access_key"\
      AWS_SECRET_KEY="aws_secret_key"\
      DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
      ENABLE_MODULES="text2vec-contextionary,backup-s3,offload-s3" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8081 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  third-offload-s3)
        GRPC_PORT=50053 \
        CONTEXTIONARY_URL=localhost:9999 \
        AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
        PERSISTENCE_DATA_PATH="./${PERSISTENCE_DATA_PATH}-weaviate-2" \
        BACKUP_FILESYSTEM_PATH="${PWD}/backups-weaviate-2" \
        BACKUP_S3_BUCKET="weaviate-backups" \
        BACKUP_S3_USE_SSL="false" \
        BACKUP_S3_ENDPOINT="localhost:9000" \
        CLUSTER_HOSTNAME="weaviate-2" \
        CLUSTER_IN_LOCALHOST=true \
        CLUSTER_GOSSIP_BIND_PORT="7104" \
        CLUSTER_DATA_BIND_PORT="7105" \
        CLUSTER_JOIN="localhost:7100" \
        PROMETHEUS_MONITORING_PORT="2114" \
	PROMETHEUS_MONITORING_METRIC_NAMESPACE="weaviate" \
        RAFT_PORT="8304" \
        RAFT_INTERNAL_RPC_PORT="8305" \
        RAFT_JOIN="weaviate-0:8300,weaviate-1:8302,weaviate-2:8304" \
        RAFT_BOOTSTRAP_EXPECT=3 \
        OFFLOAD_S3_BUCKET_AUTO_CREATE=true \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        ENABLE_MODULES="text2vec-contextionary,backup-s3,offload-s3" \
        OFFLOAD_S3_ENDPOINT="http://localhost:9000"\
        AWS_ACCESS_KEY_ID="aws_access_key"\
        AWS_SECRET_KEY="aws_secret_key"\
        go_run ./cmd/weaviate-server \
          --scheme http \
          --host "127.0.0.1" \
          --port 8082 \
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
      CLUSTER_IN_LOCALHOST=true \
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
        PERSISTENCE_DATA_PATH="./data-weaviate-1" \
        CONTEXTIONARY_URL=localhost:9999 \
        AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        GOOGLE_CLOUD_PROJECT=project-id \
        STORAGE_EMULATOR_HOST=localhost:9090 \
        BACKUP_GCS_ENDPOINT=localhost:9090 \
        BACKUP_GCS_BUCKET=weaviate-backups \
        ENABLE_MODULES="text2vec-contextionary,backup-gcs" \
        CLUSTER_IN_LOCALHOST=true \
        CLUSTER_HOSTNAME="weaviate-1" \
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
        PERSISTENCE_DATA_PATH="./data-weaviate-2" \
        CONTEXTIONARY_URL=localhost:9999 \
        AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
        DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
        GOOGLE_CLOUD_PROJECT=project-id \
        STORAGE_EMULATOR_HOST=localhost:9090 \
        BACKUP_GCS_ENDPOINT=localhost:9090 \
        BACKUP_GCS_BUCKET=weaviate-backups \
        ENABLE_MODULES="text2vec-contextionary,backup-gcs" \
        CLUSTER_IN_LOCALHOST=true \
        CLUSTER_HOSTNAME="weaviate-2" \
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
      CLUSTER_IN_LOCALHOST=true \
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
      CLUSTER_IN_LOCALHOST=true \
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
      CLUSTER_IN_LOCALHOST=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-cohere \
      ENABLE_MODULES="text2vec-cohere,reranker-cohere,generative-cohere" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-voyageai)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-voyageai \
      ENABLE_MODULES="text2vec-voyageai" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;

  local-all-voyageai)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-voyageai \
      ENABLE_MODULES="text2vec-voyageai,reranker-voyageai" \
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
      CLUSTER_IN_LOCALHOST=true \
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
      CLUSTER_IN_LOCALHOST=true \
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
  local-bigram)
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-bigram \
      BACKUP_FILESYSTEM_PATH="${PWD}/backups" \
      ENABLE_MODULES="text2vec-bigram,backup-filesystem" \
      CLUSTER_IN_LOCALHOST=true \
      CLUSTER_GOSSIP_BIND_PORT="7100" \
      CLUSTER_DATA_BIND_PORT="7101" \
      RAFT_BOOTSTRAP_EXPECT=1 \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-model2vec)
      CONTEXTIONARY_URL=localhost:9999 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      DEFAULT_VECTORIZER_MODULE=text2vec-model2vec \
      MODEL2VEC_INFERENCE_API="http://localhost:8012" \
      ENABLE_MODULES="text2vec-model2vec" \
      go_run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080 \
        --read-timeout=600s \
        --write-timeout=600s
    ;;
  local-prometheus)
    echo "Starting monitoring setup..."

    cleanup() {
      echo "Cleaning up existing containers and volumes..."
      docker stop prometheus grafana 2>/dev/null || true
      docker rm -f prometheus grafana 2>/dev/null || true
      docker volume rm grafana_data 2>/dev/null || true
      docker network rm monitoring 2>/dev/null || true
      echo "Cleanup complete."
    }

    cleanup

    echo "Creating new setup..."

    docker network create monitoring 2>/dev/null || true


    echo "Starting Prometheus..."
    docker run -d \
      --name prometheus \
      --network monitoring \
      -p 9090:9090 \
      --add-host=host.docker.internal:host-gateway \
      -v "$(pwd)/tools/dev/prometheus_config/prometheus.yml:/etc/prometheus/prometheus.yml" \
      prom/prometheus


    echo "Starting Grafana..."
    docker run -d \
      --name grafana \
      --network monitoring \
      -p 3000:3000 \
      -e "GF_SECURITY_ADMIN_PASSWORD=admin" \
      -e "GF_LOG_LEVEL=debug" \
      -v "$(pwd)/tools/dev/grafana/datasources:/etc/grafana/provisioning/datasources" \
      -v "$(pwd)/tools/dev/grafana/dashboards/dashboard.yml:/etc/grafana/provisioning/dashboards/dashboard.yml" \
      -v "$(pwd)/tools/dev/grafana/dashboards:/etc/grafana/dashboards" \
      grafana/grafana

    echo "Waiting for services to start..."
    sleep 5

    if docker ps | grep -q prometheus; then
      echo "Prometheus is running"
      echo "Checking Prometheus targets..."
      curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job:.job, state:.health}'
    else
      echo "Error: Prometheus failed to start"
      docker logs prometheus
    fi

    if docker ps | grep -q grafana; then
      echo "Grafana is running"
      echo "Checking Grafana logs..."
      docker logs grafana
    else
      echo "Error: Grafana failed to start"
      docker logs grafana
    fi

    echo "Setup complete! Services are available at:"
    echo "Prometheus: http://localhost:9090"
    echo "Grafana: http://localhost:3000 (admin/admin)"
    echo "Dashboards should be available at:"
    echo "- Overview: http://localhost:3000/d/weaviate-overview/weaviate-overview"
    ;;
  *)
    echo "Invalid config" 2>&1
    exit 1
    ;;
esac
