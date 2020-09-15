#!/usr/bin/env bash

CONFIG=${1:-local-development}

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

export GO111MODULE=on
export DEVELOPMENT_UI=on
export LOG_LEVEL=debug
export LOG_FORMAT=text

case $CONFIG in
  local-development)
    CONFIGURATION_STORAGE_TYPE=etcd \
      CONFIGURATION_STORAGE_URL=http://localhost:2379 \
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
      ESVECTOR_URL=http://localhost:9201 \
      go run ./cmd/weaviate-server \
        --scheme http \
        --host "127.0.0.1" \
        --port 8080
    ;;
  local-oidc)
    CONFIGURATION_STORAGE_TYPE=etcd \
      CONFIGURATION_STORAGE_URL=http://localhost:2379 \
      CONTEXTIONARY_URL=localhost:9999 \
      QUERY_DEFAULTS_LIMIT=20 \
      ORIGIN=http://localhost:8080 \
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=false \
      AUTHENTICATION_OIDC_ENABLED=true \
      AUTHENTICATION_OIDC_ISSUER=http://localhost:9090/auth/realms/weaviate \
      AUTHENTICATION_OIDC_USERNAME_CLAIM=email \
      AUTHENTICATION_OIDC_GROUPS_CLAIM=groups \
      AUTHENTICATION_OIDC_CLIENT_ID=demo \
      AUTHORIZATION_ADMINLIST_ENABLED=true \
      AUTHORIZATION_ADMINLIST_USERS=john@doe.com \
      ESVECTOR_URL=http://localhost:9201 \
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

