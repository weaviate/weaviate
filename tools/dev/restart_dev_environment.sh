#!/usr/bin/env bash

set -e

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

DOCKER_COMPOSE_FILE=
ADDITIONAL_SERVICES=()
if [[ "$*" == *--spark* ]]; then
  DOCKER_COMPOSE_FILE=docker-compose-spark.yml
  ADDITIONAL_SERVICES+=('analytics-api')
  ADDITIONAL_SERVICES+=('spark-master')
  ADDITIONAL_SERVICES+=('spark-worker')
else 
  DOCKER_COMPOSE_FILE=docker-compose.yml
fi

docker-compose -f $DOCKER_COMPOSE_FILE down --remove-orphans

rm -rf data connector_state.json schema_state.json

docker-compose -f $DOCKER_COMPOSE_FILE up -d index janus db etcd genesis_fake weaviate_b_fake "${ADDITIONAL_SERVICES[@]}"

