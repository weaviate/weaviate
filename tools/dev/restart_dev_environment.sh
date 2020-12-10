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
  ADDITIONAL_SERVICES+=('janus')
  ADDITIONAL_SERVICES+=('db')
  ADDITIONAL_SERVICES+=('index')
elif [[ "$*" == *--keycloak* ]]; then
  DOCKER_COMPOSE_FILE=docker-compose.yml
  ADDITIONAL_SERVICES+=('keycloak')
elif [[ "$*" == *--esvector-only* ]]; then
  DOCKER_COMPOSE_FILE=docker-compose-esonly.yml
elif [[ "$*" == *--customdb* ]]; then
  DOCKER_COMPOSE_FILE=docker-compose-customdb.yml
else
  DOCKER_COMPOSE_FILE=docker-compose.yml
fi

docker-compose -f $DOCKER_COMPOSE_FILE down --remove-orphans

rm -rf data connector_state.json schema_state.json

if [[ "$*" == *--customdb* ]]; then
  docker-compose -f $DOCKER_COMPOSE_FILE up -d contextionary "${ADDITIONAL_SERVICES[@]}"
else
  docker-compose -f $DOCKER_COMPOSE_FILE up -d etcd contextionary esvector kibana "${ADDITIONAL_SERVICES[@]}"
fi

if [[ "$*" == *--keycloak* ]]; then
  echo "Since you have specified the --keycloak option, we must now wait for"
  echo "keycloak to spin up, so that we can add some demo users"

  echo -n "Waiting "
  until curl --fail -s localhost:9090/auth; do
    echo -n .
    sleep 1
  done
  echo
  echo Keycloak is ready, now importing some users.
  ./tools/dev/keycloak/import_users.sh
fi

echo "You can now run the dev version with: ./tools/dev/run_dev_server.sh or ./tools/dev/run_dev_server_no_network.sh"
