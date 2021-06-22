#!/usr/bin/env bash

set -e

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

DOCKER_COMPOSE_FILE=docker-compose.yml
ADDITIONAL_SERVICES=()
if [[ "$*" == *--keycloak* ]]; then
  ADDITIONAL_SERVICES+=('keycloak')
fi
if [[ "$*" == *--transformers* ]]; then
  ADDITIONAL_SERVICES+=('t2v-transformers')
else 
  ADDITIONAL_SERVICES+=('contextionary')
fi
if [[ "$*" == *--qna* ]]; then
  ADDITIONAL_SERVICES+=('qna-transformers')
fi
if [[ "$*" == *--image* ]]; then
  ADDITIONAL_SERVICES+=('i2v-neural')
fi

docker-compose -f $DOCKER_COMPOSE_FILE down --remove-orphans

rm -rf data connector_state.json schema_state.json

docker-compose -f $DOCKER_COMPOSE_FILE up -d "${ADDITIONAL_SERVICES[@]}"

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

if [[ "$*" == *--transformers* ]]; then
  echo "You have specified the --transformers option. Starting up"
  echo "the text2vec-transformers model container"
fi

if [[ "$*" == *--image* ]]; then
  echo "You have specified the --image option. Starting up"
  echo "the text2vec-contextionary model container with img2vec-image module"
fi

echo "You can now run the dev version with: ./tools/dev/run_dev_server.sh or ./tools/dev/run_dev_server_no_network.sh"
