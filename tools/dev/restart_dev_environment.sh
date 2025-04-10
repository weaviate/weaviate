#!/usr/bin/env bash

set -e

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

DOCKER_COMPOSE_FILE=docker-compose.yml
ADDITIONAL_SERVICES=()
if [[ "$*" == *--keycloak* ]]; then
  ADDITIONAL_SERVICES+=('keycloak')
fi
if [[ "$*" == *--transformers-passage-query* ]]; then
  ADDITIONAL_SERVICES+=('t2v-transformers-passage')
  ADDITIONAL_SERVICES+=('t2v-transformers-query')
elif [[ "$*" == *--transformers* ]]; then
  ADDITIONAL_SERVICES+=('t2v-transformers')
else
  ADDITIONAL_SERVICES+=('contextionary')
fi
if [[ "$*" == *--contextionary* ]]; then
  ADDITIONAL_SERVICES+=('contextionary')
fi
if [[ "$*" == *--qna* ]]; then
  ADDITIONAL_SERVICES+=('qna-transformers')
fi
if [[ "$*" == *--sum* ]]; then
  ADDITIONAL_SERVICES+=('sum-transformers')
fi
if [[ "$*" == *--image* ]]; then
  ADDITIONAL_SERVICES+=('i2v-neural')
fi
if [[ "$*" == *--ner* ]]; then
  ADDITIONAL_SERVICES+=('ner-transformers')
fi
if [[ "$*" == *--spellcheck* ]]; then
  ADDITIONAL_SERVICES+=('text-spellcheck')
fi
if [[ "$*" == *--clip* ]]; then
  ADDITIONAL_SERVICES+=('multi2vec-clip')
fi
if [[ "$*" == *--bind* ]]; then
  ADDITIONAL_SERVICES+=('multi2vec-bind')
fi
if [[ "$*" == *--reranker* ]]; then
  ADDITIONAL_SERVICES+=('reranker-transformers')
fi
if [[ "$*" == *--gpt4all* ]]; then
  ADDITIONAL_SERVICES+=('t2v-gpt4all')
fi
if [[ "$*" == *--prometheus* ]]; then
  ADDITIONAL_SERVICES+=('prometheus')
  ADDITIONAL_SERVICES+=('grafana')
fi
if [[ "$*" == *--s3* ]]; then
  ADDITIONAL_SERVICES+=('backup-s3')
fi
if [[ "$*" == *--gcs* ]]; then
  ADDITIONAL_SERVICES+=('backup-gcs')
fi
if [[ "$*" == *--azure* ]]; then
  ADDITIONAL_SERVICES+=('backup-azure')
fi
if [[ "$*" == *--voyagemulti* ]]; then
  ADDITIONAL_SERVICES+=('multi2vec-voyageai')
fi
if [[ "$*" == *--ollama ]]; then
  ADDITIONAL_SERVICES+=('ollama')
fi
if [[ "$*" == *--model2vec* ]]; then
  ADDITIONAL_SERVICES+=('text2vec-model2vec')
fi

docker compose -f $DOCKER_COMPOSE_FILE down --remove-orphans

echo "Deleting local persistent state. All the collections and tenants created will be removed"
rm -rf data data-weaviate-0 data-weaviate-1 data-weaviate-2  backups-weaviate-0 backups-weaviate-1 backups-weaviate-2 connector_state.json schema_state.json

docker compose -f $DOCKER_COMPOSE_FILE up -d "${ADDITIONAL_SERVICES[@]}"

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

if [[ "$*" == *--transformers-passage-query* ]]; then
  echo "You have specified the --transformers-passage-query option. Starting up"
  echo "the t2v-transformers-passage and t2v-transformers-query model containers"
elif [[ "$*" == *--transformers* ]]; then
  echo "You have specified the --transformers option. Starting up"
  echo "the text2vec-transformers model container"
fi

if [[ "$*" == *--image* ]]; then
  echo "You have specified the --image option. Starting up"
  echo "the text2vec-contextionary model container with img2vec-image module"
fi

if [[ "$*" == *--s3* ]]; then
  echo "You have specified the --s3 option. Starting up"
  echo "the text2vec-contextionary model container with backup-s3 module"
fi

if [[ "$*" == *--gcs* ]]; then
  echo "You have specified the --gcs option. Starting up"
  echo "the text2vec-contextionary model container with backup-gcs module"
fi

if [[ "$*" == *--azure* ]]; then
  echo "You have specified the --azure option. Starting up"
  echo "the text2vec-contextionary model container with backup-azure module"
fi

echo "You can now run the dev version with: ./tools/dev/run_dev_server.sh or ./tools/dev/run_dev_server_no_network.sh"
