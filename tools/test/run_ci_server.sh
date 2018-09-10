#!/usr/bin/env bash
set -euo pipefail

# This scripts starts a Weaviate server with the test scheme and waits until weaviate is up and running.

docker-compose -f docker-compose-test.yml build weaviate janus index db
docker-compose -f docker-compose-test.yml up --force-recreate -d weaviate janus index db

MAX_WAIT_SECONDS=60
ALREADY_WAITING=0

while true; do
  if curl -s http://localhost:8080 > /dev/null; then
    break
  else
    if [ $? -eq 7 ]; then
      echo "Weaviate is not up yet. (waited for ${ALREADY_WAITING}s)"
      if [ $ALREADY_WAITING -gt $MAX_WAIT_SECONDS ]; then
        echo "Weaviate did not start up in $MAX_WAIT_SECONDS."
        docker-compose -f docker-compose-test.yml logs
        exit 1
      else
        sleep 2
        let ALREADY_WAITING=$ALREADY_WAITING+2
      fi
    fi
  fi
done

echo "Weaviate is up and running!"
