#!/bin/bash

set -e

function wait_weaviate() {
  echo "Wait for Weaviate to be ready"
  for _ in {1..120}; do
    if curl -sf -o /dev/null localhost:8080; then
      echo "Weaviate is ready"
      break
    fi

    echo "Weaviate is not ready, trying again in 1s"
    sleep 1
  done
}

echo "Building all required containers"
( cd apps/rest-patch-stops-working-after-restart/ && docker build -t rest-patch-stops-working-after-restart . )

echo "Starting Weaviate..."
docker-compose -f apps/weaviate/docker-compose.yml up -d

wait_weaviate

echo "Run consecutive update operations"
docker run --network host -it rest-patch-stops-working-after-restart python3 rest-patch-stops-working-after-restart.py

echo "Restart Weaviate..."
docker-compose -f apps/weaviate/docker-compose.yml stop
docker-compose -f apps/weaviate/docker-compose.yml up -d

wait_weaviate

echo "Run consecutive update operations after restart"
docker run --network host -it rest-patch-stops-working-after-restart python3 rest-patch-stops-working-after-restart.py

echo "Passed!"
