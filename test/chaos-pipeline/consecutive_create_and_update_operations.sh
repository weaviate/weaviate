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
( cd apps/consecutive_create_and_update_operations/ && docker build -t consecutive_create_and_update_operations . )

echo "Starting Weaviate..."
docker-compose -f apps/weaviate/docker-compose.yml up -d

wait_weaviate

echo "Run consecutive create and update operations"
docker run --network host -it consecutive_create_and_update_operations python3 consecutive_create_and_update_operations.py

echo "Passed!"
