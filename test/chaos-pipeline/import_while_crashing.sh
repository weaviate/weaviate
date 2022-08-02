#!/bin/bash

set -e

SIZE=600000

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
( cd apps/importer/ && docker build -t importer . )
( cd apps/chaotic-killer/ && docker build -t killer . )

echo "Starting Weaviate..."
docker-compose -f apps/weaviate/docker-compose.yml up -d

wait_weaviate

echo "Starting the chaos script to kill Weaviate periodically (in the background)"
docker run \
  --network host \
  --rm -it -d \
  -v "$PWD:$PWD" \
  -w "$PWD" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --name killer \
  killer

echo "Run import script in foreground..."
if ! docker run \
  -e 'DIMENSIONS=48' \
  -e 'SHARDS=1' \
  -e "SIZE=$SIZE" \
  -e 'BATCH_SIZE=128' \
  -e 'ORIGIN=http://localhost:8080' \
  --network host \
  -it importer; then
  echo "Importer failed, printing latest Weaviate logs..."
  docker-compose -f apps/weaviate/docker-compose.yml logs weaviate
  exit 1
fi

echo "Import completed successfully, stop killer"
docker rm -f killer

echo "Wait for Weaviate to be ready again in case there was a kill recently"
wait_weaviate

echo "Validate the count is correct"
object_count=$(curl -s 'localhost:8080/v1/graphql' -X POST \
  -H 'content-type: application/json' \
  -d '{"query":"{Aggregate{DemoClass{meta{count}}}}"}' | \
  jq '.data.Aggregate.DemoClass[0].meta.count')

if [ "$object_count" -lt "$SIZE" ]; then
  echo "Not enough objects present, wanted $SIZE, got $object_count"
  exit 1
fi

echo "Passed!"
