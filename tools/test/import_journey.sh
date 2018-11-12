#!/usr/bin/env bash
set -euo pipefail

# This scripts runs the import journey that is included in our development setup
# The point is to fail on CI, not only if weaviate itself cannot be built,
# but also if test data cannot be importet. It therefore tests the journey
# of the weaviate developer.

docker-compose -f docker-compose.yml build
docker-compose -f docker-compose.yml up -d

# wait for 5 minutes
MAX_WAIT_SECONDS=300

END_TIME=$(( SECONDS + MAX_WAIT_SECONDS ))
IMPORTER_EXIT_CODE=

echo -n "Waiting for the importer container to finish..."
while [ $SECONDS -lt $END_TIME ]; do
  IMPORTER_CONTAINER_ID=$(docker ps -q --filter=name=weaviate_demo_importer)
  IMPORTER_EXIT_CODE=$(
    docker container wait "$IMPORTER_CONTAINER_ID"
  )

  if [ "$IMPORTER_EXIT_CODE" -eq "0" ]; then
    echo 
    echo "Success!";
    exit 0;
  fi

  echo -n "."
done

echo
echo -------
echo FAILURE
echo -------
echo "The importer never succeeded in ${MAX_WAIT_SECONDS}s."
echo "The last exit code was $IMPORTER_EXIT_CODE."
echo "Here are the last 30 lines of logs from the importer:"
docker logs $IMPORTER_CONTAINER_ID --tail 30

exit $IMPORTER_EXIT_CODE
