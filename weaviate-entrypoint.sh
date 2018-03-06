#!/bin/bash

# Defination of a env variables with default values
WEAVIATE_SCHEME=${WEAVIATE_SCHEME:-http}
WEAVIATE_PORT=${WEAVIATE_PORT:-80}
WEAVIATE_HOST=${WEAVIATE_HOST:-0.0.0.0}
WEAVIATE_CONFIG=${WEAVIATE_CONFIG:-cassandra}
WEAVIATE_CONFIG_FILE=${WEAVIATE_CONFIG_FILE:-./weaviate.conf.json}
CQLVERSION=${CQLVERSION:-3.4.4}

# Loop for checking connection to cassandra DB
if [ "$WEAVIATE_CONFIG" == "cassandra" ]; then
  counter=1
  until cqlsh --cqlversion=$CQLVERSION "$WEAVIATE_CASSANDRA_DB_HOST" -e exit; do
    >&2 echo "Cassandra is unavailable - sleeping"
    sleep 3
    ((counter++))
    if [ $counter -gt 10 ]; then
      echo "cassandra is not available trying to start without cassandra"
      break 
    fi
  done
else
  echo "DB is not cassandra"
fi

# Starting main process of container
exec ./weaviate --scheme=${WEAVIATE_SCHEME} --port=${WEAVIATE_PORT} --host=${WEAVIATE_HOST} --config=${WEAVIATE_CONFIG} --config-file=${WEAVIATE_CONFIG_FILE}
