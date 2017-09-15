#!/bin/bash

# Start the first process
nohup dgraph --memory_mb 2048
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start dgraph: $status"
  exit $status
fi

# Start the second process
nohup /var/weaviate/weaviate --scheme=http --port=8080 --host=localhost --config=graph --config-file=weaviate.conf.json
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start weaviate: $status"
  exit $status
fi
  
while /bin/true; do
  ps aux |grep dgraph |grep -q -v grep
  PROCESS_1_STATUS=$?
  ps aux |grep weaviate |grep -q -v grep
  PROCESS_2_STATUS=$?
  # If the greps above find anything, they will exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 ]; then
    echo "One of the processes has already exited."
    exit -1
  fi
  sleep 60
done