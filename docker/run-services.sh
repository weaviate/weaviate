#!/bin/bash

# check if jq is installed
jq --help >/dev/null 2>&1 || { echo >&2 "I require jq but it's not installed. Aborting."; exit 1; }

# remove dgraph and weaviate
docker rm dgraph || true
docker rm weaviate || true

# build and start dgraph docker
mkdir -p ~/dgraph
docker run -it -p 8080:8080 -p 9080:9080 -v ~/dgraph:/dgraph --name dgraph dgraph/dgraph dgraph --bindall=true --memory_mb 2048
DGRAPHIP=$(docker inspect dgraph  | jq '.[0].NetworkSettings.IPAddress')

# build and start weaviate docker
docker build --build-arg DGRAPHIP=$DGRAPHIP -t weaviate https://raw.githubusercontent.com/weaviate/weaviate/develop/docker/Dockerfile
docker run weaviate