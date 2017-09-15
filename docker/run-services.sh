#!/bin/bash
#                          _       _
#__      _____  __ ___   ___  __ _| |_ ___
#\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
# \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
#  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
#
# Copyright © 2016 Weaviate. All rights reserved.
# LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
# AUTHOR: Bob van Luijt (bob@weaviate.com)
# See www.weaviate.com for details
# Contact: @weaviate_iot / yourfriends@weaviate.com
#

# check if jq is installed
jq --help >/dev/null 2>&1 || { echo >&2 "I require jq but it's not installed. Aborting."; exit 1; }

# remove dgraph and weaviate
docker rm dgraph || true
docker rm weaviate || true

# build and start dgraph docker
mkdir -p ~/dgraph
docker run -it -p 8080:8080 -p 9080:9080 -v ~/dgraph:/dgraph --name dgraph dgraph/dgraph dgraph --bindall=true --memory_mb 2048
DGRAPHIP=$(docker inspect dgraph  | jq -r '.[0].NetworkSettings.IPAddress')

# build and start weaviate docker
docker build --build-arg DGRAPHIP=$DGRAPHIP -t weaviate https://raw.githubusercontent.com/weaviate/weaviate/develop/docker/Dockerfile?i=$(echo $((1 + RANDOM % 999999)))
docker run weaviate