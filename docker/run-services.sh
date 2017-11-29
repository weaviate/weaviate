#!/bin/bash
#                          _       _
#__      _____  __ ___   ___  __ _| |_ ___
#\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
# \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
#  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
#
# Copyright © 2016 Weaviate. All rights reserved.
# LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
# AUTHOR: Bob van Luijt (bob@weaviate.com)
# See www.weaviate.com for details
# Contact: @weaviate_iot / yourfriends@weaviate.com
#

# Welkom message
echo "Building weaviate with Docker..."

# check OS
if [[ "$OSTYPE" == "win32" ]]; then
    echo "SORRY, NO WINDOWS SUPPORT"
    exit 1
fi

# check if jq is installed
jq --help >/dev/null 2>&1 || {
    sudo apt-get -qq update
    sudo apt-get -qq install jq
}

# check if docker is installed
sudo docker ps >/dev/null 2>&1 || {
    sudo apt-get -qq update
    sudo apt-get -qq install \
        apt-transport-https \
        ca-certificates \
        curl \
        jq \
        software-properties-common
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add - 
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    sudo apt-get -qq update
    sudo apt-get -qq install docker-ce
}

# get all running docker container names
containers=$(sudo docker ps | awk '{if(NR>1) print $NF}')
host=$(hostname)

# loop through all containers and stop weaviate and dgraph ones
for container in $containers
do
    if [[ $container == *"dgraph"* ]] ||  [[ $container == *"weaviate"* ]]; then
        echo "STOPPING: $container"
        sudo docker kill $container &>/dev/null
    fi
done

# remove dgraph and weaviate
sudo docker rm dgraph &>/dev/null || true
sudo docker rm weaviate &>/dev/null || true

# build and start dgraph docker
mkdir -p ~/dgraph
DGRAPHID=$(sudo docker run -itd -p 8080:8080 -p 9080:9080 -v ~/dgraph:/dgraph dgraph/dgraph:v0.8.1 dgraph --bindall=true --memory_mb=2048)
DGRAPHIP=$(sudo docker inspect $DGRAPHID | jq -r '.[0].NetworkSettings.IPAddress')

# build and start weaviate docker
echo "BUILDING WITH DGRAPH IP: $DGRAPHIP"
sudo docker build --quiet --build-arg DGRAPHIP=$DGRAPHIP -t weaviate "https://raw.githubusercontent.com/weaviate/weaviate/develop/docker/Dockerfile?i=$(echo $((1 + RANDOM % 999999)))"
WEAVIATEID=$(sudo docker run -d weaviate)
WEAVIATEIP=$(sudo docker inspect $WEAVIATEID | jq -r '.[0].NetworkSettings.IPAddress')

# Return end-point
echo "Next line contains Weaviate IP + Weaviate ID seperated by a vertical bar (|)"
echo "WEAVIATE|$WEAVIATEIP|$WEAVIATEID"
