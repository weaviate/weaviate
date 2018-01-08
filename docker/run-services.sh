#!/bin/bash
#                          _       _
#__      _____  __ ___   ___  __ _| |_ ___
#\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
# \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
#  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
#
# Copyright © 2016 Weaviate. All rights reserved.
# LICENSE: https://github.com/weaviate/weaviate/blob/develop/LICENSE.md
# AUTHOR: Bob van Luijt (bob@weaviate.com)
# See www.weaviate.com for details
# Contact: @weaviate_iot / yourfriends@weaviate.com
#

# uncomment net line for debugging info
set -x

# Welkom message
echo "Building weaviate with Docker..."

# check OS
if [ "$OSTYPE" == "win32" ]; then
    echo "SORRY, NO WINDOWS SUPPORT"
    exit 1
fi

# check if jq is installed
jq --help >/dev/null 2>&1 || {
	echo we need jq, please run these commands, and try again 
    echo sudo apt-get -qq update
    echo sudo apt-get -qq install jq
}

# check if docker is installed
sudo docker ps >/dev/null 2>&1 || {
    echo we need docker, please run these commands, and try again
    echo sudo apt-get -qq update
    echo sudo apt-get -qq install \
    echo      apt-transport-https \
    echo      ca-certificates \
    echo      curl \
    echo      jq \
    echo      software-properties-common
    echo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add - 
    echo sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    echo sudo apt-get -qq update
    echo sudo apt-get -qq install docker-ce
    echo sudo groupadd docker
    echo sudo gpasswd -a $USER docker
    echo docker run hello-world
}

host=$(hostname)

# kill dgraph and weaviate
docker kill weaviate_dgraph &>/dev/null || true
docker kill weaviate &>/dev/null || true

# remove dgraph and weaviate
docker rm weaviate_dgraph &>/dev/null || true
docker rm weaviate &>/dev/null || true

# create dgraph, and weaviate  data directories
mkdir -p ~/tryweaviate/dgraph

#clean up at second run, needs sudo...
# sudo rm ~/tryweaviate/dgraph/*

mkdir -p ~/tryweaviate/weaviate/config
cd ~/tryweaviate/

# start dgraph docker
docker run --name weaviate_dgraph -itd -p 8080:8080 -p 9080:9080 -v ~/tryweaviate/dgraph:/dgraph dgraph/dgraph:v0.8.1 dgraph --bindall=true --memory_mb=2048 &>/dev/null || true
DGRAPHIP=$(docker inspect weaviate_dgraph | jq -r '.[0].NetworkSettings.IPAddress')

# build and start weaviate docker
echo "BUILDING WITH DGRAPH IP: $DGRAPHIP"

# when the install oneliner, and you need the online Dockerfile
if [ -f "Dockerfile" ]
then
	# Dockerfile found.
	echo . 
else
    # Dockerfile not found, download it"
    wget -q -O Dockerfile https://raw.githubusercontent.com/weaviate/weaviate/develop/docker/Dockerfile?i=$(echo $((1 + RANDOM % 999999)))	
fi

#build locally
#docker build --quiet --build-arg DGRAPHIP=$DGRAPHIP -t weaviate .
#Dockerfile should be present in current directory 
docker build --quiet -t weaviate .  &>/dev/null || true

# get the config file 
wget -q -O weaviate/config/example_weaviate.conf.json https://raw.githubusercontent.com/weaviate/weaviate/develop/weaviate.conf.json

# replace the DGRAPH IP
echo "UPDATE config WITH dgraph IP" ${DGRAPHIP} && \
JSONRESULT=$(jq ".environments[] | select(.name | contains(\"docker\")) | .database.database_config.host = \"${DGRAPHIP}\"" weaviate/config/example_weaviate.conf.json) && \
echo "{ \"environments\": [ $JSONRESULT ] }"  > weaviate/config/weaviate.conf.json



docker run --name weaviate -v ~/tryweaviate/weaviate/config:/var/weaviate/config  -p 8070:80 -d weaviate &>/dev/null || true 
WEAVIATEIP=$(sudo docker inspect weaviate | jq -r '.[0].NetworkSettings.IPAddress')

ROOTKEY=NO_ROOT_KEY_YET
echo "it takes a while to start dgraph and weaviate"

while [ "$ROOTKEY" == "NO_ROOT_KEY_YET" ]
do  
	echo "waiting 10 seconds..."
    sleep 10
	GET_ROOTKEY=$(docker exec -it weaviate grep -s ROOTKEY /var/weaviate/first_run.log|sed 's/.*ROOTKEY=//')
	if [ "$GET_ROOTKEY" != "" ]
	then
		ROOTKEY=$GET_ROOTKEY
		echo "dgraph and weaviate started"
    
	fi
done

# Return end-point
echo "Next line contains Weaviate IP + Weaviate ID seperated by a vertical bar (|)"
echo "WEAVIATE|$WEAVIATEIP|$ROOTKEY"

