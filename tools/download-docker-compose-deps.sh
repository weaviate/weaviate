#!/bin/bash

##
# LOADS ALL DOCKER COMPOSE DEPS
##

##
# Check if JQ is available
##
if which jq >/dev/null; then
    echo "jq is installed..."
else
    echo "jq is not installed, install it first."
    exit 1
fi

##
# Check if JQ is available
##
if which basename >/dev/null; then
    echo "basename is installed..."
else
    echo "basename is not installed, install it first."
    exit 1
fi

##
# Download the files via the Github API
##
for singleFile in $(curl -s https://api.github.com/repos/creativesoftwarefdn/weaviate/contents/docker-compose/runtime?ref=master | jq -r '.[] | @base64'); do
    _jq() {
        echo ${singleFile} | base64 --decode | jq -r ${1}
    }
    URL_RAW=$(_jq '.url') 
    URL_NOPRO=${URL_RAW:7}
    URL_REL=${URL_NOPRO#*/}
    FILENAME=$(basename "/${URL_REL%%\?*}")
    curl $(_jq '.download_url') --output $FILENAME
done