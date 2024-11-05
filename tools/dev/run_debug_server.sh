#!/usr/bin/env sh

set -e

function echo_green() {
  green='\033[0;32m'
  nc='\033[0m'
  echo "${green}${*}${nc}"
}

echo_green "Building and starting a debug instance of weaviate locally..."

if [[ ! -f docker-compose-debug.yml ]]; then
    echo "Could not locate docker-compose-debug.yml file. Ensure your current PWD is the root the repository"
    exit 1
fi

# Use of the --build flag ensure the latest local changes are picked up and we are debugging the current state of the repo
docker compose -f docker-compose-debug.yml up --build -d

echo_green "Debug instance of weaviate started locally"
