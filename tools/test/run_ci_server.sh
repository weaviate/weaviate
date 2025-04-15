#!/usr/bin/env bash
set -euo pipefail

# This scripts starts a Weaviate server with the test scheme and waits until weaviate is up and running.

function build() {
  echo "Pull images..."
  surpress_on_success docker pull golang:1.19-alpine
  echo "Build containers (this will take the longest)..."
  GIT_REVISION=$(git rev-parse --short HEAD)
  GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
  echo_red $1 $2
  docker compose -f "$1" build --build-arg GIT_REVISION="$GIT_REVISION" --build-arg GIT_BRANCH="$GIT_BRANCH" --build-arg EXTRA_BUILD_ARGS="-race" "$2"
  echo "Start up docker compose setup..."
}

function wait(){
    MAX_WAIT_SECONDS=60
    ALREADY_WAITING=0

    while true; do
      if curl -s http://localhost:8080 > /dev/null; then
        break
      else
        if [ $? -eq 7 ]; then
          echo "Weaviate is not up yet. (waited for ${ALREADY_WAITING}s)"
          echo "Weaviate:"
          docker compose -f "$1" logs weaviate
          if [ $ALREADY_WAITING -gt $MAX_WAIT_SECONDS ]; then
            echo "Weaviate did not start up in $MAX_WAIT_SECONDS."
            docker compose -f "$1" logs
            exit 1
          else
            sleep 2
            let ALREADY_WAITING=$ALREADY_WAITING+2
          fi
        fi
      fi
    done

    echo "Weaviate is up and running!"
}
surpress_on_success() {
  out="$("${@}" 2>&1)" || { echo_red "FAILED!";  echo "$out"; return 1; }
  echo "Done!"
}

function echo_red() {
  red='\033[0;31m'
  nc='\033[0m'
  echo -e "${red}${*}${nc}"
}

START_WEAVIATE_AUTH=${1:-""}
if [ $# -eq 1 ] && [ "$1" == "--with-auth" ]; then
  START_WEAVIATE_AUTH="true"
fi

build docker-compose-test.yml weaviate
surpress_on_success docker compose -f docker-compose-test.yml up --force-recreate -d weaviate contextionary

if [ "$START_WEAVIATE_AUTH" == "true" ]; then
  build docker-compose-auth-test.yml weaviate-auth
  surpress_on_success docker compose -f docker-compose-auth-test.yml up --force-recreate -d weaviate-auth
fi

wait docker-compose-test.yml
if [ "$START_WEAVIATE_AUTH" == "true" ]; then
  wait docker-compose-auth-test.yml
fi
