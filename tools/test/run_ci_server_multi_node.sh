#!/bin/bash
set -euo pipefail

# This scripts starts a Weaviate multi-node cluster with the test scheme and waits until weaviate is up and running.

NODES=${1:-3}
COMPOSE_FILE=docker-compose-raft.yml
GENERATE_SCRIPT=docker-compose-raft/raft_cluster.sh

# This auxiliary function returns the number of voters based on the number of nodes, passing the number of nodes as an argument.
function get_voters() {
    if [[ $1 -ge 10 ]]; then
        echo 7
    elif [[ $1 -ge 7 && $1 -lt 10 ]]; then
        echo 5
    elif [[ $1 -ge 4 && $1 -lt 7 ]]; then
        echo 3
    elif [[ $1 -ge 3 && $1 -lt 4 ]]; then
        echo 2
    else
        echo 1
    fi
}

function generate_raft_compose() {
    $GENERATE_SCRIPT $1

    if [ ! -f "$COMPOSE_FILE" ]; then
        echo "Something went wrong generating the RAFT docker-compose file. $COMPOSE_FILE is missing."
        exit 1
    fi
}

function build() {
  echo "Pull images..."
  surpress_on_success docker pull golang:1.19-alpine
  echo "Build containers (this will take the longest)..."
  GIT_HASH=$(git rev-parse --short HEAD)
  docker compose -f "$1" build --build-arg GITHASH="$GIT_HASH" --build-arg EXTRA_BUILD_ARGS="-race" "$2"
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

VOTERS=$(get_voters $NODES)
generate_raft_compose $VOTERS
build $COMPOSE_FILE weaviate
surpress_on_success docker compose -f $COMPOSE_FILE up --force-recreate -d --scale weaviate=$((NODES - VOTERS))

wait $COMPOSE_FILE
