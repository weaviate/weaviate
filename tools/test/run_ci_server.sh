#!/usr/bin/env bash
set -euo pipefail

# This scripts starts a Weaviate server with the test scheme and waits until weaviate is up and running.

function main() {
  echo "Pull images..."
  surpress_on_success docker pull golang:1.11-alpine
  echo "Build containers (this will take the longest)..."
  docker-compose -f docker-compose-test.yml build weaviate esvector
  echo "Start up docker-compose setup..."
  surpress_on_success docker-compose -f docker-compose-test.yml up --force-recreate -d weaviate \
    contextionary 

  MAX_WAIT_SECONDS=60
  ALREADY_WAITING=0

  while true; do
    if curl -s http://localhost:8080 > /dev/null; then
      break
    else
      if [ $? -eq 7 ]; then
        echo "Weaviate is not up yet. (waited for ${ALREADY_WAITING}s)"
        echo "Weaviate:"
        docker-compose -f docker-compose-test.yml logs weaviate
        if [ $ALREADY_WAITING -gt $MAX_WAIT_SECONDS ]; then
          echo "Weaviate did not start up in $MAX_WAIT_SECONDS."
          docker-compose -f docker-compose-test.yml logs
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

main "$@"
