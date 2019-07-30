#!/bin/bash

set -e 

docker-compose down --remove-orphans

docker-compose up -d esvector

go test -count 1 -tags=integrationTest ./adapters/repos/...
