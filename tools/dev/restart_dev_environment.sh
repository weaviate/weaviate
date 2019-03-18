#!/usr/bin/env bash

set -e

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

docker-compose down --remove-orphans

rm -rf data connector_state.json schema_state.json

docker-compose up -d index janus db etcd genesis_fake weaviate_b_fake 

echo "You can now run the dev version with: ./tools/dev/run_dev_server.sh"