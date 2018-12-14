#!/usr/bin/env bash

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

set -e

go run ./tools/remote_weaviate_fake $@
