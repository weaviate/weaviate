#!/usr/bin/env bash

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

set -e

DEBUG=1 go run ./tools/schema_loader -action-schema tools/dev/schema/actions_schema.json -thing-schema tools/dev/schema/things_schema.json $@

# For now, we have hardcoded the credentials, host and location of dataset to import.
DEBUG=1 go run ./tools/fixture_importer $@
