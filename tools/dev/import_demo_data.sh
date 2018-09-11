#!/usr/bin/env bash

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

# For now, we have hardcoded the credentials, host and location of dataset to import.
go run ./tools/fixture_importer $@
