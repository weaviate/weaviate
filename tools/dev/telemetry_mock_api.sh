#!/usr/bin/env bash

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

go run ./tools/telemetry_mock_api/mock_api.go
