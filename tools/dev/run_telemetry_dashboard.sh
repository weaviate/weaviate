#!/usr/bin/env bash

# Local telemetry dashboard for development.
# Receives telemetry payloads from Weaviate and displays them in a web UI.
#
# Usage:
#   ./tools/dev/run_telemetry_dashboard.sh
#
# Dashboard will be available at http://localhost:9696
# Use Ctrl+C to stop.

set -e

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

PORT=9696

# Check if something is already listening on the port
if lsof -i :${PORT} -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Error: Port ${PORT} is already in use."
    echo "To stop the existing process: kill \$(lsof -t -i :${PORT})"
    exit 1
fi

echo "Starting telemetry dashboard on http://localhost:${PORT}"
echo "Press Ctrl+C to stop."
echo ""

exec go run ./tools/telemetry-dashboard/main.go
