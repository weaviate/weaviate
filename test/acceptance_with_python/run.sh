#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Check if Python is installed
if ! command -v python3 &>/dev/null; then
    echo "Python is not installed. Please install Python and try again."
    exit 1
fi

# Check if a virtual environment (venv) exists
if [ ! -d ".venv" ]; then
    echo "Creating a new virtual environment (venv)..."
    python3 -m venv .venv
fi

# Activate the virtual environment
source .venv/bin/activate

cd "$SCRIPT_DIR" || return

pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

# Mode dispatch:
#   default      → standard python suite against docker-compose-test.yml
#   namespaces   → only test_namespace_refs.py against the 3-node
#                  docker-compose-namespaces-test.yml cluster (caller is
#                  responsible for bringing the compose up beforehand; see
#                  test/run.sh --acceptance-only-python-namespaces).
#
# test_readonly_recovery.py is always excluded from the default mode because
# it requires its own dedicated docker-compose-readonly-recovery-test.yml.
# Run it manually with:
#   pytest test/acceptance_with_python/test_readonly_recovery.py
MODE="${1:-default}"

# Dumps every container's log tail (including exited ones) after a test
# failure, so a crash is diagnosable from CI output without a local repro.
# Local copy of dump_container_logs from test/run.sh — that script cannot be
# sourced because it executes main on load.
dump_container_logs() {
  local tail_lines=2000
  echo "Dumping docker container logs (last $tail_lines lines per container)..."
  docker ps -a || true
  local ids
  ids=$(docker ps -aq) || true
  if [[ -z "$ids" ]]; then
    echo "dump_container_logs: no containers left (already cleaned up?)"
    return 0
  fi
  local id header
  for id in $ids; do
    header=$(docker inspect --format '{{.Name}} status={{.State.Status}} exit={{.State.ExitCode}} oom-killed={{.State.OOMKilled}}' "$id" 2>/dev/null || echo "$id")
    echo "===== BEGIN container logs: $header ====="
    docker logs --tail "$tail_lines" "$id" 2>&1 || true
    echo "===== END container logs: $header ====="
  done
}

case "$MODE" in
  namespaces)
    if ! pytest -n auto --dist loadgroup test_namespace_refs.py; then
      dump_container_logs
      exit 1
    fi
    ;;
  default)
    if ! pytest -n auto --dist loadgroup \
      --ignore=test_readonly_recovery.py \
      --ignore=test_namespace_refs.py \
      .; then
      dump_container_logs
      exit 1
    fi
    ;;
  *)
    echo "unknown mode: $MODE (expected one of: default, namespaces)" >&2
    exit 2
    ;;
esac
