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

case "$MODE" in
  namespaces)
    pytest -n auto --dist loadgroup test_namespace_refs.py
    ;;
  default)
    pytest -n auto --dist loadgroup \
      --ignore=test_readonly_recovery.py \
      --ignore=test_namespace_refs.py \
      .
    ;;
  *)
    echo "unknown mode: $MODE (expected one of: default, namespaces)" >&2
    exit 2
    ;;
esac
