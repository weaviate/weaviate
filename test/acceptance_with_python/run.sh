#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Check if Python is installed
if ! command -v python3 &>/dev/null; then
    echo "Python is not installed. Please install Python and try again."
    exit 1
fi

# Check if a virtual environment (venv) exists
if [ ! -d "venv" ]; then
    echo "Creating a new virtual environment (venv)..."
    python3 -m venv .venv
fi

# Activate the virtual environment
source .venv/bin/activate

cd "$SCRIPT_DIR" || return

pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

# run python tests in parallel
pytest -n auto --dist loadgroup .
