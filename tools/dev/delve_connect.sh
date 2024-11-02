#!/usr/bin/env bash

set -e

echo "Connecting delve to headless instance at 127.0.0.1:2345..."

dlv connect 127.0.0.1:2345
