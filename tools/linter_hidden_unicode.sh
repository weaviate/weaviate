#!/usr/bin/env bash
# Convenience wrapper for local dev. The canonical script lives inside the
# security-lint composite action so client repos (Python, TS, Go, Java, C#)
# can reuse it by SHA. This wrapper keeps the familiar tools/ entry point.
#
# A plain forwarding script is used instead of a symlink because symlinks
# aren't reliably supported across developer OSes (notably Windows + some
# Git/filesystem configurations).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/../.github/actions/security-lint/linter_hidden_unicode.sh" "$@"
