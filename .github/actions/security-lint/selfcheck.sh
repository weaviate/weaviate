#!/usr/bin/env bash
# Positive control for linter_hidden_unicode.sh.
#
# The linter prints "No hidden Unicode characters detected." and exits 0 both
# when a diff is genuinely clean and when the scanner has stopped matching
# altogether, so a green PR Security Lint proves nothing on its own. This feeds
# the linter a known-bad diff and fails unless it fires and names the character.
#
# It runs as the first step of the composite action so every consumer of that
# action, including the client repos that pin it by SHA and have no Go
# toolchain, re-proves the scanner on every run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINTER="$SCRIPT_DIR/linter_hidden_unicode.sh"

fail() {
    echo "POSITIVE CONTROL FAILED: $LINTER $1" >&2
    echo "  The hidden Unicode scanner no longer detects trojan-source characters." >&2
    echo "  Until this is fixed, PR Security Lint passes every pull request." >&2
    exit 1
}

# Written as UTF-8 bytes rather than a literal character: a tracked file
# containing a real U+200B would be flagged by this very linter when it scans
# the pull request that adds the file.
zwsp=$(printf '\xe2\x80\x8b')

if bad_out=$(printf '+++ b/fixture.go\n@@ -0,0 +1 @@\n+var x = "%s"\n' "$zwsp" \
    | bash "$LINTER" --stdin 2>&1); then
    fail "exited 0 on a known-bad diff containing U+200B ZERO WIDTH SPACE. Output: $bad_out"
fi

case "$bad_out" in
    *"U+200B ZERO WIDTH SPACE"*) ;;
    *) fail "exited non-zero on the known-bad diff but never named U+200B ZERO WIDTH SPACE. Output: $bad_out" ;;
esac

# Without this the control would also pass against a linter hardwired to fail.
if ! clean_out=$(printf '+++ b/fixture.go\n@@ -0,0 +1 @@\n+var x = "safe"\n' \
    | bash "$LINTER" --stdin 2>&1); then
    fail "rejected a clean diff, so its failure on the known-bad diff proves nothing. Output: $clean_out"
fi

echo "Positive control passed: hidden Unicode scanner fires on U+200B and passes a clean diff."
