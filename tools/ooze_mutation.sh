#!/usr/bin/env bash
#
# Reusable ooze (https://github.com/gtramontina/ooze) mutation-testing runner.
#
# Mutates one target package and reports surviving mutants — source changes that
# the package's tests failed to catch. A high "survived" count means weak tests.
#
# Usage:
#   tools/ooze_mutation.sh <package-dir> [extra go-test args...]
#
# Examples:
#   tools/ooze_mutation.sh usecases/byteops
#   tools/ooze_mutation.sh usecases/floatcomp -parallel 4
#
# Environment overrides:
#   OOZE_TEST_CMD      test command run per mutant
#                      (default: "go test -count=1 -timeout 60s ./<target>/...")
#   OOZE_THRESHOLD     minimum score 0.0-1.0; below it the run exits non-zero
#                      (default: 0.0, i.e. report only)
#   OOZE_IGNORE_EXTRA  extra regexp of files to exclude from mutation, e.g.
#                      'copy_(be|le)\.go$' to skip files ooze cannot format
#                      (generic type unions) or build-tagged files inactive on
#                      this architecture
#   OOZE_PARALLEL      mutant concurrency (default: 6)
#   OOZE_RUN_TIMEOUT   overall go-test timeout (default: 60m)
#
set -euo pipefail

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
  echo "usage: $0 <package-dir> [extra go-test args...]" >&2
  echo "   e.g. $0 usecases/byteops" >&2
  exit 2
fi
shift

REPO_ROOT="$(git rev-parse --show-toplevel)"

# Normalise: strip a leading ./ and any trailing slash.
TARGET="${TARGET#./}"
TARGET="${TARGET%/}"

if [[ ! -d "$REPO_ROOT/$TARGET" ]]; then
  echo "error: '$TARGET' is not a directory under $REPO_ROOT" >&2
  exit 2
fi

if ! find "$REPO_ROOT/$TARGET" -name '*.go' ! -name '*_test.go' | grep -q .; then
  echo "error: no non-test .go files found under '$TARGET' — nothing to mutate" >&2
  exit 2
fi

export OOZE_REPO_ROOT="$REPO_ROOT"
export OOZE_TARGET="$TARGET"
export OOZE_TEST_CMD="${OOZE_TEST_CMD:-go test -count=1 -timeout 60s ./$TARGET/...}"
export OOZE_THRESHOLD="${OOZE_THRESHOLD:-0.0}"
export OOZE_IGNORE_EXTRA="${OOZE_IGNORE_EXTRA:-}"

echo "▶ ooze mutation testing" >&2
echo "    target:    $TARGET" >&2
echo "    test cmd:  $OOZE_TEST_CMD" >&2
echo "    threshold: $OOZE_THRESHOLD" >&2
[[ -n "$OOZE_IGNORE_EXTRA" ]] && echo "    ignore +:  $OOZE_IGNORE_EXTRA" >&2

# -mod=mod is required: Weaviate vendors deps (vendor/ is git-ignored) but ooze
# is a tag-gated dev tool that is not vendored, so the outer test binary must
# resolve it from the module cache. The inner test command keeps vendor mode.
cd "$REPO_ROOT"
exec go test -mod=mod -tags=mutation -v -run '^TestMutation$' \
  -parallel "${OOZE_PARALLEL:-6}" \
  -timeout "${OOZE_RUN_TIMEOUT:-60m}" \
  "$@" \
  ./tools/ooze/
