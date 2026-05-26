#!/usr/bin/env bash
#
# Mutation-testing runner wrapping mutest (https://github.com/fchimpan/mutest).
#
# Mutates one target package (relational-operator replacement: > >= < <= == !=)
# and runs that package's tests against each mutant. A surviving mutant is a
# source change the tests failed to detect — i.e. weak test coverage.
#
# Unlike a library-style runner, mutest is a standalone CLI: it rewrites source
# in place, runs `go test`, and restores the file. No build tags, no in-repo
# harness, no module dependency — it is installed as a tool binary on demand.
#
# Usage:
#   tools/mutest.sh <package> [extra mutest flags...]
#
# Examples:
#   tools/mutest.sh usecases/byteops
#   tools/mutest.sh usecases/cluster -v
#   tools/mutest.sh usecases/cluster/...           # include sub-packages
#   MUTEST_THRESHOLD=80 tools/mutest.sh entities/schema   # gate CI at 80%
#
# Environment overrides:
#   MUTEST_VERSION    pinned tool version to install   (default: v0.4.3)
#   MUTEST_TIMEOUT    per-mutant test timeout          (default: 60s)
#   MUTEST_WORKERS    parallel test processes          (default: mutest's NumCPU)
#   MUTEST_THRESHOLD  minimum kill rate %, 0-100; exit 1 if below
#                     (default: unset -> mutest fails if ANY mutant survives)
#
# Exit code: propagated from mutest (non-zero when below threshold), so it can
# gate CI. The full KILLED/SURVIVED report is printed regardless.
#
set -euo pipefail

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
  echo "usage: $0 <package> [extra mutest flags...]" >&2
  echo "   e.g. $0 usecases/byteops" >&2
  exit 2
fi
shift

REPO_ROOT="$(git rev-parse --show-toplevel)"
VERSION="${MUTEST_VERSION:-v0.4.3}"

# Normalise <package> into a ./-prefixed go package pattern. A bare directory
# (no "...") mutates only that package, conveniently skipping generated
# sub-packages such as mocks/.
norm="${TARGET#./}"
norm="${norm%/}"
PKG="./${norm}"
if [[ "$PKG" != *"..."* && ! -d "$REPO_ROOT/$norm" ]]; then
  echo "error: '$norm' is not a directory under $REPO_ROOT" >&2
  exit 2
fi

# Locate mutest; install the pinned version on demand. `go install pkg@version`
# ignores the surrounding module, so Weaviate's vendored go.mod is untouched and
# mutest is never added as a dependency.
MUTEST_BIN="$(command -v mutest || true)"
if [[ -z "$MUTEST_BIN" ]]; then
  GOBIN_DIR="$(go env GOBIN)"
  [[ -z "$GOBIN_DIR" ]] && GOBIN_DIR="$(go env GOPATH)/bin"
  MUTEST_BIN="$GOBIN_DIR/mutest"
fi
if [[ ! -x "$MUTEST_BIN" ]]; then
  echo "▶ installing mutest@$VERSION ..." >&2
  GOFLAGS=-mod=mod go install "github.com/fchimpan/mutest@$VERSION"
fi

flags=( -timeout "${MUTEST_TIMEOUT:-60s}" )
[[ -n "${MUTEST_WORKERS:-}" ]] && flags+=( -workers "$MUTEST_WORKERS" )
[[ -n "${MUTEST_THRESHOLD:-}" ]] && flags+=( -threshold "$MUTEST_THRESHOLD" )

echo "▶ mutest mutation testing" >&2
echo "    binary:    $MUTEST_BIN ($("$MUTEST_BIN" -version 2>/dev/null || echo '?'))" >&2
echo "    package:   $PKG" >&2
echo "    flags:     ${flags[*]} $*" >&2

# User-supplied flags ("$@") come after the defaults so they win on conflict;
# the package pattern is the trailing positional argument.
cd "$REPO_ROOT"
rc=0
"$MUTEST_BIN" "${flags[@]}" "$@" "$PKG" || rc=$?
exit "$rc"
