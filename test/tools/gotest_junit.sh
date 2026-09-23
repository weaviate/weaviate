# Sourced by test/run.sh and test/integration/run.sh. Everything is gated on
# JUNIT_DIR: unset (the default, and the local-dev case) means plain `go test`
# with unchanged behavior. With JUNIT_DIR set, go_test runs through gotestsum,
# which prints go test's results in its own format and additionally writes one
# JUnit XML file per invocation for CI result reporting.

GOTESTSUM_VERSION="v1.13.0"
GOTESTSUM_BIN=""
_junit_seq=0

# junit_init normalizes JUNIT_DIR to an absolute path (it must survive the
# `cd` into nested test modules) and installs gotestsum. Call it once, from
# the repository root, before the first go_test.
function junit_init() {
  if [[ -z "${JUNIT_DIR:-}" ]]; then
    return 0
  fi
  mkdir -p "$JUNIT_DIR"
  JUNIT_DIR="$(cd "$JUNIT_DIR" && pwd)"
  export JUNIT_DIR
  # A version-specific directory guarantees the pinned version is the one
  # used, even when another gotestsum is already on PATH or in GOBIN.
  local tool_dir
  tool_dir="$(go env GOPATH | cut -d: -f1)/gotestsum-${GOTESTSUM_VERSION}"
  if [[ -x "$tool_dir/gotestsum" ]] ||
    GOBIN="$tool_dir" go install "gotest.tools/gotestsum@${GOTESTSUM_VERSION}"; then
    GOTESTSUM_BIN="$tool_dir/gotestsum"
  else
    # Never fail a test run over reporting tooling. Go tests degrade to plain
    # go test; JUNIT_DIR stays set so pytest XML and the stale-XML cleanup in
    # test/run.sh still apply.
    echo "WARN: gotestsum install failed; Go JUnit output disabled" >&2
  fi
}

# go_test is a drop-in replacement for `go test`; all flags pass through
# verbatim and the exit code is gotestsum's passthrough of go test's.
function go_test() {
  if [[ -z "${JUNIT_DIR:-}" || -z "$GOTESTSUM_BIN" ]]; then
    go test "$@"
    return
  fi
  _junit_seq=$((_junit_seq + 1))
  # Mirror the caller's output style: verbose only when it asked for -v.
  local format="standard-quiet"
  local arg
  for arg in "$@"; do
    if [[ "$arg" == "-v" ]]; then
      format="standard-verbose"
    fi
  done
  # Package identity lives inside the XML, so the filename only needs to be
  # unique per invocation; $$ separates test/run.sh from the child
  # test/integration/run.sh process.
  "$GOTESTSUM_BIN" \
    --format "$format" --format-hide-empty-pkg \
    --junitfile "$(printf '%s/go-%s-%03d.xml' "$JUNIT_DIR" "$$" "$_junit_seq")" \
    -- "$@"
}
