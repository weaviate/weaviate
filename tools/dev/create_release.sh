#!/usr/bin/env bash

set -euo pipefail

print_usage() {
  echo "Usage: $0 <version> [--branch=<base_branch>] [--remote=<remote_name>] [--qa]"
  echo "Example:"
  echo "$0 v1.37.14"
  echo "$0 v1.37.14 --branch=stable/v1.37"
  echo "$0 v1.37.14 --branch=stable/v1.37 --remote=upstream"
  echo "$0 v1.37.14 --branch=stable/v1.37 --qa"
  echo ""
  echo "--qa requires --branch and additionally pushes the prepare-release"
  echo "branch, opens a PR against <base_branch>, and triggers the QA"
  echo "pipeline via tools/dev/qa_pr.sh."
}

if [ "$#" -lt 1 ]; then
  print_usage
  exit 1
fi

RAW_VERSION="$1"
VERSION="${1#v}"
shift

BASE_BRANCH=""
REMOTE="origin"
QA=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    --branch=*)
      BASE_BRANCH="${1#--branch=}"
      ;;
    --remote=*)
      REMOTE="${1#--remote=}"
      ;;
    --qa)
      QA=true
      ;;
    *)
      echo "Unknown argument: '$1'"
      print_usage
      exit 1
      ;;
  esac
  shift
done

REQUIRED_TOOLS="jq git make go"
if [ "$QA" = true ]; then
  REQUIRED_TOOLS="$REQUIRED_TOOLS gh awk"
fi

for tool in $REQUIRED_TOOLS; do
  if ! hash "$tool" 2>/dev/null; then
    echo "This script requires '$tool', but it is not installed."
    exit 1
  fi
done

if [ "$QA" = true ] && [ -z "$BASE_BRANCH" ]; then
  echo "Error: --qa requires --branch=<base_branch> (we need to know which branch to target with the prepare release PR)."
  print_usage
  exit 1
fi

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]]; then
  echo "Invalid version: '$RAW_VERSION'. Expected format: v1.2.3 or 1.2.3 (optionally with a -suffix)."
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

echo "==> Starting release process for v$VERSION"

if [ -n "$(git status --porcelain)" ]; then
  echo "Error: working tree has uncommitted changes. Commit, stash, or clean them before running this script."
  git status --short
  exit 1
fi

if [ -n "$BASE_BRANCH" ]; then
  echo "==> Fetching latest changes from remote '$REMOTE'"
  if ! git fetch "$REMOTE"; then
    echo "Error: 'git fetch $REMOTE' failed."
    exit 1
  fi

  echo "==> Checking out base branch '$BASE_BRANCH'"
  if ! git checkout "$BASE_BRANCH"; then
    echo "Error: base branch '$BASE_BRANCH' does not exist or cannot be checked out."
    exit 1
  fi

  echo "==> Fast-forwarding '$BASE_BRANCH' to '$REMOTE/$BASE_BRANCH'"
  if ! git merge --ff-only "$REMOTE/$BASE_BRANCH"; then
    echo "Error: 'git merge --ff-only $REMOTE/$BASE_BRANCH' failed."
    exit 1
  fi

  echo "==> Creating release branch 'prepare-release-v$VERSION'"
  git checkout -b "prepare-release-v$VERSION"
fi

SCHEMA_FILE="openapi-specs/schema.json"

echo "==> Running 'make deps' to vendor Go dependencies"
make deps

CURRENT_VERSION="$(jq -r '.info.version' "$SCHEMA_FILE")"
echo "==> Updating info.version in $SCHEMA_FILE from $CURRENT_VERSION to $VERSION"
TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT
jq --arg v "$VERSION" '.info.version = $v' "$SCHEMA_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$SCHEMA_FILE"
trap - EXIT

echo "==> Running tools/prepare_release.sh"
tools/prepare_release.sh

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo ""
echo "Release v$VERSION is prepared."

if [ "$QA" = false ]; then
  echo "Push the prepare release branch with: git push $REMOTE $CURRENT_BRANCH and create prepare release pull request"
  exit 0
fi

PR_TEMPLATE=".github/PULL_REQUEST_TEMPLATE.md"
if [ ! -f "$PR_TEMPLATE" ]; then
  echo "Error: PR template '$PR_TEMPLATE' not found; cannot construct PR body."
  exit 1
fi

PR_TITLE="prepare release v$VERSION"

echo "==> Pushing branch '$CURRENT_BRANCH' to remote '$REMOTE'"
git push --set-upstream "$REMOTE" "$CURRENT_BRANCH"

# Insert the PR title into the template body right after the
# "### What's being changed:" heading, preserving the rest of the template.
PR_BODY="$(awk -v title="$PR_TITLE" '
  !inserted && /^### What.s being changed:/ {
    print
    print ""
    print title
    inserted = 1
    next
  }
  { print }
' "$PR_TEMPLATE")"

echo "==> Creating prepare release PR against '$BASE_BRANCH'"
PR_LINK="$(gh pr create \
  --base "$BASE_BRANCH" \
  --head "$CURRENT_BRANCH" \
  --title "$PR_TITLE" \
  --body "$PR_BODY")"
echo "==> PR created: $PR_LINK"

echo "==> Waiting 30s for pipelines to start..."
sleep 30

QA_PR_SH="$ROOT_DIR/tools/dev/qa_pr.sh"
if [ ! -x "$QA_PR_SH" ]; then
  echo "Error: qa_pr.sh not found or not executable at $QA_PR_SH"
  exit 1
fi

echo "==> Triggering QA pipeline via tools/dev/qa_pr.sh"
QA_LOG="$(mktemp)"
"$QA_PR_SH" "$PR_LINK" 2>&1 | tee "$QA_LOG"
QA_SUMMARY="$(tail -n 1 "$QA_LOG")"
rm -f "$QA_LOG"

echo ""
echo "Success: prepare release PR opened and QA pipeline triggered for v$VERSION."
echo ">>>   Prepare release PR: $PR_LINK"
echo "$QA_SUMMARY"
