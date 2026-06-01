#!/usr/bin/env bash

# Trigger the QA pipeline (e2e + chaos tests) for a Weaviate release PR.
#
# Steps performed end-to-end:
#   1. Fetch docker image tags from the PR's docker workflow run
#   2. Create a QA issue in weaviate/weaviate-qa
#   3. Add the issue to project board #28 and set status to "In Progress"
#   4. Trigger the e2e + chaos tests matrix workflow
#
# Usage:
#   tools/dev/qa_pr.sh <pr_number_or_url> [--e2e-branch=<branch>] [--chaos-branch=<branch>]
#
# You can pass the PR in either of two ways:
#   1. As a PR number:
#        ./tools/dev/qa_pr.sh 11222
#   2. As a full PR URL:
#        ./tools/dev/qa_pr.sh https://github.com/weaviate/weaviate/pull/11222
#
# Optional flags (both default to "main" when omitted or empty):
#   --e2e-branch=<branch>    Branch of the e2e test suite to run
#   --chaos-branch=<branch>  Branch of the chaos test suite to run
#
# Examples:
#   ./tools/dev/qa_pr.sh 11222 --e2e-branch=custom-e2e-branch
#   ./tools/dev/qa_pr.sh 11222 --chaos-branch=custom-chaos-branch
#   ./tools/dev/qa_pr.sh 11222 --e2e-branch=foo --chaos-branch=bar
#
# Requires gh scopes: repo, project
#   gh auth refresh -h github.com -s project

set -euo pipefail

usage() {
  sed -n '3,30p' "$0" | sed 's/^# \{0,1\}//'
}

if [[ $# -eq 0 ]]; then
  echo "ERROR: missing required argument <pr_number_or_url>" >&2
  echo "" >&2
  usage >&2
  exit 1
fi

PR_INPUT=""
E2E_BRANCH=""
CHAOS_BRANCH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --e2e-branch=*)
      E2E_BRANCH="${1#--e2e-branch=}"
      shift
      ;;
    --chaos-branch=*)
      CHAOS_BRANCH="${1#--chaos-branch=}"
      shift
      ;;
    --*)
      echo "ERROR: unknown option '$1'" >&2
      echo "" >&2
      usage >&2
      exit 1
      ;;
    *)
      if [[ -n "$PR_INPUT" ]]; then
        echo "ERROR: unexpected positional argument '$1' (PR already set to '$PR_INPUT')" >&2
        echo "" >&2
        usage >&2
        exit 1
      fi
      PR_INPUT="$1"
      shift
      ;;
  esac
done

if [[ -z "$PR_INPUT" ]]; then
  echo "ERROR: missing required argument <pr_number_or_url>" >&2
  echo "" >&2
  usage >&2
  exit 1
fi

# Default both branches to "main" when omitted or empty.
E2E_BRANCH="${E2E_BRANCH:-main}"
CHAOS_BRANCH="${CHAOS_BRANCH:-main}"

# Accept either a bare PR number (e.g. 11222) or a GitHub PR URL
# (e.g. https://github.com/weaviate/weaviate/pull/11222).
if [[ "$PR_INPUT" =~ ^[0-9]+$ ]]; then
  PR_NUMBER="$PR_INPUT"
elif [[ "$PR_INPUT" =~ ^https?://github\.com/[^/]+/[^/]+/pull/([0-9]+)(/.*)?$ ]]; then
  PR_NUMBER="${BASH_REMATCH[1]}"
else
  echo "ERROR: '$PR_INPUT' is not a PR number or a GitHub PR URL" >&2
  echo "" >&2
  usage >&2
  exit 1
fi

WEAVIATE_REPO="weaviate/weaviate"
QA_REPO="weaviate/weaviate-qa"

# Project #28 in weaviate/weaviate-qa
PROJECT_ORG="weaviate"
PROJECT_NUMBER="28"
PROJECT_NODE_ID="PVT_kwDOAkCx8s4A5etq"
PROJECT_STATUS_FIELD_ID="PVTSSF_lADOAkCx8s4A5etqzguRUHg"
PROJECT_IN_PROGRESS_OPTION_ID="47fc9ee4"

log() { echo ">>> $*" >&2; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "ERROR: $1 is required but not installed" >&2; exit 1; }
}

require_cmd gh
require_cmd jq

ASSIGNEE=$(gh api user --jq '.login' 2>/dev/null || true)
if [[ -z "$ASSIGNEE" ]]; then
  echo "ERROR: could not determine current GitHub user (is 'gh auth login' done?)" >&2
  exit 1
fi

# ===== Step 1: Fetch docker image tags =====
log "Step 1: Fetching docker image tags for PR #$PR_NUMBER"

PR_INFO=$(gh pr view "$PR_NUMBER" --repo "$WEAVIATE_REPO" --json title,headRefName,commits)
PR_TITLE=$(echo "$PR_INFO" | jq -r '.title')
PR_BRANCH=$(echo "$PR_INFO" | jq -r '.headRefName')
COMMIT_COUNT=$(echo "$PR_INFO" | jq '.commits | length')
log "PR title:  $PR_TITLE"
log "PR branch: $PR_BRANCH"
log "Commits:   $COMMIT_COUNT"

# A "release PR" is one whose sole commit headline contains "prepare release"
# (matches the convention used by the release-prep automation, e.g.
# "prepare release v1.36.13"). Anything else is treated as a regular PR being
# QA'd ad-hoc, so the issue title doesn't claim to be a release.
IS_RELEASE_PR=false
if [[ "$COMMIT_COUNT" == "1" ]]; then
  COMMIT_HEADLINE=$(echo "$PR_INFO" | jq -r '.commits[0].messageHeadline // ""')
  if echo "$COMMIT_HEADLINE" | grep -qi 'prepare release'; then
    IS_RELEASE_PR=true
  fi
fi

if [[ "$IS_RELEASE_PR" == true ]]; then
  VERSION=$(echo "$PR_TITLE" | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
  if [[ -z "$VERSION" ]]; then
    log "ERROR: Could not extract semver (vX.Y.Z) from PR title: $PR_TITLE"
    exit 1
  fi
  log "Version:   $VERSION"
  ISSUE_TITLE="Release: $VERSION"
else
  ISSUE_TITLE="QA PR: $PR_NUMBER"
  log "Issue:     $ISSUE_TITLE (not a release PR)"
fi

log "Locating Tests workflow run on branch $PR_BRANCH..."
# The docker images are produced inside the "Tests" workflow by the "docker report" job.
# We iterate through recent Tests runs (newest first) and use the first one whose
# docker report job is either in-progress (we'll wait) or already succeeded.
# This handles freshly-pushed branches, branches with retried runs, and merged/closed
# PRs whose pipelines have already finished.
TESTS_RUNS=$(gh run list --repo "$WEAVIATE_REPO" --branch "$PR_BRANCH" \
  --workflow Tests --json databaseId,status,conclusion,createdAt --limit 10)

if [[ -z "$TESTS_RUNS" || "$TESTS_RUNS" == "[]" ]]; then
  log "ERROR: No Tests workflow runs found for branch $PR_BRANCH"
  exit 1
fi

DOCKER_RUN_ID=""
JOB_ID=""

for run_id in $(echo "$TESTS_RUNS" | jq -r '.[].databaseId'); do
  log "  Checking Tests run $run_id for docker report job..."
  JOB_INFO=$(gh run view "$run_id" --repo "$WEAVIATE_REPO" --json jobs \
    --jq '.jobs[] | select(.name | test("^docker report$|^generate-docker-report$"))' 2>&1 || true)

  if [[ -z "$JOB_INFO" || "$JOB_INFO" == "null" ]]; then
    # Run is too fresh — no docker report job queued yet. Treat as in-progress.
    DOCKER_RUN_ID="$run_id"
    log "  Tests run $run_id has no docker report job yet; will wait"
    break
  fi

  job_status=$(echo "$JOB_INFO" | jq -r '.status // "unknown"')
  job_conclusion=$(echo "$JOB_INFO" | jq -r '.conclusion // "unknown"')

  if [[ "$job_status" == "completed" && "$job_conclusion" == "success" ]]; then
    DOCKER_RUN_ID="$run_id"
    JOB_ID=$(echo "$JOB_INFO" | jq -r '.databaseId')
    log "  Tests run $run_id has successful docker report (job ID: $JOB_ID)"
    break
  elif [[ "$job_status" != "completed" ]]; then
    DOCKER_RUN_ID="$run_id"
    log "  Tests run $run_id docker report is $job_status; will wait"
    break
  fi

  log "  Tests run $run_id docker report is $job_status/$job_conclusion; trying older run"
done

if [[ -z "$DOCKER_RUN_ID" ]]; then
  log "ERROR: No Tests workflow run with a usable docker report job found for branch $PR_BRANCH"
  exit 1
fi
log "Tests run ID: $DOCKER_RUN_ID"

if [[ -z "$JOB_ID" ]]; then
  log "Waiting for docker report job to complete..."
  while true; do
    JOB_INFO=$(gh run view "$DOCKER_RUN_ID" --repo "$WEAVIATE_REPO" --json jobs \
      --jq '.jobs[] | select(.name | test("^docker report$|^generate-docker-report$"))' 2>&1 || true)

    if [[ -n "$JOB_INFO" && "$JOB_INFO" != "null" ]]; then
      JOB_STATUS=$(echo "$JOB_INFO" | jq -r '.status // "unknown"')
      JOB_CONCLUSION=$(echo "$JOB_INFO" | jq -r '.conclusion // "unknown"')
      log "  [$(date '+%H:%M:%S')] status=$JOB_STATUS conclusion=$JOB_CONCLUSION"

      if [[ "$JOB_STATUS" == "completed" ]]; then
        if [[ "$JOB_CONCLUSION" == "success" ]]; then
          JOB_ID=$(echo "$JOB_INFO" | jq -r '.databaseId')
          log "docker report completed successfully (job ID: $JOB_ID)"
          break
        else
          log "ERROR: docker report concluded with: $JOB_CONCLUSION"
          exit 1
        fi
      fi
    else
      log "  [$(date '+%H:%M:%S')] docker report job not yet started"
    fi
    sleep 30
  done
fi

log "Extracting docker image tags from job logs..."
# Use gh api (works even when logs are large / job just completed)
TAGS=$(gh api "repos/$WEAVIATE_REPO/actions/jobs/$JOB_ID/logs" 2>&1 \
  | sed 's/\x1b\[[0-9;]*m//g' \
  | grep -oE 'semitechnologies/weaviate:[a-zA-Z0-9._-]+' \
  | sort -u || true)

# Prefer the preview-branch tag (e.g. preview-prepare-release-v1-36-13-<sha>) over the
# semver tag (e.g. 1.36.13-<sha>) — the preview tag encodes the source branch and is
# what the QA workflow expects per the QA release process.
#
# Two shapes are possible depending on how the PR built its image:
#   - per-arch (default): two tags, suffixed `.amd64` and `.arm64`
#   - multi-arch (opt-in): a single tag covering both arches, no suffix
AMD64_TAG=$(echo "$TAGS" | grep '^semitechnologies/weaviate:preview-.*\.amd64$' | head -1 || true)
ARM64_TAG=$(echo "$TAGS" | grep '^semitechnologies/weaviate:preview-.*\.arm64$' | head -1 || true)
MULTIARCH_TAG=""

if [[ -n "$AMD64_TAG" && -n "$ARM64_TAG" ]]; then
  log "amd64 tag: $AMD64_TAG"
  log "arm64 tag: $ARM64_TAG"
  WEAVIATE_VERSION_INPUT="${AMD64_TAG#semitechnologies/weaviate:}"
  TAGS_BLOCK="$AMD64_TAG"$'\n'"$ARM64_TAG"
else
  MULTIARCH_TAG=$(echo "$TAGS" \
    | grep '^semitechnologies/weaviate:preview-' \
    | grep -vE '\.(amd64|arm64)$' | head -1 || true)
  if [[ -z "$MULTIARCH_TAG" ]]; then
    log "ERROR: Could not extract preview-* docker tags from job logs (neither per-arch nor multi-arch)"
    log "Tags found:"
    echo "$TAGS" | sed 's/^/  /' >&2
    exit 1
  fi
  log "multi-arch tag: $MULTIARCH_TAG"
  WEAVIATE_VERSION_INPUT="${MULTIARCH_TAG#semitechnologies/weaviate:}"
  TAGS_BLOCK="$MULTIARCH_TAG"
fi

# ===== Step 2: Create QA issue =====
log ""
log "Step 2: Creating QA issue in $QA_REPO"

if [[ "$IS_RELEASE_PR" == true ]]; then
  ISSUE_BODY=$(cat <<EOF
This issue tracks the testing executed to validate the Weaviate version $VERSION.

\`\`\`
$TAGS_BLOCK
\`\`\`
EOF
)
else
  ISSUE_BODY=$(cat <<EOF
This issue tracks the QA tests executed for [PR #$PR_NUMBER](https://github.com/$WEAVIATE_REPO/pull/$PR_NUMBER): $PR_TITLE.

\`\`\`
$TAGS_BLOCK
\`\`\`
EOF
)
fi

ISSUE_URL=$(gh issue create \
  --repo "$QA_REPO" \
  --title "$ISSUE_TITLE" \
  --body "$ISSUE_BODY" \
  --assignee "$ASSIGNEE")

ISSUE_NUMBER=$(echo "$ISSUE_URL" | grep -oE '[0-9]+$')
log "Created issue: $ISSUE_URL (#$ISSUE_NUMBER)"

# ===== Step 3: Add issue to project board #28 and set status to In Progress =====
log ""
log "Step 3: Adding issue to project board #28 and setting status to 'In Progress'"

ISSUE_NODE_ID=$(gh api "repos/$QA_REPO/issues/$ISSUE_NUMBER" --jq '.node_id')

ITEM_RESPONSE=$(gh api graphql -f query="
mutation {
  addProjectV2ItemById(input: {projectId: \"$PROJECT_NODE_ID\", contentId: \"$ISSUE_NODE_ID\"}) {
    item { id databaseId }
  }
}")

ITEM_ID=$(echo "$ITEM_RESPONSE" | jq -r '.data.addProjectV2ItemById.item.id')
ITEM_DB_ID=$(echo "$ITEM_RESPONSE" | jq -r '.data.addProjectV2ItemById.item.databaseId')

if [[ -z "${ITEM_ID:-}" || "$ITEM_ID" == "null" ]]; then
  log "ERROR: Failed to add issue to project board (missing 'project' scope?)"
  log "  Run: gh auth refresh -h github.com -s project"
  exit 1
fi

# Build the deep-link URL to the issue inside the project board view.
# The `issue` query param is "<owner>|<repo>|<number>" URL-encoded ('|' -> '%7C').
QA_OWNER="${QA_REPO%%/*}"
QA_REPO_NAME="${QA_REPO##*/}"
BOARD_URL="https://github.com/orgs/${PROJECT_ORG}/projects/${PROJECT_NUMBER}?pane=issue&itemId=${ITEM_DB_ID}&issue=${QA_OWNER}%7C${QA_REPO_NAME}%7C${ISSUE_NUMBER}"

gh api graphql -f query="
mutation {
  updateProjectV2ItemFieldValue(input: {
    projectId: \"$PROJECT_NODE_ID\",
    itemId: \"$ITEM_ID\",
    fieldId: \"$PROJECT_STATUS_FIELD_ID\",
    value: { singleSelectOptionId: \"$PROJECT_IN_PROGRESS_OPTION_ID\" }
  }) { projectV2Item { id } }
}" >/dev/null

log "Added to project board (item ID: $ITEM_ID), status set to 'In Progress'"

# ===== Step 4: Trigger tests matrix workflow =====
log ""
log "Step 4: Triggering tests matrix workflow in $QA_REPO"
log "  weaviate_version:        $WEAVIATE_VERSION_INPUT"
log "  issue_number:            $ISSUE_NUMBER"
log "  run_e2e_tests:           true"
log "  e2e_branch:              $E2E_BRANCH"
log "  run_chaos_tests:         true"
log "  chaos_branch:            $CHAOS_BRANCH"
log "  run_vectorizer_tests:    false"
log "  run_performance_tests:   false"
log "  include_7_replicas:      true"

gh workflow run main.yaml \
  --repo "$QA_REPO" \
  -f weaviate_version="$WEAVIATE_VERSION_INPUT" \
  -f e2e_branch="$E2E_BRANCH" \
  -f run_e2e_tests=true \
  -f run_vectorizer_tests=false \
  -f include_7_replicas=true \
  -f run_chaos_tests=true \
  -f chaos_branch="$CHAOS_BRANCH" \
  -f run_performance_tests=false \
  -f performance_branch="main" \
  -f issue_number="$ISSUE_NUMBER"

log ""
log "QA pipeline triggered successfully!"
log "  QA board link: $BOARD_URL"
