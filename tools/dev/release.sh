#!/usr/bin/env bash
# Weaviate release driver — state machine for cutting a core release.
# Lives at tools/dev/release.sh. Run from the weaviate repo root.
#
# Delegates QA dispatch to tools/dev/qa_pr.sh.
#
# State sources (checked in this order):
#   1. Journal file at _local/release/v<version>.json — only when --journal is passed.
#   2. Inferred state — probed from GitHub + the local repo at init time:
#        • prep branch     →  git ls-remote prepare-release-v<ver>
#        • PR + merge state →  gh pr list --head prepare-release-v<ver>
#        • QA tracking issue →  gh issue list --repo weaviate/weaviate-qa
#        • QA E2E/Chaos    →  project board "Central CI View" fields
#        • tag             →  git ls-remote --tags v<ver>
#        • release         →  gh release view v<ver>
#      Inference runs whenever there's no journal to read (stateless mode, or
#      --journal passed but the file doesn't exist yet). The result lives in
#      memory only — to persist it, re-run with --journal.
#
# Common entry points (run from the weaviate repo root):
#
#   bash tools/dev/release.sh                              # auto-detect branch + resume
#   bash tools/dev/release.sh prepare    <version>         # explicit prepare
#   bash tools/dev/release.sh qa         <version>         # poll docker, dispatch QA
#   bash tools/dev/release.sh monitor-qa <version>         # poll board until E2E+Chaos settle
#   bash tools/dev/release.sh finalize   <version> [<pr#>] # full post-merge flow
#   bash tools/dev/release.sh finalize   <step> <version>  # single step: merge|tag|draft|image|publish
#   bash tools/dev/release.sh status     <version>         # human-readable state summary
#   bash tools/dev/release.sh reset      <version>         # delete state file
#   bash tools/dev/release.sh reset-step <version> <step>  # wipe one step from state
#
# Read-only helpers (run from inside a weaviate clone, or pass clone path):
#   bash tools/dev/release.sh journals   [clone-path]
#   bash tools/dev/release.sh candidates [clone-path]
#
# Verification (any CWD; uses GitHub + Docker Hub — no clone needed):
#   bash tools/dev/release.sh verify <stage> <version>
#     stages: prepare | merge | finalize | publish

set -euo pipefail
shopt -s nullglob

# Loud-fail safety net. `set -e` can otherwise abort silently — especially on
# `[[ ]] && cmd` short-circuits that bubble out of helper functions. Direct
# `exit N` calls from validation errors do not run ERR (they run EXIT), so
# this only fires on unintended aborts. $LINENO and $BASH_COMMAND are captured
# at trap-fire time and passed as args so they reflect the failing line, not
# the trap handler's body.
_err_trap() {
  printf "ERROR: release.sh aborted (exit %d) at line %d: %s\n" "$1" "$2" "$3" >&2
}
trap '_err_trap "$?" "$LINENO" "$BASH_COMMAND"' ERR

REPO="weaviate/weaviate"
STATE_DIR="_local/release"
SPEC="openapi-specs/schema.json"
QA_PR_SH="${QA_PR_SH:-$(cd "$(dirname "$0")" && pwd)/qa_pr.sh}"
CREATE_RELEASE_SH="${CREATE_RELEASE_SH:-$(cd "$(dirname "$0")" && pwd)/create_release.sh}"

# X.Y.Z or X.Y.Z-rc.N / -alpha.N / -beta.N (lowercase pre-release tags only).
VERSION_RE='^[0-9]+\.[0-9]+\.[0-9]+(-(rc|alpha|beta)\.[0-9]+)?$'

for tool in jq git gh sed curl awk; do
  hash "$tool" 2>/dev/null || { echo "ERROR: '$tool' is required but not installed." >&2; exit 1; }
done

# ─── small helpers (no state-file dependency) ────────────────────────────────

# gate_banner "TITLE" "line1" "line2" ... — prints a uniform horizontal-rule banner.
gate_banner() {
  local title="$1"; shift
  local rule="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  printf '\n%s\n%s\n' "$rule" "$title"
  local line; for line in "$@"; do printf '  %s\n' "$line"; done
  printf '%s\n' "$rule"
}

# Set to 1 by -y/--yes to skip every interactive confirm() prompt.
ASSUME_YES=0

# confirm "action description" — prompt before any operation with externally
# visible side effects (push, PR create, draft release, QA dispatch).
# Returns 0 on Y/empty, non-zero on N. With -y/--yes the prompt is skipped
# and a one-line note is printed instead.
confirm() {
  local msg="$1"
  if (( ASSUME_YES )); then
    echo ">>> $msg [auto-confirmed via -y]"
    return 0
  fi
  # Read from /dev/tty so confirms still work when stdout is piped.
  printf '>>> %s [Y/n]: ' "$msg" >&2
  local ans=""
  if [[ -r /dev/tty ]]; then
    read -r ans </dev/tty || ans=""
  else
    read -r ans || ans=""
  fi
  case "$ans" in
    ''|y|Y|yes|Yes|YES) return 0 ;;
    *) echo ">>> Aborted at user prompt." >&2; return 1 ;;
  esac
}

# git_remote_branch_exists <branch> — 0 iff heads/$branch exists on weaviate/weaviate.
git_remote_branch_exists() {
  git ls-remote --exit-code --heads "https://github.com/${REPO}.git" "$1" >/dev/null 2>&1
}

# git_remote_tag_exists <tag> — 0 iff tags/$tag exists on weaviate/weaviate.
git_remote_tag_exists() {
  git ls-remote --exit-code --tags "https://github.com/${REPO}.git" "$1" >/dev/null 2>&1
}

# git_local_ref_exists <ref> — 0 iff $ref resolves locally (e.g. refs/remotes/origin/X, refs/tags/vX).
git_local_ref_exists() {
  git rev-parse --verify "$1" >/dev/null 2>&1
}

# gh_pr_for_branch <branch> — echoes the single PR JSON (number,url,state,mergeCommit,baseRefName)
# for $branch in weaviate/weaviate, or empty if no PR exists. Callers pipe through jq.
gh_pr_for_branch() {
  gh pr list --repo "$REPO" --head "$1" --state all --limit 1 \
    --json number,url,state,mergeCommit,baseRefName 2>/dev/null \
    | jq '.[0] // empty'
}

# gh_qa_issue_number <version> [state=open] — echoes the QA tracking issue number, or empty.
# Matches the title qa_pr.sh emits ("Release: v<version>") to avoid colliding with older
# issues that merely mention the same version string. Pass "all" to include closed issues.
gh_qa_issue_number() {
  local version="$1" state="${2:-open}"
  gh issue list --repo weaviate/weaviate-qa \
    --search "\"Release: v${version}\" in:title" --state "$state" --limit 1 \
    --json number --jq '.[0].number // empty' 2>/dev/null || true
}

# qa_issue_url_for <number> — echoes the canonical weaviate-qa issue URL.
qa_issue_url_for() {
  echo "https://github.com/weaviate/weaviate-qa/issues/$1"
}

# GraphQL query for the weaviate-qa project board fields. Used by infer_state (one-shot
# inspection) and cmd_monitor_qa (poll loop). Pass the issue number via -F n=…
# shellcheck disable=SC2016  # $n is a GraphQL variable, intentionally not expanded by the shell.
readonly GRAPHQL_BOARD_QUERY='
  query($n: Int!) {
    repository(owner: "weaviate", name: "weaviate-qa") {
      issue(number: $n) {
        projectItems(first: 10) {
          nodes {
            project { title }
            fieldValues(first: 30) {
              nodes {
                ... on ProjectV2ItemFieldSingleSelectValue {
                  name field { ... on ProjectV2SingleSelectField { name } }
                }
                ... on ProjectV2ItemFieldTextValue {
                  text field { ... on ProjectV2Field { name } }
                }
              }
            }
          }
        }
      }
    }
  }'

# _board_field <board_json> <single|text> <field_name>
# Extract one field from the "Central CI View" project board response. Echoes the
# single-select option name (single) or the text value (text), or empty if absent.
_board_field() {
  local board_json="$1" kind="$2" field_name="$3" jq_value
  case "$kind" in
    single) jq_value='.name' ;;
    text)   jq_value='.text' ;;
    *) echo "_board_field: unknown kind '$kind'" >&2; return 1 ;;
  esac
  jq -r --arg fn "$field_name" "
    .data.repository.issue.projectItems.nodes[]
    | select(.project.title==\"Central CI View\")
    | .fieldValues.nodes[]
    | select(.field != null and .field.name==\$fn)
    | $jq_value // empty" <<<"$board_json" | head -1
}

# In-memory state snapshot built by infer_state() when there's no journal to read.
# Schema mirrors the journal: {version, stable_branch, prep_branch, pr_number,
# pr_url, merge_sha, release_url, _inferred:true, steps:{<name>:{at,...}}}.
INFERRED_STATE_JSON=""

# _state_source — echoes the JSON to read state from, or empty string if no source.
# Journal file wins over inferred state; if both absent, callers default to empty.
_state_source() {
  if [[ -n "$STATE_FILE" && -f "$STATE_FILE" ]]; then
    cat "$STATE_FILE"
  elif [[ -n "$INFERRED_STATE_JSON" ]]; then
    printf '%s' "$INFERRED_STATE_JSON"
  fi
}

# state_get <jq-path> [default] — read a value from the active state source.
state_get() {
  local path="$1" default="${2:-}"
  local src; src=$(_state_source)
  if [[ -z "$src" ]]; then echo "$default"; return; fi
  jq -r --arg d "$default" "($path) // \$d" <<<"$src"
}

# ─── usage ────────────────────────────────────────────────────────────────────

usage() {
  cat <<'HELP'
Weaviate release driver — state machine for cutting a core release.
Run from the weaviate repo root. Delegates QA dispatch to tools/dev/qa_pr.sh.

USAGE
  release.sh [--journal] [-y|--yes] [command] [args...]

COMMANDS
  (no command, or 'auto')         Auto-detect current git branch (main →
                                  cut new stable; stable/vX.Y → next patch on
                                  that train) and resume from the last step.

  prepare    <ver>                Create prep branch + delegate schema.json
                                  bump, make deps, prepare_release.sh to
                                  tools/dev/create_release.sh; then push the
                                  branch and open the prepare-release PR.

  qa         <ver>                Dispatch the E2E + chaos QA matrix for the
                                  prepare-release PR via tools/dev/qa_pr.sh
                                  (which creates the QA tracking issue and
                                  adds it to project board #28).

  monitor-qa <ver>                Poll the "Central CI View" project board every
                                  5 min (2 h cap) until E2E and Chaos settle.
                                  Exits 0 (passed), 1 (failed), 2 (timeout).
                                  --journal lets re-runs short-circuit on the
                                  recorded result; stateless mode re-polls (but
                                  short-circuits via inference once QA settles).

  finalize   <ver> [<pr#>]        Full post-merge flow: verify PR merged, push
                                  tag, create draft release, wait for Docker Hub
                                  image, then verify release is published.
  finalize   <step> <ver> [<pr#>] Run a single finalize step independently:
                                    merge   — verify PR merged + ancestry check
                                    tag     — push local tag to remote
                                    draft   — create draft GitHub Release
                                    image   — check Docker Hub for release image
                                    publish — verify release published; mark done

  status     <ver>                Human-readable state summary: PR, QA issue,
                                  E2E/Chaos URLs, tag + release. Reads the
                                  journal file if --journal is on, otherwise
                                  re-infers state from GitHub on each call.

  reset      <ver>                Delete the journal file at
                                  _local/release/v<ver>.json (no-op in stateless
                                  mode — nothing on disk to remove).

  reset-step <ver> <step>         Wipe a single step from the journal so it
                                  re-runs on the next invocation. Useful for
                                  forcing QA re-dispatch:
                                    reset-step 1.36.13 qa_dispatched
                                  Requires a journal file (i.e. an earlier run
                                  with --journal).

  journals   [clone-path]         List in-progress and completed releases found
                                  in <clone-path>/_local/release/. Defaults to
                                  the git toplevel of $PWD.

  candidates [clone-path]         Show the expected next patch for each active
                                  stable/vX.Y branch (top 3 by version, plus
                                  the current stable/vX.Y branch if HEAD is on
                                  one). Defaults to the git toplevel of $PWD.

  verify     <stage> <ver>        Sanity-check GitHub state at a given stage.
                                  Stages: prepare | merge | finalize | publish

  help, -h, --help                Show this page.

FLOW
  stable/vX.Y branch  →  prepare  →  qa  →  [await merge]
                      →  finalize  →  done

  finalize runs five steps in order (each re-runnable independently):
    merge → tag → draft → image → publish

FLAGS
  --journal    Enable the release journal at _local/release/v<ver>.json.
               Without this flag the script runs stateless (no file written or read).
               If a journal file is found on disk but the flag was not passed, a
               warning is printed.

  -y, --yes    Skip the [Y/n] confirmation prompt before every operation with
               externally visible side effects (push prep branch, create PR,
               dispatch QA, push tag, create draft release, cut stable branch).
               Without this flag every such step prompts; an empty answer
               defaults to "yes". Read the prompt before pressing enter.

STATE SOURCES
  The script resolves "what's done" from one of two sources, in this order:

  1. Journal file at _local/release/v<ver>.json (only when --journal is passed).
     Every step is idempotent — re-running skips already-completed steps.

  2. Inferred state — when no journal is in play (or --journal is passed but
     the file doesn't exist yet), the script probes:
       • prep branch     →  git ls-remote prepare-release-v<ver>
       • PR + merge state →  gh pr list --head prepare-release-v<ver>
       • QA tracking issue →  gh issue list --repo weaviate/weaviate-qa
       • QA E2E/Chaos    →  project board "Central CI View" fields
       • tag             →  git ls-remote --tags v<ver>
       • release         →  gh release view v<ver>
     Inferred state lives in memory; nothing is written to disk. Pass --journal
     to persist (the journal will be seeded from the inferred state on first run).

  To force a single step to re-run from a journal:
    reset-step <ver> <step>

ENVIRONMENT
  QA_PR_SH          Override path to qa_pr.sh         (default: next to this script)
  CREATE_RELEASE_SH Override path to create_release.sh (default: next to this script)

EXAMPLES
  # Start or resume the next patch on the current stable branch:
  cd /path/to/weaviate && bash tools/dev/release.sh

  # Same, but auto-approve every confirm prompt (re-run after aborts):
  bash tools/dev/release.sh -y

  # Show what's been done for a release in flight (stateless inference):
  bash tools/dev/release.sh status 1.37.3

  # Same, but persist a journal so monitor-qa / reset-step work:
  bash tools/dev/release.sh --journal status 1.37.3

  # Poll QA until E2E + chaos settle (stateless — works, results not persisted):
  bash tools/dev/release.sh monitor-qa 1.37.3
  # Same, with --journal so re-runs after Ctrl-C short-circuit on the result:
  bash tools/dev/release.sh --journal monitor-qa 1.37.3

  # Force QA to re-dispatch without losing the rest of the journal:
  bash tools/dev/release.sh --journal reset-step 1.37.3 qa_dispatched
  bash tools/dev/release.sh --journal qa 1.37.3

  # Verify everything is in order before publishing:
  bash tools/dev/release.sh verify finalize 1.37.3

  # Run a single finalize step (e.g. after manual tag push):
  bash tools/dev/release.sh finalize tag 1.37.3
  bash tools/dev/release.sh finalize publish 1.37.3
HELP
}

# ─── state helpers (require $VERSION and $STATE_FILE) ─────────────────────────

state_init() {
  [[ -z "$STATE_FILE" ]] && return
  mkdir -p "$STATE_DIR"
  if [[ ! -f "$STATE_FILE" ]]; then
    # Seed from inferred state when --journal is passed but the file is missing —
    # so first-time journal use captures whatever already exists on GitHub.
    if [[ -n "$INFERRED_STATE_JSON" ]]; then
      jq 'del(._inferred)' <<<"$INFERRED_STATE_JSON" > "$STATE_FILE"
      echo ">>> State file created (seeded from inferred state): ${STATE_FILE}"
    else
      jq -n --arg v "$VERSION" --arg sb "$STABLE_BRANCH" --arg pb "$PREP_BRANCH" \
        '{version:$v, stable_branch:$sb, prep_branch:$pb, steps:{}}' > "$STATE_FILE"
      echo ">>> State file created: ${STATE_FILE}"
    fi
  else
    echo ">>> State file: ${STATE_FILE} (resuming)"
  fi
}

state_done() {
  local src; src=$(_state_source)
  [[ -z "$src" ]] && return 1
  jq -e --arg s "$1" '.steps[$s] != null' <<<"$src" >/dev/null 2>&1
}

# Writes to journal (if enabled) AND to INFERRED_STATE_JSON (if active), so
# intra-invocation reads after a write see the new value in either mode.
state_complete() {
  local step="$1"; shift
  local now; now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local jq_args=()
  for arg in "$@"; do jq_args+=(--arg "${arg%%=*}" "${arg#*=}"); done
  local extras; extras=$(jq -n "${jq_args[@]}" '$ARGS.named')

  if [[ -n "$STATE_FILE" && -f "$STATE_FILE" ]]; then
    local tmp; tmp="$(mktemp)"
    jq --arg s "$step" --arg at "$now" --argjson e "$extras" \
      '.steps[$s] = ($e + {at:$at})' "$STATE_FILE" > "$tmp"
    mv "$tmp" "$STATE_FILE"
  fi
  if [[ -n "$INFERRED_STATE_JSON" ]]; then
    INFERRED_STATE_JSON=$(jq --arg s "$step" --arg at "$now" --argjson e "$extras" \
      '.steps[$s] = ($e + {at:$at})' <<<"$INFERRED_STATE_JSON")
  fi
}

state_set() {
  if [[ -n "$STATE_FILE" && -f "$STATE_FILE" ]]; then
    local tmp; tmp="$(mktemp)"
    jq --arg k "$1" --arg v "$2" '. + {($k):$v}' "$STATE_FILE" > "$tmp"
    mv "$tmp" "$STATE_FILE"
  fi
  if [[ -n "$INFERRED_STATE_JSON" ]]; then
    INFERRED_STATE_JSON=$(jq --arg k "$1" --arg v "$2" '. + {($k):$v}' <<<"$INFERRED_STATE_JSON")
  fi
}

state_ensure() { state_done "$1" || state_complete "$@"; }

resolve_pr_number() {
  local n; n=$(state_get '.pr_number')
  [[ -n "$n" ]] || { echo "ERROR: cannot determine PR number for ${PREP_BRANCH}" >&2; exit 1; }
  echo "$n"
}

# ─── infer_state — populate INFERRED_STATE_JSON from GitHub + local repo ──────
#
# Called from init_release when no journal is being read. Probes each step's
# remote footprint with single targeted queries (≈5 gh/git calls). Failures
# degrade gracefully (treated as "step not done"). Schema matches the journal
# so state_done / state_get / cmd_status all work transparently.
#
# Caveats:
#   • docker_preview_built is NOT probed — treated as done if a PR exists. The
#     real check (PR CI status, docker tag in registry) is intentionally lax to
#     keep init_release fast and to avoid blocking on transient CI noise.
#   • Step timestamps are recorded as "inferred" rather than wall-clock times,
#     since we can't recover when the work was originally done.
# All _infer_* helpers below populate locals in infer_state via dynamic scope.
# Only _infer_build_state_json writes the script-global INFERRED_STATE_JSON.

# Sets pr_number, pr_url, pr_state, merge_sha from the latest PR for PREP_BRANCH.
_infer_pr_meta() {
  local pr_json; pr_json=$(gh_pr_for_branch "$PREP_BRANCH")
  [[ -n "$pr_json" ]] || return 0
  pr_number=$(jq -r '.number // empty' <<<"$pr_json")
  pr_url=$(jq -r '.url // empty'       <<<"$pr_json")
  pr_state=$(jq -r '.state // empty'   <<<"$pr_json")
  merge_sha=$(jq -r '.mergeCommit.oid // empty' <<<"$pr_json")
}

# Sets qa_issue_number and qa_issue_url (--state all to also catch closed
# issues from already-published releases).
_infer_qa_issue() {
  qa_issue_number=$(gh_qa_issue_number "$VERSION" all)
  [[ -n "$qa_issue_number" ]] && qa_issue_url=$(qa_issue_url_for "$qa_issue_number")
  return 0  # see comment at end of cmd_status: terminal [[ ]] && cmd short-circuits leak under set -e
}

# Sets qa_e2e, qa_chaos, qa_e2e_url, qa_chaos_url from the "Central CI View" board.
_infer_qa_board() {
  [[ -n "$qa_issue_number" ]] || return 0
  local board
  board=$(gh api graphql -F n="$qa_issue_number" -f query="$GRAPHQL_BOARD_QUERY" 2>/dev/null || true)
  [[ -n "$board" ]] && jq -e '.data.repository.issue' <<<"$board" >/dev/null 2>&1 || return 0
  qa_e2e=$(_board_field       "$board" single "E2E")
  qa_chaos=$(_board_field     "$board" single "Chaos")
  qa_e2e_url=$(_board_field   "$board" text   "E2E Job")
  qa_chaos_url=$(_board_field "$board" text   "Chaos Job")
}

# Sets rel_draft and rel_url from a draft or published release for v$VERSION.
_infer_release() {
  local rel_json
  rel_json=$(gh release view "v${VERSION}" --repo "$REPO" --json isDraft,url 2>/dev/null || true)
  [[ -n "$rel_json" ]] || return 0
  rel_draft=$(jq -r '.isDraft' <<<"$rel_json")
  rel_url=$(jq -r '.url'       <<<"$rel_json")
}

# Build INFERRED_STATE_JSON from the locals populated by the helpers above.
# This is the single writer of the script-global INFERRED_STATE_JSON.
_infer_build_state_json() {
  INFERRED_STATE_JSON=$(jq -n \
    --arg v   "$VERSION" \
    --arg sb  "$STABLE_BRANCH" \
    --arg pb  "$PREP_BRANCH" \
    --arg prep_exists "$prep_exists" \
    --arg pr_number   "$pr_number" \
    --arg pr_url      "$pr_url" \
    --arg pr_state    "$pr_state" \
    --arg merge_sha   "$merge_sha" \
    --arg qa_iss_num  "$qa_issue_number" \
    --arg qa_iss_url  "$qa_issue_url" \
    --arg qa_e2e      "$qa_e2e" \
    --arg qa_chaos    "$qa_chaos" \
    --arg qa_e2e_url  "$qa_e2e_url" \
    --arg qa_chaos_url "$qa_chaos_url" \
    --arg tag_exists  "$tag_exists" \
    --arg rel_draft   "$rel_draft" \
    --arg rel_url     "$rel_url" '
    {
      version:       $v,
      stable_branch: $sb,
      prep_branch:   $pb,
      pr_number:     ($pr_number | if . == "" then null else tonumber end),
      pr_url:        ($pr_url    | if . == "" then null else . end),
      merge_sha:     ($merge_sha | if . == "" then null else . end),
      release_url:   ($rel_url   | if . == "" then null else . end),
      _inferred:     true,
      steps: (
        {}
        + (if $prep_exists == "true"    then {branch_setup:      {at:"inferred", mode:"existing"}} else {} end)
        + (if $pr_number != ""          then {schema_bump:       {at:"inferred"},
                                              prepare_release_sh:{at:"inferred"},
                                              pr_create:         {at:"inferred", pr_number:($pr_number|tonumber), pr_url:$pr_url}} else {} end)
        + (if $pr_state == "MERGED"     then {merged:            {at:"inferred", sha:$merge_sha}} else {} end)
        + (if $qa_iss_num != ""         then {qa_dispatched:     {at:"inferred", issue_url:$qa_iss_url, dispatch_time:"inferred"}} else {} end)
        + (if ($qa_e2e_url != "" or $qa_chaos_url != "")
                                        then {qa_run_urls:       {at:"inferred", e2e:$qa_e2e_url, chaos:$qa_chaos_url}} else {} end)
        + (if (($qa_e2e == "Passed" or $qa_e2e == "Failed") and
               ($qa_chaos == "Passed" or $qa_chaos == "Failed"))
                                        then {qa_done:           {at:"inferred", e2e:$qa_e2e, chaos:$qa_chaos,
                                                                   result:(if $qa_e2e == "Passed" and $qa_chaos == "Passed" then "passed" else "failed" end)}} else {} end)
        + (if $tag_exists == "true"     then {push_tag:          {at:"inferred"}} else {} end)
        + (if $rel_url != ""            then {draft_release:     {at:"inferred", url:$rel_url}} else {} end)
        + (if $rel_draft == "false"     then {release_published: {at:"inferred", url:$rel_url}} else {} end)
      )
    }')
}

# Print a 1-5 line summary of what we found.
_infer_print_summary() {
  local _mode_note
  if (( JOURNAL_ENABLED )); then
    _mode_note="will seed journal at ${STATE_FILE}"
  else
    _mode_note="no journal in use"
  fi
  echo ">>> Inferred release state for v${VERSION} (${_mode_note})"
  [[ "$prep_exists" == "true" ]] && echo "    prep branch:  exists"
  [[ -n "$pr_number" ]]          && echo "    PR:           #${pr_number} ${pr_state} ${pr_url}"
  [[ -n "$qa_issue_number" ]]    && echo "    QA issue:     ${qa_issue_url} (E2E: ${qa_e2e:-—}  Chaos: ${qa_chaos:-—})"
  [[ "$tag_exists" == "true" ]]  && echo "    tag:          v${VERSION} pushed"
  [[ -n "$rel_url" ]]            && echo "    release:      ${rel_url} (draft=${rel_draft})"
  return 0  # ensure trailing `[[ ]] && echo` short-circuits don't bubble up under set -e
}

infer_state() {
  local pr_number="" pr_url="" pr_state="" merge_sha=""
  local prep_exists="false" tag_exists="false"
  local qa_issue_number="" qa_issue_url=""
  local qa_e2e="" qa_chaos="" qa_e2e_url="" qa_chaos_url=""
  local rel_draft="" rel_url=""

  _infer_pr_meta
  git_remote_branch_exists "$PREP_BRANCH" && prep_exists="true"
  _infer_qa_issue
  _infer_qa_board
  git_remote_tag_exists "v${VERSION}" && tag_exists="true"
  _infer_release
  _infer_build_state_json
  _infer_print_summary
}

# ─── read-only subcommands ────────────────────────────────────────────────────

cmd_reset() {
  [[ -n "${1:-}" ]] || { echo "Usage: $0 reset <version>" >&2; exit 1; }
  local f="${STATE_DIR}/v${1}.json"
  if [[ -f "$f" ]]; then rm "$f" && echo ">>> Removed $f"; else echo ">>> No state file at $f"; fi
}

cmd_reset_step() {
  [[ -n "${1:-}" && -n "${2:-}" ]] || {
    echo "Usage: $0 reset-step <version> <step>" >&2; exit 1; }
  local f="${STATE_DIR}/v${1}.json"
  local step="$2"
  [[ -f "$f" ]] || { echo "ERROR: no state file at $f" >&2; exit 1; }
  if ! jq -e --arg s "$step" '.steps[$s] != null' "$f" >/dev/null 2>&1; then
    echo ">>> Step '$step' not present in $f — nothing to reset"
    echo ">>> Steps currently recorded: $(jq -r '.steps | keys | join(", ")' "$f")"
    return 0
  fi
  local tmp; tmp="$(mktemp)"
  jq --arg s "$step" 'del(.steps[$s])' "$f" > "$tmp"
  mv "$tmp" "$f"
  echo ">>> Wiped .steps.$step from $f"
  echo ">>> Re-run release.sh to redo that step."
}

cmd_status() {
  [[ -n "${1:-}" ]] || { echo "Usage: $0 status <version>" >&2; exit 1; }
  init_release "$1"  # populates STATE_FILE and/or INFERRED_STATE_JSON

  local source_label
  if [[ -n "$STATE_FILE" && -f "$STATE_FILE" ]]; then
    source_label="$STATE_FILE"
  else
    source_label="(inferred from GitHub — no journal; pass --journal to persist)"
  fi

  local stable prep pr_number pr_url release_url merge_sha
  stable=$(state_get '.stable_branch' "—")
  prep=$(state_get '.prep_branch' "—")
  pr_number=$(state_get '.pr_number' "—")
  pr_url=$(state_get '.pr_url' "—")
  release_url=$(state_get '.release_url' "—")
  merge_sha=$(state_get '.merge_sha' "—")

  local audit qa_done qa_e2e qa_chaos qa_dispatch qa_issue_url qa_e2e_run qa_chaos_run
  audit=$(state_get '.steps.merge_forward_audit.status' "—")
  qa_dispatch=$(state_get '.steps.qa_dispatched.dispatch_time' "—")
  qa_issue_url=$(state_get '.steps.qa_dispatched.issue_url' "")
  qa_done=$(state_get '.steps.qa_done.result' "(not yet settled)")
  qa_e2e=$(state_get '.steps.qa_done.e2e' "—")
  qa_chaos=$(state_get '.steps.qa_done.chaos' "—")
  qa_e2e_run=$(state_get '.steps.qa_run_urls.e2e' "")
  qa_chaos_run=$(state_get '.steps.qa_run_urls.chaos' "")

  echo "═══════════════════════════════════════════════════════════"
  echo "  Release v${VERSION}"
  echo "═══════════════════════════════════════════════════════════"
  echo "  Source:        $source_label"
  echo "  Stable branch: $stable"
  echo "  Prep branch:   $prep"
  echo "  PR:            #${pr_number}  $pr_url"
  echo "  Merge SHA:     $merge_sha"
  echo "  Release:       $release_url"
  echo ""
  echo "  Merge-forward audit: $audit"
  echo "  QA dispatched at:    $qa_dispatch"
  [[ -n "$qa_issue_url" ]] && echo "  QA issue:            $qa_issue_url"
  echo "  QA result:           $qa_done  (E2E: $qa_e2e  Chaos: $qa_chaos)"
  [[ -n "$qa_e2e_run"   ]] && echo "  E2E run:             $qa_e2e_run"
  [[ -n "$qa_chaos_run" ]] && echo "  Chaos run:           $qa_chaos_run"
  echo ""
  echo "  Completed steps:"
  local src; src=$(_state_source)
  if [[ -n "$src" ]]; then
    jq -r '.steps | to_entries[] | "    • \(.key)  \(.value.at // "")"' <<<"$src"
  else
    echo "    (none)"
  fi
  echo "═══════════════════════════════════════════════════════════"
}

# Resolve <clone-path> arg. With no arg, defaults to the toplevel of the git
# repo containing $PWD. With an arg, normalizes to that repo's toplevel too,
# so callers passing a subdirectory still get path concatenations right
# (notably cmd_journals' "${CLONE}/_local/release"). Fails fast with a
# helpful error if the path isn't a git working tree.
resolve_clone_path() {
  local START="${1:-$PWD}"
  local TOPLEVEL
  if ! TOPLEVEL=$(git -C "$START" rev-parse --show-toplevel 2>/dev/null); then
    echo "ERROR: '$START' is not inside a git working tree." >&2
    echo "       Run from inside a weaviate clone, or pass <clone-path>." >&2
    exit 1
  fi
  printf '%s\n' "$TOPLEVEL"
}

cmd_journals() {
  local CLONE; CLONE=$(resolve_clone_path "${1:-}")
  local JDIR="${CLONE}/_local/release"
  if [[ ! -d "$JDIR" ]]; then echo "No release in progress."; return; fi
  local JOURNALS; JOURNALS=$(find "$JDIR" -maxdepth 1 -name 'v*.json' -type f | sort)
  if [[ -z "$JOURNALS" ]]; then echo "No release in progress."; return; fi
  while IFS= read -r j; do
    local v rel pub steps
    v=$(jq -r '.version' "$j")
    rel=$(jq -r '.release_url // empty' "$j")
    pub=$(jq -r '.steps.release_published // empty' "$j")
    steps=$(jq -r '.steps | keys | join(", ")' "$j")
    if [[ -n "$pub" ]]; then echo "✅ v$v — completed ($rel)"
    else echo "🟡 v$v — in progress (steps done: $steps)"; fi
  done <<< "$JOURNALS"
}

cmd_candidates() {
  local CLONE; CLONE=$(resolve_clone_path "${1:-}")
  local TOP_MINORS; TOP_MINORS=$(git -C "$CLONE" branch -r --list 'origin/stable/v*' \
      | sed 's|.*origin/stable/v||' | sort -V | tail -3)

  # Driven by the largest *remote* minor — captured before mixing in CUR_XY
  # so an older current branch can't drag the "next new minor" backwards.
  local largest_xy; largest_xy=$(echo "$TOP_MINORS" | tail -1)

  # If HEAD is on stable/vX.Y, include its next-patch row even when outside
  # the top-3 remote minors; mark it for the reader.
  local CUR_BRANCH CUR_XY=""
  CUR_BRANCH=$(git -C "$CLONE" rev-parse --abbrev-ref HEAD 2>/dev/null || true)
  if [[ "$CUR_BRANCH" =~ ^stable/v([0-9]+)\.([0-9]+)$ ]]; then
    CUR_XY="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
  fi

  local ALL_MINORS
  ALL_MINORS=$(printf '%s\n%s\n' "$TOP_MINORS" "$CUR_XY" \
    | grep -v '^$' | sort -V -u)

  while IFS= read -r xy; do
    [[ -z "$xy" ]] && continue
    # Ignore pre-release tags (v1.36.14-rc.1) when computing the next patch number —
    # they confuse ${latest##*.} arithmetic and aren't release tags anyway.
    # `|| true` keeps `set -euo pipefail` from aborting when grep finds no
    # release tag (e.g. a freshly-cut stable/vX.Y branch with no patches yet).
    local latest; latest=$(git -C "$CLONE" tag --list "v${xy}.*" --sort=-v:refname \
        | grep -E "^v${xy//./\\.}\.[0-9]+$" | head -1 || true)
    if [[ -n "$latest" ]]; then
      local marker=""
      [[ "$xy" == "$CUR_XY" ]] && marker="  (current branch)"
      printf "v%s.%d  next patch on %s (latest: %s)%s\n" \
        "$xy" "$(( ${latest##*.} + 1 ))" "$xy" "$latest" "$marker"
    fi
  done <<< "$ALL_MINORS"

  if [[ -n "$largest_xy" ]]; then
    local major=${largest_xy%%.*}
    local next_minor=$(( ${largest_xy##*.} + 1 ))
    printf "v%s.%d.0  start a new minor train (cut stable/v%s.%d from main first)\n" \
      "$major" "$next_minor" "$major" "$next_minor"
  fi
}

cmd_verify() {
  [[ -n "${1:-}" && -n "${2:-}" ]] || {
    echo "Usage: $0 verify <stage> <version>  (prepare|merge|finalize|publish)" >&2; exit 1; }
  local V_STAGE="$1" V_VERSION="$2"
  if [[ ! "$V_VERSION" =~ $VERSION_RE ]]; then
    echo "ERROR: invalid version '$V_VERSION'" >&2; exit 1
  fi
  local V_MAJOR="${V_VERSION%%.*}"
  local V_REST="${V_VERSION#*.}"
  local V_MINOR="${V_REST%%.*}"
  local V_STABLE="stable/v${V_MAJOR}.${V_MINOR}"
  local V_PREP="prepare-release-v${V_VERSION}"
  local V_TAG="v${V_VERSION}"
  local V_FAIL=0

  check_branch() {
    if git_remote_branch_exists "$V_PREP"; then
      echo "✅ prep branch $V_PREP exists on $REPO"
    else
      echo "❌ prep branch $V_PREP missing on $REPO"; V_FAIL=1
    fi
  }

  check_pr_state() {
    local want="$1" pr_json pr_num pr_state pr_base
    pr_json=$(gh_pr_for_branch "$V_PREP")
    if [[ -z "$pr_json" ]]; then
      echo "❌ no PR found for $V_PREP in $REPO"; V_FAIL=1; return
    fi
    pr_num=$(jq -r '.number'       <<<"$pr_json")
    pr_state=$(jq -r '.state'      <<<"$pr_json")
    pr_base=$(jq -r '.baseRefName' <<<"$pr_json")
    if [[ "$pr_base" != "$V_STABLE" ]]; then
      echo "❌ PR #$pr_num targets '$pr_base', expected '$V_STABLE'"; V_FAIL=1
    fi
    if [[ "$pr_state" != "$want" ]]; then
      echo "❌ PR #$pr_num state=$pr_state, expected $want"; V_FAIL=1
    else
      echo "✅ PR #$pr_num state=$pr_state base=$pr_base"
    fi
  }

  check_tag() {
    if git_remote_tag_exists "$V_TAG"; then
      echo "✅ tag $V_TAG exists on $REPO"
    else
      echo "❌ tag $V_TAG missing on $REPO"; V_FAIL=1
    fi
  }

  check_release() {
    local want_draft="$1" rel_json is_draft url
    rel_json=$(gh release view "$V_TAG" --repo "$REPO" --json isDraft,url 2>/dev/null || true)
    if [[ -z "$rel_json" ]]; then
      echo "❌ release $V_TAG not found in $REPO"; V_FAIL=1; return
    fi
    is_draft=$(jq -r '.isDraft' <<<"$rel_json")
    url=$(jq -r '.url'          <<<"$rel_json")
    if [[ "$is_draft" != "$want_draft" ]]; then
      echo "❌ release $V_TAG isDraft=$is_draft, expected $want_draft ($url)"; V_FAIL=1
    else
      local label; [[ "$is_draft" == "true" ]] && label="draft" || label="published"
      echo "✅ release $V_TAG $label ($url)"
    fi
  }

  case "$V_STAGE" in
    prepare)  check_branch; check_pr_state "OPEN" ;;
    merge)    check_pr_state "MERGED" ;;
    finalize) check_tag; check_release "true" ;;
    publish)  check_tag; check_release "false" ;;
    *) echo "ERROR: unknown stage '$V_STAGE' (want: prepare|merge|finalize|publish)" >&2; exit 1 ;;
  esac
  return "$V_FAIL"
}

# ─── init_release: set VERSION-derived globals ────────────────────────────────

# Global release-context vars (set by init_release or cmd_auto)
VERSION="" MAJOR="" MINOR_VER="" STABLE_BRANCH="" PREP_BRANCH="" STATE_FILE=""

init_release() {
  VERSION="$1"
  if [[ ! "$VERSION" =~ $VERSION_RE ]]; then
    echo "ERROR: invalid version '$VERSION' (expected X.Y.Z or X.Y.Z-rc.N)" >&2; exit 1
  fi
  MAJOR="${VERSION%%.*}"
  local rest="${VERSION#*.}"
  MINOR_VER="${rest%%.*}"
  STABLE_BRANCH="stable/v${MAJOR}.${MINOR_VER}"
  PREP_BRANCH="prepare-release-v${VERSION}"

  # State source resolution. Inference runs whenever there's no journal to read:
  #   • stateless mode (no --journal)         → infer
  #   • --journal passed, file doesn't exist  → infer, then state_init seeds it
  local default_journal="${STATE_DIR}/v${VERSION}.json"
  local need_infer=0
  if (( JOURNAL_ENABLED )); then
    STATE_FILE="$default_journal"
    [[ -f "$STATE_FILE" ]] || need_infer=1
  else
    STATE_FILE=""
    need_infer=1
    if [[ -f "$default_journal" ]]; then
      echo "⚠️  Journal file found at ${default_journal} — did you forget --journal?" >&2
    fi
  fi
  if (( need_infer )); then infer_state; fi
  # When --journal is on, materialize the file now (seeded from inference if new)
  # so it exists for any subsequent state_complete/state_set writes, regardless
  # of which subcommand was invoked.
  if (( JOURNAL_ENABLED )); then state_init; fi
}

# Repo-root guard. Called by commands that touch the working tree
# (prepare/qa/finalize). Read-only commands skip it so they can run anywhere.
require_repo_root() {
  [[ -f "$SPEC" ]] || { echo "ERROR: $SPEC not found — run from the weaviate repo root." >&2; exit 1; }
}

# ─── cmd_prepare ─────────────────────────────────────────────────────────────

# Merge-forward audit — informational, never gates.
_prepare_audit_merge_forward() {
  local PREV_MINOR=$(( MINOR_VER - 1 ))
  local PREV_STABLE="stable/v${MAJOR}.${PREV_MINOR}"
  echo ""
  echo ">>> Merge-forward audit: ${PREV_STABLE} → ${STABLE_BRANCH}"
  if ! git_local_ref_exists "refs/remotes/origin/${PREV_STABLE}"; then
    echo "    (no ${PREV_STABLE} on remote — skipping)"
    return
  fi
  local COUNT; COUNT=$(git log --oneline "origin/${PREV_STABLE}" ^"origin/${STABLE_BRANCH}" | wc -l | tr -d ' ')
  local AUDIT_STATUS; AUDIT_STATUS=$([[ "$COUNT" -eq 0 ]] && echo "In Sync" || echo "Needs Attention")
  echo "    ${PREV_STABLE} → ${STABLE_BRANCH}: ${AUDIT_STATUS}"
  state_complete merge_forward_audit status="$AUDIT_STATUS"
}

# Get onto PREP_BRANCH. Three modes:
#   recorded → state already knows about the branch; just check it out
#   existing → branch exists on remote; check out and warn on local/remote drift
#   new      → cut a fresh branch off STABLE_BRANCH
_prepare_setup_branch() {
  echo ""
  echo ">>> Preparing branch ${PREP_BRANCH}"
  if state_done branch_setup; then
    echo "    Branch setup already recorded — checking out ${PREP_BRANCH}"
    git checkout "${PREP_BRANCH}"
    return
  fi
  if git_local_ref_exists "refs/remotes/origin/${PREP_BRANCH}"; then
    echo "    Branch already on remote — checking out"
    git checkout "${PREP_BRANCH}"
    local LOCAL_SHA REMOTE_SHA
    LOCAL_SHA=$(git rev-parse HEAD)
    REMOTE_SHA=$(git rev-parse "refs/remotes/origin/${PREP_BRANCH}")
    if [[ "$LOCAL_SHA" != "$REMOTE_SHA" ]]; then
      echo "    ⚠️  Local ${PREP_BRANCH} (${LOCAL_SHA:0:8}) differs from remote (${REMOTE_SHA:0:8})."
      echo "       Reconcile manually before continuing."
    fi
    state_complete branch_setup mode=existing
    return
  fi
  git checkout "${STABLE_BRANCH}"
  git pull -q
  git checkout -b "${PREP_BRANCH}"
  state_complete branch_setup mode=new
}

# Delegate the prep core (schema.json bump + make deps + prepare_release.sh)
# to create_release.sh. We're already on PREP_BRANCH, so no --branch flag —
# create_release.sh just runs the in-place mechanics.
_prepare_run_create_release() {
  if git rev-parse "v${VERSION}" &>/dev/null 2>&1; then
    echo ">>> Tag v${VERSION} already exists locally — skipping create_release.sh"
    state_ensure schema_bump tag=preexisting
    state_ensure prepare_release_sh tag=preexisting
    return
  fi
  [[ -x "$CREATE_RELEASE_SH" || -f "$CREATE_RELEASE_SH" ]] || {
    echo "ERROR: create_release.sh not found at $CREATE_RELEASE_SH" >&2; exit 1; }
  local CURRENT; CURRENT="$(jq -r '.info.version' "$SPEC")"
  echo ">>> Delegating prepare core to $CREATE_RELEASE_SH"
  bash "$CREATE_RELEASE_SH" "${VERSION}"
  # Verify the on-disk bump landed before recording state. create_release.sh
  # is deterministic, but a stale checkout or jq failure would leave us
  # claiming success on an unchanged schema.json.
  local NEW_VER; NEW_VER=$(jq -r '.info.version' "$SPEC")
  if [[ "$NEW_VER" != "$VERSION" ]]; then
    echo "ERROR: schema.json bump failed — expected ${VERSION}, got ${NEW_VER}." >&2
    exit 1
  fi
  state_complete schema_bump from="$CURRENT" to="$VERSION"
  echo ""
  echo "    ⚠️  Tag v${VERSION} created LOCALLY. Do not push it yet."
  state_complete prepare_release_sh
}

_prepare_push_branch() {
  confirm "Push ${PREP_BRANCH} to origin?" || exit 1
  echo ">>> Pushing ${PREP_BRANCH}"
  git push -u origin "${PREP_BRANCH}"
  state_complete branch_push
}

# Sets PR_NUMBER and PR_URL (inherited from cmd_prepare's locals via dynamic scope).
# Idempotent: if a PR already exists for PREP_BRANCH, reuse it instead of creating a duplicate.
_prepare_open_or_reuse_pr() {
  local EXISTING_PR; EXISTING_PR=$(gh_pr_for_branch "${PREP_BRANCH}" | jq -r '.number // ""')
  if [[ -n "$EXISTING_PR" ]]; then
    PR_NUMBER="$EXISTING_PR"
    PR_URL="https://github.com/${REPO}/pull/${EXISTING_PR}"
    echo ">>> PR already exists: ${PR_URL}"
  else
    confirm "Create prepare-release PR on ${REPO} (base: ${STABLE_BRANCH})?" || exit 1
    echo ">>> Creating PR"
    local PR_BODY; PR_BODY=$(mktemp)
    awk -v v="$VERSION" '
      /^### What.s being changed:/ { print; print ""; print "prepare release v" v; next }
      { print }
    ' .github/PULL_REQUEST_TEMPLATE.md > "$PR_BODY"
    # The URL is on the last line of `gh pr create` output.
    PR_URL=$(gh pr create \
      --repo "$REPO" \
      --title "prepare release v${VERSION}" \
      --base "${STABLE_BRANCH}" \
      --body-file "$PR_BODY" | tail -1)
    rm -f "$PR_BODY"
    PR_NUMBER="${PR_URL##*/}"
    echo ">>> PR created: ${PR_URL}"
  fi
  state_set pr_number "$PR_NUMBER"
  state_set pr_url    "$PR_URL"
  state_ensure pr_create
}

cmd_prepare() {
  require_repo_root
  echo ">>> Release: v${VERSION}  branch: ${STABLE_BRANCH}"
  # journal file (if --journal) is already created by init_release
  git fetch --all --prune --tags -q

  local PR_NUMBER="" PR_URL=""
  _prepare_audit_merge_forward
  _prepare_setup_branch
  _prepare_run_create_release
  _prepare_push_branch
  _prepare_open_or_reuse_pr

  gate_banner "GATE 1 — prep branch and PR ready" \
    "PR: ${PR_URL}" \
    "Re-run this script to poll the docker build and dispatch QA."
}

# ─── cmd_qa: delegate to tools/dev/qa_pr.sh ───────────────────────────────────

cmd_qa() {
  require_repo_root
  local PR_NUMBER; PR_NUMBER=$(resolve_pr_number)
  local PR_URL="https://github.com/${REPO}/pull/${PR_NUMBER}"

  if state_done qa_dispatched; then
    local DISPATCH_TIME; DISPATCH_TIME=$(state_get '.steps.qa_dispatched.dispatch_time')
    echo ">>> QA already dispatched at ${DISPATCH_TIME} — skipping"
    echo ">>>   PR:    ${PR_URL}"
    echo ">>>   Run 'monitor-qa ${VERSION}' to check QA status."
    echo ">>> To force a re-dispatch: bash $0 reset-step ${VERSION} qa_dispatched"
    return 0
  fi

  [[ -x "$QA_PR_SH" || -f "$QA_PR_SH" ]] || {
    echo "ERROR: qa_pr.sh not found at $QA_PR_SH" >&2; exit 1; }

  confirm "Dispatch QA pipeline (e2e + chaos) for PR #${PR_NUMBER}?" || exit 1
  echo ">>> Step: Dispatching QA for PR #${PR_NUMBER} via tools/dev/qa_pr.sh"
  if ! bash "$QA_PR_SH" "$PR_NUMBER"; then
    echo ">>> qa_pr.sh failed; state not advanced." >&2
    return 1
  fi

  # Look up the just-created QA tracking issue so cmd_status can surface its URL.
  local QA_ISSUE_NUMBER QA_ISSUE_URL=""
  QA_ISSUE_NUMBER=$(gh_qa_issue_number "$VERSION")
  [[ -n "$QA_ISSUE_NUMBER" ]] && QA_ISSUE_URL=$(qa_issue_url_for "$QA_ISSUE_NUMBER")

  state_complete qa_dispatched \
    dispatch_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    issue_url="$QA_ISSUE_URL"

  gate_banner "GATE 1 — QA dispatched" \
    "PR:    ${PR_URL}" \
    "Next: merge the PR (Create a merge commit) when CI + QA + review are green," \
    "      then re-run this script."
}

# ─── finalize steps ───────────────────────────────────────────────────────────

_finalize_merge() {
  local PR_NUMBER="${1:-}"
  [[ -n "$PR_NUMBER" ]] || PR_NUMBER=$(resolve_pr_number)

  echo ">>> Step merge: verifying PR #${PR_NUMBER}"
  state_init

  local PR_JSON; PR_JSON=$(gh pr view "$PR_NUMBER" --repo "$REPO" --json state,mergeCommit)
  local PR_STATE; PR_STATE=$(jq -r '.state' <<<"$PR_JSON")
  if [[ "$PR_STATE" != "MERGED" ]]; then
    echo "GATE: PR #${PR_NUMBER} is not merged yet (state: ${PR_STATE})."
    exit 2
  fi
  local MERGE_SHA; MERGE_SHA=$(jq -r '.mergeCommit.oid' <<<"$PR_JSON")
  echo "    ✅ PR #${PR_NUMBER} merged"

  git fetch --all --tags -q

  if state_done merge_ancestor_check; then
    echo "    Merge ancestry check already passed — skipping"
  else
    if ! git merge-base --is-ancestor "$MERGE_SHA" "origin/${STABLE_BRANCH}" 2>/dev/null; then
      echo "ERROR: merge commit ${MERGE_SHA:0:12} not found in ${STABLE_BRANCH} — investigate." >&2
      exit 2
    fi
    echo "    ✅ merge commit ${MERGE_SHA:0:12} is in ${STABLE_BRANCH}"
    state_complete merge_ancestor_check
  fi
  state_set merge_sha "$MERGE_SHA"
  state_ensure pr_merged
}

_finalize_tag() {
  echo ">>> Step tag: pushing v${VERSION}"
  if git_local_ref_exists "refs/tags/v${VERSION}" && \
     git ls-remote --exit-code --tags origin "refs/tags/v${VERSION}" &>/dev/null; then
    echo "    Tag v${VERSION} already on remote — skipping"
    state_ensure tag_push tag=preexisting
  else
    confirm "Push tag v${VERSION} to origin? (public, triggers release CI)" || exit 1
    git push origin "v${VERSION}"
    state_complete tag_push
    echo "    ✅ Tag v${VERSION} pushed"
  fi
}

_finalize_draft() {
  echo ">>> Step draft: creating GitHub Release"
  if state_done draft_release; then
    local RELEASE_URL; RELEASE_URL=$(state_get '.release_url')
    echo "    Draft release already created: ${RELEASE_URL}"
    return 0
  fi

  git fetch --tags -q 2>/dev/null || echo "    ⚠️  git fetch --tags failed; using local refs" >&2
  local PREV_TAG; PREV_TAG=$(git tag --list "v${MAJOR}.${MINOR_VER}.*" --sort=-v:refname \
    | grep -vFx "v${VERSION}" | head -1 || true)
  if [[ -z "$PREV_TAG" ]]; then
    PREV_TAG="stable/v${MAJOR}.$(( MINOR_VER - 1 ))"
    echo "    No prior patch on v${MAJOR}.${MINOR_VER} — using ${PREV_TAG} as the comparison base"
  fi
  echo "    Generating changeset from ${PREV_TAG} to v${VERSION}"
  # Drop release-housekeeping commits (subjects starting with "prepare release" or "merge stable").
  local CHANGESET; CHANGESET=$(git log "${PREV_TAG}..v${VERSION}" --oneline --no-merges \
    | { grep -Ev '^[0-9a-f]+ (prepare release|merge stable)' || true; } \
    | sed 's/^/- /')
  [[ -z "$CHANGESET" ]] && CHANGESET="*(no user-visible changes)*"

  local NOTES; NOTES="$(cat <<NOTES_EOF
## Breaking Changes
*none*

## New Features
*none*

## Fixes
${CHANGESET}

**Full Changelog**: https://github.com/${REPO}/compare/${PREV_TAG}...v${VERSION}
NOTES_EOF
)"
  confirm "Create draft GitHub release for v${VERSION} on ${REPO}?" || exit 1
  local RELEASE_URL; RELEASE_URL=$(gh release create "v${VERSION}" \
    --repo "$REPO" --title "v${VERSION}" --notes "$NOTES" --draft --verify-tag)
  state_set release_url "$RELEASE_URL"
  state_complete draft_release
  echo "    ✅ Draft release: ${RELEASE_URL}"
}

_finalize_image() {
  echo ">>> Step image: checking Docker Hub"
  if state_done docker_hub; then
    echo "    Docker Hub check already passed — skipping"
    return 0
  fi
  local HTTP; HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    "https://hub.docker.com/v2/repositories/semitechnologies/weaviate/tags/${VERSION}/")
  if [[ "$HTTP" != "200" ]]; then
    echo "GATE: semitechnologies/weaviate:${VERSION} not on Docker Hub yet (HTTP ${HTTP})."
    echo "      Edit release notes in the draft while you wait, then re-run."
    exit 2
  fi
  echo "    ✅ Image semitechnologies/weaviate:${VERSION} found"
  state_complete docker_hub
}

_finalize_publish() {
  echo ">>> Step publish: verifying release is published"
  if state_done release_published; then
    local url; url=$(state_get '.steps.release_published.url')
    echo "    Release already marked published: ${url}"
    return 0
  fi

  local rel_json is_draft url
  rel_json=$(gh release view "v${VERSION}" --repo "$REPO" --json isDraft,url 2>/dev/null || true)
  if [[ -z "$rel_json" ]]; then
    echo "ERROR: release v${VERSION} not found on GitHub." >&2; exit 1
  fi
  is_draft=$(jq -r '.isDraft' <<<"$rel_json")
  url=$(jq -r '.url'     <<<"$rel_json")

  if [[ "$is_draft" == "true" ]]; then
    gate_banner "GATE — release is still a draft:" \
      "${url}" \
      "1. Edit release notes (if not done already)" \
      "2. Set 'latest' if this is the newest minor" \
      "3. Publish" \
      "Then re-run: bash tools/dev/release.sh finalize publish ${VERSION}"
    exit 2
  fi

  state_complete release_published url="$url"
  echo "    ✅ Release published: ${url}"
  echo ""
  echo "✅ Release v${VERSION} is complete."
}

# ─── cmd_finalize ─────────────────────────────────────────────────────────────

# Finalize sub-steps in the order the full flow runs them.
# Single source of truth: changing the order here changes both the default
# full-flow order and the validated set of single-step names.
FINALIZE_STEPS=(merge tag draft image publish)
declare -A FINALIZE_HANDLERS=(
  [merge]=_finalize_merge
  [tag]=_finalize_tag
  [draft]=_finalize_draft
  [image]=_finalize_image
  [publish]=_finalize_publish
)

cmd_finalize() {
  require_repo_root
  local STEP="${1:-}"
  local PR_ARG="${2:-}"

  # Only _finalize_merge reads $PR_ARG; the other handlers ignore extras.
  if [[ -z "$STEP" ]]; then
    echo ">>> Finalizing v${VERSION}"
    local s
    for s in "${FINALIZE_STEPS[@]}"; do
      "${FINALIZE_HANDLERS[$s]}" "$PR_ARG"
    done
    return
  fi

  local fn="${FINALIZE_HANDLERS[$STEP]:-}"
  [[ -n "$fn" ]] || { echo "ERROR: unknown finalize step '${STEP}' (${FINALIZE_STEPS[*]})" >&2; exit 1; }
  "$fn" "$PR_ARG"
}

# ─── cmd_monitor_qa ──────────────────────────────────────────────────────────

# monitor-qa works in both modes:
#   • --journal:  qa_done + qa_run_urls writes persist; re-runs after Ctrl-C
#                 short-circuit on the recorded result.
#   • stateless:  writes go to in-memory inferred state only and are discarded
#                 at exit. Re-runs re-poll from scratch — but if QA settled in
#                 the meantime, infer_state catches it and short-circuits.
# init_release already ensures STATE_FILE exists when --journal is set, so this
# helper just announces the trade-off when running stateless.
_monitor_qa_announce_mode() {
  (( JOURNAL_ENABLED )) && return
  echo "    ℹ️  Stateless mode — results are reported live but not persisted."
  echo "       Use --journal monitor-qa ${VERSION} to short-circuit on re-run."
}

# Sets QA_ISSUE_NUMBER and QA_ISSUE_URL (inherited from cmd_monitor_qa via dynamic scope).
# Exits 1 if no open issue exists — that means QA was never dispatched.
_monitor_qa_resolve_issue() {
  QA_ISSUE_NUMBER=$(gh_qa_issue_number "$VERSION")
  [[ -n "$QA_ISSUE_NUMBER" ]] || {
    echo "ERROR: no open QA issue for v${VERSION} in weaviate/weaviate-qa — dispatch QA first." >&2
    exit 1; }
  QA_ISSUE_URL=$(qa_issue_url_for "$QA_ISSUE_NUMBER")
}

# If qa_done was already recorded (re-run after a prior settle), short-circuit
# without polling. Returns:
#   0 → already settled, passed
#   1 → already settled, failed
#   2 → not settled yet; caller must run the poll loop
_monitor_qa_short_circuit_if_done() {
  state_done qa_done || return 2
  local result; result=$(state_get '.steps.qa_done.result')
  echo ">>> QA already settled: ${result}"
  [[ "$result" == "passed" ]]
}

# One iteration of the poll loop. Echoes a status line on success.
# Returns:
#   0 → continue polling (matched terminal state or unfinished but no error)
#   2 → terminal Passed
#   3 → terminal Failed
# Sets E2E_RUN_URL / CHAOS_RUN_URL / urls_saved in caller scope (dynamic scope).
_monitor_qa_poll_once() {
  local board_data
  if ! board_data=$(gh api graphql -F n="$QA_ISSUE_NUMBER" -f query="$GRAPHQL_BOARD_QUERY" 2>&1); then
    echo "    ⚠️  graphql call failed: ${board_data:0:200}" >&2
    return 0
  fi
  if ! jq -e '.data.repository.issue' <<<"$board_data" >/dev/null 2>&1; then
    echo "    ⚠️  unexpected graphql response shape — skipping cycle (first 200 chars: ${board_data:0:200})" >&2
    return 0
  fi

  local matched; matched=$(jq -r '
    [.data.repository.issue.projectItems.nodes[] | select(.project.title=="Central CI View")]
    | length' <<<"$board_data")
  if [[ "$matched" == "0" ]]; then
    echo "    ⚠️  issue not yet linked to 'Central CI View' — waiting" >&2
  fi

  local e2e_status chaos_status e2e_url chaos_url
  e2e_status=$(_board_field   "$board_data" single "E2E")
  chaos_status=$(_board_field "$board_data" single "Chaos")
  e2e_url=$(_board_field      "$board_data" text   "E2E Job")
  chaos_url=$(_board_field    "$board_data" text   "Chaos Job")
  e2e_status="${e2e_status:-—}"
  chaos_status="${chaos_status:-—}"
  [[ -n "$e2e_url"   ]] && E2E_RUN_URL="$e2e_url"
  [[ -n "$chaos_url" ]] && CHAOS_RUN_URL="$chaos_url"
  if (( ! urls_saved )) && [[ -n "$E2E_RUN_URL" || -n "$CHAOS_RUN_URL" ]]; then
    state_complete qa_run_urls e2e="${E2E_RUN_URL:-}" chaos="${CHAOS_RUN_URL:-}"
    [[ -n "$E2E_RUN_URL"   ]] && echo "    E2E:   ${E2E_RUN_URL}"
    [[ -n "$CHAOS_RUN_URL" ]] && echo "    Chaos: ${CHAOS_RUN_URL}"
    urls_saved=1
  fi

  echo "    [$(date '+%H:%M')] E2E: ${e2e_status}  Chaos: ${chaos_status}"

  local e2e_done=0 chaos_done=0
  [[ "$e2e_status" == "Passed" || "$e2e_status" == "Failed" ]] && e2e_done=1
  [[ "$chaos_status" == "Passed" || "$chaos_status" == "Failed" ]] && chaos_done=1
  (( e2e_done && chaos_done )) || return 0

  echo ""
  if [[ "$e2e_status" == "Passed" && "$chaos_status" == "Passed" ]]; then
    echo "    ✅ QA green — E2E: Passed  Chaos: Passed"
    [[ -n "$E2E_RUN_URL"   ]] && echo "       E2E run:   ${E2E_RUN_URL}"
    [[ -n "$CHAOS_RUN_URL" ]] && echo "       Chaos run: ${CHAOS_RUN_URL}"
    state_complete qa_done result=passed e2e="$e2e_status" chaos="$chaos_status"
    return 2
  fi
  echo "    ❌ QA failed — E2E: ${e2e_status}  Chaos: ${chaos_status}"
  [[ -n "$E2E_RUN_URL"   ]] && echo "       E2E run:   ${E2E_RUN_URL}"
  [[ -n "$CHAOS_RUN_URL" ]] && echo "       Chaos run: ${CHAOS_RUN_URL}"
  state_complete qa_done result=failed e2e="$e2e_status" chaos="$chaos_status"
  return 3
}

cmd_monitor_qa() {
  _monitor_qa_announce_mode
  local QA_ISSUE_NUMBER="" QA_ISSUE_URL=""
  _monitor_qa_resolve_issue

  local short_rc=0
  _monitor_qa_short_circuit_if_done || short_rc=$?
  (( short_rc == 2 )) || return $short_rc  # 0 or 1 → already settled; 2 → fall through.

  local E2E_RUN_URL="" CHAOS_RUN_URL="" urls_saved=0
  echo ">>> Monitoring QA for v${VERSION}"
  echo "    Issue: ${QA_ISSUE_URL}"
  echo "    Polling every 5 min (2h cap). Ctrl-C to abort."
  echo ""

  # Poll the "Central CI View" project board — the downstream test repos (weaviate-e2e-tests,
  # weaviate-chaos-engineering) write E2E/Chaos field values directly via update_project.yaml.
  # Terminal values: "Passed" or "Failed". Running: "In progress" or absent.
  # The "E2E Job" / "Chaos Job" text fields hold the run URLs once the downstream workflows start.
  local deadline=$(( $(date +%s) + 7200 ))
  while (( $(date +%s) < deadline )); do
    local rc=0; _monitor_qa_poll_once || rc=$?
    case $rc in
      0) sleep 300 ;;             # continue polling
      2) return 0 ;;              # passed
      3) return 1 ;;              # failed
      *) echo "internal: _monitor_qa_poll_once returned $rc" >&2; return 1 ;;
    esac
  done

  echo "⚠️  monitor-qa timed out after 2h — check board manually: ${QA_ISSUE_URL}"
  exit 2
}

# ─── auto mode helpers ────────────────────────────────────────────────────────

determine_next_step() {
  # Reads $STATE_FILE, $VERSION, $REPO — returns one of:
  #   prepare | qa | await_merge | finalize | await_publish | done
  if ! state_done pr_create;     then echo "prepare"; return; fi
  if ! state_done qa_dispatched; then echo "qa";      return; fi

  local PR_NUM; PR_NUM=$(resolve_pr_number)
  local PR_STATE; PR_STATE=$(gh pr view "$PR_NUM" --repo "$REPO" --json state \
    --jq .state 2>/dev/null || echo "UNKNOWN")
  if [[ "$PR_STATE" != "MERGED" ]]; then echo "await_merge"; return; fi

  if ! state_done draft_release;      then echo "finalize"; return; fi
  if ! state_done docker_hub;         then echo "finalize"; return; fi
  if ! state_done release_published;  then echo "finalize"; return; fi

  echo "done"
}

print_await_merge() {
  local PR_NUMBER; PR_NUMBER=$(resolve_pr_number)
  local PR_URL="https://github.com/${REPO}/pull/${PR_NUMBER}"
  echo ">>> Awaiting merge — v${VERSION}"
  echo "    PR:  ${PR_URL}"
  echo ""
  echo ">>> CI status (PR #${PR_NUMBER}):"
  gh pr checks "$PR_NUMBER" --repo "$REPO" 2>/dev/null || echo "    (could not fetch CI status)"
  echo ""
  echo "    Merge the PR with 'Create a merge commit', then re-run this script."
}

# ─── cmd_auto ─────────────────────────────────────────────────────────────────

# All _auto_* helpers below populate AUTO_VERSION / AUTO_MAJOR / AUTO_MINOR
# in cmd_auto via dynamic scope.

# On main → prepare a new minor release by cutting stable/vX.Y from HEAD.
_auto_from_main() {
  git fetch --all --prune --tags -q 2>/dev/null || echo "    ⚠️  git fetch failed; using local refs" >&2
  local LATEST_XY; LATEST_XY=$(git branch -r --list 'origin/stable/v*' \
    | sed 's|.*origin/stable/v||' | sort -V | tail -1)
  [[ -n "$LATEST_XY" ]] || { echo "ERROR: no stable/v* branches found on remote." >&2; exit 1; }
  AUTO_MAJOR="${LATEST_XY%%.*}"
  AUTO_MINOR=$(( ${LATEST_XY##*.} + 1 ))
  AUTO_VERSION="${AUTO_MAJOR}.${AUTO_MINOR}.0"
  local NEW_STABLE="stable/v${AUTO_MAJOR}.${AUTO_MINOR}"

  echo ">>> New minor release: v${AUTO_VERSION} (branch: ${NEW_STABLE})"
  if git_local_ref_exists "refs/remotes/origin/${NEW_STABLE}"; then
    echo ">>> ${NEW_STABLE} already on remote — checking out"
    git checkout "$NEW_STABLE"
    local LOCAL_SHA REMOTE_SHA
    LOCAL_SHA=$(git rev-parse HEAD)
    REMOTE_SHA=$(git rev-parse "refs/remotes/origin/${NEW_STABLE}")
    if [[ "$LOCAL_SHA" != "$REMOTE_SHA" ]]; then
      echo "    ⚠️  Local ${NEW_STABLE} (${LOCAL_SHA:0:8}) differs from remote (${REMOTE_SHA:0:8})."
      echo "       Reconcile manually before continuing."
    fi
    return
  fi
  confirm "Cut new stable branch ${NEW_STABLE} from main HEAD and push to origin?" || exit 1
  echo ">>> Creating ${NEW_STABLE} from main HEAD"
  git checkout -b "$NEW_STABLE"
  git push -u origin "$NEW_STABLE"
  echo "    ✅ ${NEW_STABLE} created and pushed"
}

# Echo the version of any in-progress release for ${AUTO_MAJOR}.${AUTO_MINOR}, or empty.
# Journal-mode scans local STATE_DIR; stateless mode scans remote prepare-release-v*
# branches whose tags aren't yet pushed (same heuristic, from GitHub instead of disk).
_auto_scan_in_progress() {
  if (( JOURNAL_ENABLED )) && [[ -d "$STATE_DIR" ]]; then
    local j jv pub
    for j in "$STATE_DIR"/v*.json; do
      [[ -f "$j" ]] || continue
      jv=$(jq -r .version "$j")
      [[ "$jv" == "${AUTO_MAJOR}.${AUTO_MINOR}."* ]] || continue
      pub=$(jq -r '.steps.release_published // empty' "$j")
      [[ -z "$pub" ]] && { echo "$jv"; return; }
    done
  fi
  local _remote="https://github.com/${REPO}.git"
  local _candidates _pv result=""
  _candidates=$(git ls-remote --heads "$_remote" \
    "prepare-release-v${AUTO_MAJOR}.${AUTO_MINOR}.*" 2>/dev/null \
    | awk '{print $2}' | sed 's|^refs/heads/prepare-release-v||' | sort -V)
  while IFS= read -r _pv; do
    [[ -z "$_pv" ]] && continue
    if ! git_remote_tag_exists "v${_pv}"; then
      result="$_pv"  # keep going — last (highest) wins
    fi
  done <<< "$_candidates"
  echo "$result"
}

# Compute the next patch version on stable/v${AUTO_MAJOR}.${AUTO_MINOR}.
# Ignores pre-release tags (vX.Y.Z-rc.N) — they aren't release tags and would
# confuse ${LATEST##*.}.
_auto_next_patch_version() {
  git fetch --tags -q 2>/dev/null || echo "    ⚠️  git fetch --tags failed; using local refs" >&2
  local LATEST; LATEST=$(git tag --list "v${AUTO_MAJOR}.${AUTO_MINOR}.*" --sort=-v:refname \
    | grep -E "^v${AUTO_MAJOR}\.${AUTO_MINOR}\.[0-9]+$" | head -1)
  local NEXT_PATCH; NEXT_PATCH=$([[ -n "$LATEST" ]] && echo "$(( ${LATEST##*.} + 1 ))" || echo "0")
  echo "${AUTO_MAJOR}.${AUTO_MINOR}.${NEXT_PATCH}"
}

# On stable/vX.Y → resume any in-progress release, or cut a new patch.
_auto_from_stable() {
  AUTO_MAJOR="$1"
  AUTO_MINOR="$2"
  local IN_PROGRESS; IN_PROGRESS=$(_auto_scan_in_progress)
  if [[ -n "$IN_PROGRESS" ]]; then
    AUTO_VERSION="$IN_PROGRESS"
    echo ">>> Resuming in-progress release v${AUTO_VERSION}"
  else
    AUTO_VERSION=$(_auto_next_patch_version)
    echo ">>> Starting new release v${AUTO_VERSION}"
  fi
}

# On prepare-release-vX.Y.Z[-rc.N] → resume that exact release.
_auto_from_prep_branch() {
  AUTO_VERSION="$1"
  AUTO_MAJOR="${AUTO_VERSION%%.*}"
  local _rest="${AUTO_VERSION#*.}"
  AUTO_MINOR="${_rest%%.*}"
  echo ">>> On prepare branch — resuming v${AUTO_VERSION}"
}

# Dispatch to the right handler for the resolved next step.
_auto_dispatch() {
  case "$1" in
    prepare)     cmd_prepare ;;
    qa)          cmd_qa ;;
    await_merge) print_await_merge ;;
    finalize)    cmd_finalize ;;
    done)
      local RELEASE_URL; RELEASE_URL=$(state_get '.release_url')
      echo "✅ Release v${VERSION} is complete."
      echo "   Release: ${RELEASE_URL}"
      ;;
  esac
}

cmd_auto() {
  local BRANCH; BRANCH=$(git rev-parse --abbrev-ref HEAD)
  local AUTO_VERSION="" AUTO_MAJOR="" AUTO_MINOR=""

  case "$BRANCH" in
    main)
      _auto_from_main
      ;;
    stable/v*)
      [[ "$BRANCH" =~ ^stable/v([0-9]+)\.([0-9]+)$ ]] || {
        echo "ERROR: Unrecognized stable branch '${BRANCH}'." >&2; exit 1; }
      _auto_from_stable "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
      ;;
    prepare-release-v*)
      [[ "$BRANCH" =~ ^prepare-release-v([0-9]+\.[0-9]+\.[0-9]+(-(rc|alpha|beta)\.[0-9]+)?)$ ]] || {
        echo "ERROR: Unrecognized prepare branch '${BRANCH}'." >&2; exit 1; }
      _auto_from_prep_branch "${BASH_REMATCH[1]}"
      ;;
    *)
      echo "ERROR: Unrecognized branch '${BRANCH}'." >&2
      echo "       Expected: main, stable/vX.Y, or prepare-release-vX.Y.Z" >&2
      exit 1
      ;;
  esac

  init_release "$AUTO_VERSION"
  local NEXT; NEXT=$(determine_next_step)
  echo ">>> Next step: ${NEXT}"
  echo ""
  _auto_dispatch "$NEXT"
}

# ─── entry point ──────────────────────────────────────────────────────────────

JOURNAL_ENABLED=0
_NEWARGS=()
for _a in "$@"; do
  case "$_a" in
    --journal)    JOURNAL_ENABLED=1 ;;
    -y|--yes)     ASSUME_YES=1 ;;
    *)            _NEWARGS+=("$_a") ;;
  esac
done
set -- "${_NEWARGS[@]+"${_NEWARGS[@]}"}"
unset _a _NEWARGS

# Subcommands that take <version> as $1 and dispatch through init_release.
# Saves repeating the same `[[ -n "${2:-}" ]] || { usage; exit 1; }; init_release "$2"; cmd_X`
# pattern for prepare / qa / monitor-qa.
_init_and() {
  local fn="$1" ver="${2:-}"
  [[ -n "$ver" ]] || { usage; exit 1; }
  init_release "$ver"
  "$fn"
}

case "${1:-}" in
  ""|auto)        cmd_auto ;;
  -h|--help|help) usage ;;
  reset)          cmd_reset       "${2:-}" ;;
  reset-step)     cmd_reset_step  "${2:-}" "${3:-}" ;;
  status)         cmd_status      "${2:-}" ;;
  journals)       cmd_journals    "${2:-}" ;;
  candidates)     cmd_candidates  "${2:-}" ;;
  verify)         cmd_verify      "${2:-}" "${3:-}" ;;
  prepare)        _init_and cmd_prepare    "${2:-}" ;;
  qa)             _init_and cmd_qa         "${2:-}" ;;
  monitor-qa)     _init_and cmd_monitor_qa "${2:-}" ;;
  finalize)
    [[ -n "${2:-}" ]] || { usage; exit 1; }
    case "${2:-}" in
      merge|tag|draft|image|publish)
        [[ -n "${3:-}" ]] || { usage; exit 1; }
        init_release "$3"; cmd_finalize "$2" "${4:-}" ;;
      *)
        init_release "$2"; cmd_finalize "" "${3:-}" ;;
    esac ;;
  *) echo "ERROR: unknown command '${1}'" >&2; usage; exit 1 ;;
esac
