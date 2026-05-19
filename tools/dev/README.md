

## Usage 
### gen-code-from-swagger.sh
#### Steps

1. Download the specified version of go-swagger if it is not already present.

2. Install `goimports` to ensure consistent formatting https://pkg.go.dev/golang.org/x/tools/cmd/goimports 

3. Make sure go path is exported `export PATH=$GOPATH/bin:$PATH`

4.  you should find `Success` message which means the script ran :) 

### qa_pr.sh

Trigger the QA pipeline (E2E + chaos tests) for any Weaviate PR. Creates a tracking
issue in `weaviate/weaviate-qa`, adds it to project board #28 with status set to
"In Progress", and dispatches the `weaviate-qa/main.yaml` matrix workflow with the
PR's preview docker tags.

```bash
tools/dev/qa_pr.sh <pr-number-or-url>
```

Override the assignee via `QA_ASSIGNEE` (default: `antas-marcin`).

Requires `gh` with the `project` scope: `gh auth refresh -h github.com -s project`.

### create_release.sh

Single-version prep helper. Bumps `openapi-specs/schema.json` to the target
version, runs `make deps`, and calls `tools/prepare_release.sh` to build the
prepare-release commit + local tag. Optionally fetches and creates the prep
branch from a base branch.

```bash
tools/dev/create_release.sh v1.37.14
tools/dev/create_release.sh v1.37.14 --branch=stable/v1.37
tools/dev/create_release.sh v1.37.14 --branch=stable/v1.37 --remote=upstream
```

Requires a clean working tree. Does not push, does not open a PR — at the
end it prints a `git push` command for the user to run. For full release
orchestration (PR, QA, finalize), use `release.sh` below, which delegates the
schema-bump + `make deps` + `prepare_release.sh` trio to this script.

### release.sh

State machine that drives a Weaviate core release end-to-end: branch + PR
prep (delegated in part to `create_release.sh`), docker preview poll + QA
dispatch (delegated to `qa_pr.sh`), tag push, and draft GitHub Release. Run
from the weaviate repo root.

```bash
tools/dev/release.sh                      # auto-detect branch and resume
tools/dev/release.sh prepare    <ver>
tools/dev/release.sh qa         <ver>
tools/dev/release.sh monitor-qa <ver>
tools/dev/release.sh finalize   <ver> [<pr#>]
tools/dev/release.sh status     <ver>     # human-readable state summary
tools/dev/release.sh reset      <ver>     # delete state file
tools/dev/release.sh reset-step <ver> <step>  # wipe one step (e.g. qa_dispatched)
tools/dev/release.sh verify     <stage> <ver>
tools/dev/release.sh --journal <command> ...  # opt-in to disk journal
tools/dev/release.sh -y <command> ...         # skip [Y/n] confirmations
```

**Confirmation prompts.** Every operation with externally visible side
effects (push prep branch, create PR, dispatch QA, push tag, create draft
release, cut new stable branch) prompts `[Y/n]` before running. Empty input
defaults to yes. Pass `-y` (or `--yes`) to skip all prompts — useful for
re-runs of an aborted release where you've already approved each step.

**Two-layer prep.** The schema-bump + `make deps` + `prepare_release.sh`
core of `release.sh prepare` is delegated to `create_release.sh`. Use
`release.sh` for the full orchestration; invoke `create_release.sh` directly
only for one-off manual prep.

**State sources.** By default the script is stateless — every invocation
re-probes GitHub and the local repo to figure out what's already been done
(prep branch, PR, QA tracking issue, project-board E2E/Chaos status, release
tag, GitHub Release). Inferred state lives in memory; nothing is written to
disk.

Pass `--journal` to persist state at `_local/release/v<version>.json`
(gitignored). When the flag is set but the file is missing, the journal is
seeded from the inferred state on first run. Steps are idempotent regardless
of source — re-running picks up where it left off. To force a single step to
re-run from a journal, use `reset-step <ver> <step>` rather than wiping the
entire file.
