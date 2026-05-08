

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

### release.sh

State machine that drives a Weaviate core release end-to-end: branch + PR
prep, docker preview poll + QA dispatch (delegated to `qa_pr.sh`), tag push,
and draft GitHub Release. Run from the weaviate repo root.

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
```

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
