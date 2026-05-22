

## Usage 
### gen-code-from-swagger.sh
#### Steps

1. Download the specified version of go-swagger if it is not already present.

2. Install `goimports` to ensure consistent formatting https://pkg.go.dev/golang.org/x/tools/cmd/goimports 

3. Make sure go path is exported `export PATH=$GOPATH/bin:$PATH`

4.  you should find `Success` message which means the script ran :) 

### qa_pr.sh

Trigger the QA pipeline (E2E + chaos tests) for any Weaviate PR. Creates a tracking
issue in `weaviate/weaviate-qa`, adds it to project board `#28` with status set to
"In Progress", and dispatches the `weaviate-qa/main.yaml` matrix workflow with the
PR's preview docker tags.

```bash
tools/dev/qa_pr.sh <pr-number-or-url>
```

Requires `gh` with the `project` scope: `gh auth refresh -h github.com -s project`.

### create_release.sh

Single-version prep helper. Bumps `openapi-specs/schema.json` to the target
version, calls `tools/prepare_release.sh` to build the prepare-release commit + local tag
and runs `tools/dev/qa_pr.sh` to QA the given release.

```bash
./tools/dev/create_release.sh v1.37.14 --branch=stable/v1.37 --qa
```

Requires a clean working tree. Script creates a prepare release branch off of the
specified branch, pushes it to origin and then creates a prepare release PR and
runs e2e and chaos tests against it.
