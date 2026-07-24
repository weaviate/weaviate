# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Weaviate is an open-source, cloud-native vector database written in Go. It stores both objects and vectors, supporting semantic search, hybrid search (BM25 + vector), RAG, and reranking.

## No bug is ever out of scope

This is a production database. Data loss and silent failures are unacceptable, full stop. If you uncover or even *suspect* a bug — adjacent failure mode, race window, edge case in a related journey, anything — you MUST address it in the same change set. Acceptable outcomes:

1. **Reproduce and fix it.** Include a regression test that fails without the fix and passes with it.
2. **Reproduce it and commit a failing (red) test** that pins the bug, then escalate explicitly to the user. Never silently leave a known-bad code path with no test.

Unacceptable:

- "Out of scope." There is no out of scope for bugs in this codebase. If you find one, you own it until it's either fixed or pinned with a failing test.
- "Known issue, leaving for follow-up." Same rule — pin it with a failing test before you stop working on it.
- Fixing only the one specific reproduction a reviewer gave you and shipping. If a bug exists in journey X, enumerate every realistic adjacent journey (X-1, X+1, multi-property, multi-round, every related state-machine transition) and add tests for the lot. Whack-a-mole on individual repros is forbidden.

When you find a bug while working on something else, the response is: stop the original work, switch context to the bug, write the test, write the fix (or commit the red test and surface it loudly), then resume. The original task can wait.

This rule overrides any conflicting guidance about staying focused, minimal diffs, or scope discipline. Bug coverage wins.

## Build & Run

```bash
# Build the binary (CGO_ENABLED=0, static linking)
make weaviate

# Build debug binary (with delve support)
make weaviate-debug

# Run locally (starts dependencies via docker-compose)
make local

# Build Docker image
make weaviate-image

# Regenerate gRPC protobuf code
make grpc

# Regenerate mocks (uses mockery via Docker)
make mocks
```

## Testing

When creating tests prefer table-driven tests.

### Unit Tests
```bash
# Run a single package's tests
go test ./adapters/repos/db/lsmkv/...

# Run a specific test
go test -run TestBucketReplace ./adapters/repos/db/lsmkv/

# Run all unit tests (slow, use sparingly)
go test -race -count 1 $(go list ./... | grep -v 'test/acceptance' | grep -v 'test/modules')
```

### Integration Tests
Use build tag `integrationTest`. Run per-package only, never repo-wide:
```bash
go test -tags integrationTest -count 1 -race ./adapters/repos/db/...
```

### E2E / Acceptance Tests
Prefer testcontainers (modern style) over requiring a running Weaviate instance (legacy style). Never run the full e2e suite. Run only specific packages you changed:
```bash
go test -count 1 -race -timeout 15m ./test/acceptance/grpc/...
```
When adding new e2e tests, prefer creating a separate package. Only extend existing packages when tests clearly fit.

**Pre-building the Docker image for acceptance tests:**
By default, testcontainers builds the Weaviate Docker image from source on every test run. For packages with many test functions (e.g. `reindex_multinode` with 9 tests), this rebuilds the image 9 times, wasting disk and time.

Pre-build once and reuse:
```bash
# Build the image once (tag it with a recognizable name)
make weaviate-image WEAVIATE_IMAGE=weaviate-test:local

# Run tests with the pre-built image (skips docker build entirely)
TEST_WEAVIATE_IMAGE=weaviate-test:local go test -count 1 -race -timeout 20m ./test/acceptance/reindex_multinode/...
```

Always rebuild the image after code changes. The `TEST_WEAVIATE_IMAGE` env var is read by `test/docker/compose.go` and applies to all testcontainer-based tests.

**Adding new acceptance test CI jobs (`test/run.sh`):**
When adding a new `run_acceptance_*` function in `test/run.sh`, it **must** build the Docker image and export `TEST_WEAVIATE_IMAGE` before running tests. Without this, testcontainers builds from source on every test function, which is slow and can exceed startup timeouts. Follow this pattern:
```bash
function run_acceptance_my_new_tests() {
  echo_green "acceptance — my-tests: building weaviate/test-server image..."
  GIT_REVISION=$(git rev-parse --short HEAD)
  GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
  docker compose -f docker-compose-test.yml build \
    --build-arg GIT_REVISION="$GIT_REVISION" \
    --build-arg GIT_BRANCH="$GIT_BRANCH" \
    --build-arg EXTRA_BUILD_ARGS="-race" \
    weaviate
  export TEST_WEAVIATE_IMAGE=weaviate/test-server
  run_aof_group "my-tests" test/acceptance/my_tests
}
```

### Linting
Always validate linters pass at the end of a task:
```bash
golangci-lint run ./...
./tools/linter_go_routines.sh
```

The goroutine linter enforces that all goroutines use `entities/errors/go_wrapper.go` instead of bare `go` statements.

### Mutation Testing
Mutation testing measures how *effective* the existing tests are (not just coverage): it mutates source code one change at a time and checks whether the tests fail. A mutant the tests still pass is a "survivor" — a gap where a behaviour change goes undetected. Uses [mutest](https://github.com/fchimpan/mutest), a standalone CLI, via a thin wrapper. Run it against a single package:
```bash
tools/mutest.sh usecases/byteops
tools/mutest.sh usecases/cluster -v                  # extra flags pass through to mutest
tools/mutest.sh usecases/cluster/...                 # include sub-packages
MUTEST_THRESHOLD=80 tools/mutest.sh entities/schema  # gate CI: exit 1 if kill rate < 80%
```
The wrapper installs the pinned `mutest` binary on demand (`go install`, not a module dependency — Weaviate's vendored `go.mod` is untouched) and runs it from the repo root. Env overrides: `MUTEST_VERSION`, `MUTEST_TIMEOUT` (per-mutant, default 60s), `MUTEST_WORKERS`, `MUTEST_THRESHOLD` (0-100). Results print as `--- KILLED/SURVIVED: file:line:col  > to >=` plus a summary mutation score; the wrapper propagates mutest's exit code.

How it differs from a library runner: mutest rewrites source in place, runs `go test`, then restores it — no build tags, no in-repo harness, no `-mod=mod`. Scope: it mutates only the named package (a bare dir, no `/...`), so generated sub-packages like `mocks/` are skipped automatically. Operators: relational-operator replacement only (`> >= < <= == !=`), which keeps mutant counts low. Note the per-mutant test run uses the package's own suite, so packages whose tests need external services or long timeouts may need a larger `MUTEST_TIMEOUT`.

## Architecture

The codebase follows a **hexagonal (ports-and-adapters) architecture**:

```
cmd/weaviate-server/     → Entry point (go-swagger generated main)
adapters/
  handlers/
    rest/                → REST API handlers (go-swagger generated + configure_api.go wiring)
    grpc/                → gRPC API handlers (search, batch, aggregation)
    graphql/             → GraphQL API layer
  repos/
    db/                  → Database/storage implementation
      lsmkv/             → Custom LSM key-value store
      vector/            → Vector index implementations (HNSW, flat, dynamic)
usecases/                → Business logic (schema, objects, traverser, backup, classification)
entities/                → Domain models, interfaces, shared types
modules/                 → Plugin modules (vectorizers, generative, rerankers, backup providers)
cluster/                 → Distributed consensus (RAFT), replication, sharding
grpc/proto/              → gRPC protocol buffer definitions
```

### Data Path: DB → Index → Shard → Store
- **DB**: Top-level, holds all indices (one per collection/class)
- **Index**: Per-collection, manages shards and multi-tenancy
- **Shard**: Unit of storage — contains an LSM store for objects/properties, vector index(es), and inverted index for filtering
- **LSM Store** (`lsmkv`): Custom LSM with three bucket strategies:
  - **Replace**: Classic KV (one value per key)
  - **Set**: Unordered multi-value per key (inverted index)
  - **Map**: Key-value pairs per key (BM25 term frequencies)

### Vector Indexes (`adapters/repos/db/vector/`)
- **HNSW**: Primary index — supports PQ/BQ/SQ compression, multi-vector, tombstone cleanup
- **Flat**: Brute-force for small datasets
- **Dynamic**: Auto-switches between flat and HNSW based on data size

### Module System (`modules/`)
Modules implement the `Module` interface (`Name()`, `Init()`, `Type()`) and optionally provide HTTP handlers, vectorization, generative capabilities, or reranking. ~67 modules covering OpenAI, Cohere, HuggingFace, Anthropic, backup-s3/gcs/azure, and more. Modules are registered in `adapters/handlers/rest/configure_api.go`.

### Startup Wiring
All initialization happens in `adapters/handlers/rest/configure_api.go` → `MakeAppState()`: DB creation, schema manager, cluster/RAFT services, module registration, gRPC server startup, and monitoring.

### Additional Resources
Before starting on an unfamiliar area, list `docs/` to check whether a topic-specific design note exists.

## Code Conventions

### Allocation Efficiency
On hot paths, avoid unnecessary allocations. Specifically avoid `binary.Read` (allocates heavily) — use `usecases/byteops` package instead.

### Goroutines
Never use bare `go` statements. Always use the wrapper from `entities/errors/go_wrapper.go`. The `tools/linter_go_routines.sh` linter enforces this.

### Linter Configuration
Uses `golangci-lint` v2 with `gofumpt` formatter. Key enabled linters: `bodyclose`, `errorlint`, `exhaustive`, `forbidigo` (no `fmt.Print*` or `println`), `gocritic` (deferInLoop), `misspell`, `nolintlint`.

### Logging

We use logrus. Errors land in the **message body**, not in a separate `WithError` field, at **every level** (`Error`, `Warn`, `Info`, `Debug`).

```go
// Wrong — error text ends up in a separate "error" field that operator-side
// log aggregators frequently render as a sibling column rather than inline.
logger.WithField("path", p).WithError(err).Warn("failed to remove dir")

// Right — error text lands in the message body.
logger.WithField("path", p).Warnf("failed to remove dir: %v", err)
```

```go
// Wrong — Error(fmt.Errorf(...)) builds a useless intermediate error value
// just to stringify it.
logger.WithField("k", v).Error(fmt.Errorf("torn state: %q missing", x))

// Right — let the logger format directly.
logger.WithField("k", v).Errorf("torn state: %q missing", x)
```

The rule applies to every level — `Errorf`, `Warnf`, `Infof`, `Debugf`. `WithError` is never used.


### API Code Generation
REST API is generated from OpenAPI specs via go-swagger (`openapi-specs/`). You can regenerate by running `./tools/gen-code-from-swagger.sh`.
gRPC is generated from protobuf definitions in `grpc/proto/` using `buf`.

## CI / Pipeline Monitoring

All monitoring scripts live in `.claude/scripts/` and are committed to the repo. Always run them as background tasks (use `run_in_background=true` in the Bash tool) so you get notified on completion without blocking the conversation.

### Monitor PR checks
Polls all CI checks for a PR until they complete. Exits with code 1 if any checks fail:
```bash
PR=1234 .claude/scripts/monitor_pr.sh
```

### Monitor Docker image build
Use this when waiting for a PR's docker image to be produced. First get the run ID from the PR's docker checks, then pass it to the script:
```bash
# Get the run ID
gh pr checks <PR> --repo weaviate/weaviate 2>&1 | grep -i "docker"
# Monitor the build
.claude/scripts/monitor_docker.sh <run_id>
```
The script also prints the docker image tags on success. Tags are fetched via `gh api` (works even if the overall run is still in progress).

### Inspect failed checks / rerun
```bash
# Get job ID from: gh pr checks <PR> --repo weaviate/weaviate
gh api repos/weaviate/weaviate/actions/jobs/<job_id>/logs 2>&1 | grep "FAIL:" | head -20

# Re-run only failed jobs:
gh run rerun <run_id> --failed --repo weaviate/weaviate
```
