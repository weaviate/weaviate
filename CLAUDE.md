# CLAUDE.md

Weaviate is an open-source vector database written in Go. It stores objects and vectors and supports semantic search, hybrid search (BM25 + vector), RAG and reranking.

## No bug is ever out of scope

This is a production database: data loss and silent failures are unacceptable. If you find or even *suspect* a bug (an adjacent failure mode, a race window, an edge case in a related journey, anything), you MUST address it in the same change set, in one of two ways:

1. **Reproduce and fix it**, with a regression test that fails without the fix and passes with it.
2. **Reproduce it, commit a failing (red) test that pins it, and escalate explicitly to the user.**

"Out of scope" and "known issue, leaving for follow-up" are never acceptable: you own the bug until it is fixed or pinned by a failing test. Never fix only the reproduction a reviewer gave you. If a bug exists in journey X, enumerate every realistic adjacent journey (X-1, X+1, multi-property, multi-round, every related state-machine transition) and test them all.

When you find a bug while working on something else, stop, write the test, write the fix (or commit the red test and surface it loudly), then resume the original task.

This rule overrides any guidance about staying focused, minimal diffs or scope discipline.

## Build & Run

```bash
make weaviate        # static binary (CGO_ENABLED=0)
make weaviate-debug  # binary for delve (optimizations off)
make weaviate-image  # Docker image
./tools/dev/restart_dev_environment.sh  # tear down and restart dependencies (docker compose)
make local           # single-node dev server; dependencies must be running
make grpc            # regenerate gRPC code (buf)
make mocks           # regenerate mocks (mockery via Docker)
./tools/gen-code-from-swagger.sh  # regenerate REST API from openapi-specs/ (go-swagger)
```

## Testing

Prefer table-driven tests.

- **Unit**, all packages (slow, use sparingly): `go test -race -count 1 $(go list ./... | grep -v 'test/acceptance' | grep -v 'test/modules')`
- **Integration**: build tag `integrationTest`, run per package, never repo-wide: `go test -tags integrationTest -count 1 -race ./adapters/repos/db/...`
- **E2E / acceptance**: never run the full suite, only the packages you changed: `go test -count 1 -race -timeout 15m ./test/acceptance/grpc/...`. Prefer testcontainers over tests that need a running Weaviate instance (legacy). Put new e2e tests in a new package unless they clearly fit an existing one. Before running a testcontainer package locally or adding a `run_acceptance_*` job to `test/run.sh`, read `docs/acceptance-test-images.md`: without a pre-built image, testcontainers rebuilds Weaviate for every container.

## Linting & Code Conventions

At the end of every task, `golangci-lint run ./...` and `./tools/linter_go_routines.sh` must pass. golangci-lint is v2 with the `gofumpt` formatter and enables `bodyclose`, `errorlint`, `exhaustive`, `forbidigo` (no `fmt.Print*`, `println` or `spew.Dump`), `gocritic` (deferInLoop only), `misspell` and `nolintlint`.

- **Goroutines:** never use a bare `go` statement; use the wrapper in `entities/errors/go_wrapper.go` (enforced by `linter_go_routines.sh`).
- **Allocations:** avoid them on hot paths. Never use `binary.Read` (allocates heavily); use `usecases/byteops`.
- **Logging (logrus):** errors go in the message body at every level. Never use `WithError`: log aggregators render its field as a separate column. Don't build an error with `fmt.Errorf` just to log it:
  ```go
  logger.WithField("path", p).Warnf("failed to remove dir: %v", err) // not .WithError(err).Warn("failed to remove dir")
  logger.WithField("k", v).Errorf("torn state: %q missing", x)       // not .Error(fmt.Errorf("torn state: %q missing", x))
  ```

## Architecture

Hexagonal (ports and adapters):

- `cmd/weaviate-server/`: entry point (go-swagger generated main)
- `adapters/handlers/`: `rest/` (go-swagger generated, wired in `configure_api.go`), `grpc/` (search, batch, aggregation), `graphql/`
- `adapters/repos/db/`: storage; `lsmkv/` is the custom LSM store, `vector/` the vector indexes
- `usecases/`: business logic (schema, objects, traverser, backup, classification)
- `entities/`: domain models, interfaces, shared types
- `modules/`: plugin modules (vectorizers, generative, rerankers, backup providers)
- `cluster/`: RAFT consensus, replication, sharding
- `grpc/proto/`: gRPC protobuf definitions

Data path DB → Index → Shard → Store:

- **DB** holds one Index per collection (class).
- **Index** manages the collection's shards and multi-tenancy.
- **Shard** is the unit of storage: an LSM store for objects and properties, vector index(es), and the inverted index.
- **LSM store** (`lsmkv`) bucket strategies: **Replace** (one value per key), **SetCollection** (unordered values per key), **MapCollection** (key-value pairs per key; legacy BM25 index), **RoaringSet** (bitmaps; filterable index), **RoaringSetRange** (range filters), **Inverted** (BM25 with BlockMax WAND).

Vector indexes (`adapters/repos/db/vector/`): **HNSW** (primary; PQ/BQ/SQ/RQ compression, multi-vector, tombstone cleanup), **Flat** (brute force for small datasets), **Dynamic** (switches between flat and HNSW based on data size), **HFresh** (SPFresh algorithm, work in progress).

Modules implement `Module` (`Name()`, `Init()`, `Type()`) and optionally provide HTTP handlers, vectorization, generative capabilities or reranking. `MakeAppState()` in `adapters/handlers/rest/configure_api.go` does all startup wiring: DB, schema manager, cluster/RAFT services, module registration, gRPC server and monitoring.

Before starting on an unfamiliar area, list `docs/` for a topic-specific design note.

## CI / Pipeline Monitoring

For PR checks, a PR's Docker image build, or failed CI jobs, read `.claude/scripts/README.md`. Always run the monitoring scripts as background tasks.
