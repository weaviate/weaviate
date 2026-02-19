# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Weaviate is an open-source, cloud-native vector database written in Go. It stores both objects and vectors, supporting semantic search, hybrid search (BM25 + vector), RAG, and reranking.

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

### Linting
Always validate linters pass at the end of a task:
```bash
golangci-lint run ./...
./tools/linter_go_routines.sh
```

The goroutine linter enforces that all goroutines use `entities/errors/go_wrapper.go` instead of bare `go` statements.

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

## Code Conventions

### Allocation Efficiency
On hot paths, avoid unnecessary allocations. Specifically avoid `binary.Read` (allocates heavily) — use `usecases/byteops` package instead.

### Goroutines
Never use bare `go` statements. Always use the wrapper from `entities/errors/go_wrapper.go`. The `tools/linter_go_routines.sh` linter enforces this.

### Linter Configuration
Uses `golangci-lint` v2 with `gofumpt` formatter. Key enabled linters: `bodyclose`, `errorlint`, `exhaustive`, `forbidigo` (no `fmt.Print*` or `println`), `gocritic` (deferInLoop), `misspell`, `nolintlint`.

### Logging
We use logrus as logger. Always populate errors using `.Error(err)` and do NOT use `WithError`.


### API Code Generation
REST API is generated from OpenAPI specs via go-swagger (`openapi-specs/`). You can regenerate by running `./tools/gen-code-from-swagger.sh`.
gRPC is generated from protobuf definitions in `grpc/proto/` using `buf`.
