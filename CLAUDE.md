# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Weaviate is an open-source, cloud-native vector database written in Go. It stores objects and vectors, enabling semantic search, hybrid search, and retrieval-augmented generation (RAG) at scale. It supports REST, gRPC, and GraphQL APIs.

## Build Commands

```bash
make weaviate                    # Build binary (default target)
make weaviate-debug              # Build with debug symbols (for delve)
make weaviate-image              # Build Docker image
```

Direct build: `CGO_ENABLED=0 go build -tags netgo -o cmd/weaviate-server/weaviate ./cmd/weaviate-server`

## Testing

```bash
make test                        # Unit tests (builds binary first)
make test-integration            # Integration tests
go test ./path/to/package        # Run tests for a specific package
go test -run TestName ./path/to/package  # Run a single test
go test -race -count 1 ./path/to/package # With race detector, no cache
```

Test runner with more options: `./test/run.sh` (use `-u` unit, `-i` integration, `-a` acceptance, `-m` module tests).

Integration tests use build tags `integrationTest` and `integrationTestSlow`.

## Linting

```bash
golangci-lint run                # Main linter (config: .golangci.yml)
```

Key lint rules:
- `fmt.Print*()` and `print()` are **forbidden** — use the logger
- `spew.Dump()` is forbidden
- Exhaustive switch statements required
- Code formatted with `gofumpt` (not `gofmt`)

Custom linters (run in CI):
- `./tools/linter_error_groups.sh` — must use `entities/errors/error_group_wrapper.go`
- `./tools/linter_go_routines.sh` — must use `entities/errors/go_wrapper.go` for goroutines
- `./tools/linter_waitgroups_done.sh` — must call `defer wg.Done()`

## Code Generation

```bash
make banner                      # Regenerate REST API code from OpenAPI spec
make mocks                       # Regenerate test mocks (uses mockery v2.53.5)
make grpc                        # Regenerate gRPC/protobuf code
```

After modifying `openapi-specs/schema.json`, run `make banner` and commit the generated changes.

## Local Development

```bash
make local                       # Single-node dev server
make local-oidc                  # Dev server with OIDC auth
make local-rbac                  # Dev server with RBAC
make debug                       # Dev server with delve debugger
```

## Architecture

Weaviate follows **clean architecture** (ports and adapters):

```
adapters/handlers/ (REST, gRPC)  ←  inbound adapters
    ↓ calls
usecases/ (business logic)       ←  application layer
    ↓ operates on
entities/ (domain models)        ←  core domain
    ↓ persisted via
adapters/repos/ (storage)        ←  outbound adapters
```

**Inner layers never depend on outer layers.** Repositories implement interfaces defined in usecases.

### Key Directories

- **`cmd/weaviate-server/`** — Entry point. Minimal main.go delegates to go-swagger generated server.
- **`adapters/handlers/rest/`** — REST API handlers (`handlers_*.go` files). All registered in `configure_api.go` (the central wiring file that initializes DB, cluster, modules, and all handlers).
- **`adapters/handlers/grpc/`** — gRPC handlers for internal cluster communication.
- **`adapters/repos/db/`** — Storage layer. `DB` → `Index` (per class) → `Shard` hierarchy. Contains HNSW vector index, inverted index, and LSMKV storage engine.
- **`usecases/`** — Business logic managers. Key packages: `objects/` (CRUD), `schema/` (schema management), `traverser/` (search/query), `backup/`, `replica/`.
- **`entities/`** — Domain models and value objects. `models/` contains go-swagger generated models from the OpenAPI spec.
- **`cluster/`** — Distributed clustering with RAFT consensus, schema sync, replication protocol, and node-to-node RPC.
- **`modules/`** — Plugin system (60+ modules). Vectorizers (text2vec-openai, etc.), generative models, backup providers, rerankers. All implement `modulecapabilities.Module` interface.
- **`openapi-specs/schema.json`** — REST API specification (source of truth for generated code).
- **`grpc/proto/`**, **`cluster/proto/`** — Protobuf definitions.

### Request Flow Example (Add Object)

REST handler (`handlers_objects.go`) → `objects.Manager.Add()` → validates entity → calls DB repo → HNSW vector index + inverted index + LSMKV storage → triggers replication if clustered.

### Testing Patterns

- Unit tests: same package, `_test.go` suffix
- Fakes/mocks: `fakes_for_test.go` in same package
- Acceptance tests: `test/acceptance/`
- Module-specific tests: `test/modules/`

## Commit Convention

Include GitHub issue numbers in commits: `gh-9001 your message here`.

## Go Version

Go 1.25 (see Makefile `GO_VERSION`). CGO is disabled (`CGO_ENABLED=0`).
