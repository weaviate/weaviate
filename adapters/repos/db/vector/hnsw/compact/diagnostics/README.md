# Diagnostics

Developer-only tools for validating and debugging the HNSW compactor v2. All files use `//go:build ignore` and are not part of the production build. Run them manually with `go run` from the `compact/` directory.

## Data setup

These tools operate on `.condensed` files from a real Weaviate HNSW index. To get them, copy the commit log files from a shard's vector index directory:

```
<weaviate-data-dir>/<collection>/shards/<shard>/<vector-index>/
```

Place the `.condensed` files in the appropriate directory relative to `compact/`:

- **`e2e_compactor_validation.go`** expects a `test_data.bak.d/` directory (sibling of `diagnostics/`). It copies files from there into a fresh `test_data/` working directory on each run.
- **`e2e_validation.go`** expects `.condensed` files in `test_data/` (sibling of `diagnostics/`).
- **`diagnose_single_log.go`** and **`diagnose_tombstone_diff.go`** expect files in `compact/test_data/` (run from `compact/`). Note: `diagnose_single_log.go` has a hardcoded file path and node ID — edit these before running.
- **`regenerate_sorted_files.go`** expects `.condensed` files in `compact/test_data/`.

```
compact/
  test_data/              # used by most tools
    *.condensed
  test_data.bak.d/        # used by e2e_compactor_validation (source copy)
    *.condensed
  diagnostics/
    *.go
```

Neither directory is committed to the repo.

## Tools

### e2e_compactor_validation.go

Full end-to-end validation of the `Compactor` orchestrator. Copies `.condensed` files to a working directory, computes a control result by reading them sequentially, runs the compactor in a loop until convergence, then compares the final output against the control. Validates that the full pipeline (convert, merge, snapshot) preserves correctness.

```bash
cd adapters/repos/db/vector/hnsw/compact
go run diagnostics/e2e_compactor_validation.go
```

### e2e_validation.go

Lower-level E2E validation of individual pipeline stages:
1. Reads `.condensed` files sequentially (control)
2. Converts each `.condensed` to `.sorted`
3. Reads `.sorted` files sequentially
4. Merges all `.sorted` files using the N-way merger
5. Writes and reads a snapshot
6. Compares all results to ensure they match

```bash
cd adapters/repos/db/vector/hnsw/compact
go run diagnostics/e2e_validation.go
```

### diagnose_single_log.go

Reads a single `.condensed` file and inspects the state of a specific node (hardcoded). Converts to `.sorted` and reads it back to verify round-trip correctness. Traces all operations for the target node in both formats. Useful for tracking down node-specific bugs.

```bash
cd adapters/repos/db/vector/hnsw/compact
go run diagnostics/diagnose_single_log.go
```

### diagnose_tombstone_diff.go

Reads all `.condensed` and `.sorted` files, compares tombstone state between them, and traces operations for the first differing node. Useful for diagnosing tombstone loss or duplication during the condensed-to-sorted conversion.

```bash
cd adapters/repos/db/vector/hnsw/compact
go run diagnostics/diagnose_tombstone_diff.go
```

### regenerate_sorted_files.go

Converts all `.condensed` files into `.sorted` format. Useful for regenerating test fixtures or preparing data for merge tests.

```bash
cd adapters/repos/db/vector/hnsw/compact
go run diagnostics/regenerate_sorted_files.go
```

## Profiling

The E2E tools (`e2e_compactor_validation.go` and `e2e_validation.go`) start a pprof server on `:6060`:

```bash
# CPU profile (30 seconds)
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Heap profile
go tool pprof http://localhost:6060/debug/pprof/heap
```
