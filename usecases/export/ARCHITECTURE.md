# Export Feature Architecture

## Overview

The export feature produces point-in-time snapshots of Weaviate collections as
Parquet files on an external storage backend (S3, GCS, Azure). It uses a
**two-phase commit (2PC) protocol** across the cluster so that every
participating node anchors its snapshot in the same coordinated window, then
scans and uploads independently.

```
                        +-----------+
                        |  Client   |
                        +-----+-----+
                              |
                     POST /v1/export/{backend}
                              |
                        +-----v-----+
                        | Scheduler |  (coordinator node)
                        +-----+-----+
                              |
                 Phase 1: Prepare (all nodes)
                              |
                 Phase 2: Commit  (all nodes)
                              |
              +---------------+---------------+
              |                               |
    +---------+---------+           +---------+---------+
    | Participant       |           | Participant       |
    | node-1            |           | node-2            |
    |                   |           |                   |
    | Prepare:          |           | Prepare:          |
    |   snapshot shards |           |   snapshot shards |
    | Commit:           |           | Commit:           |
    |   scan -> parquet |           |   scan -> parquet |
    |   upload          |           |   upload          |
    +-------------------+           +-------------------+
```

## Two-Phase Commit Protocol

The coordinator (Scheduler, on the node that receives the HTTP request) drives
the protocol. All persistent state lives on the backend (metadata JSON,
per-node status files, Parquet files). The coordinator is stateless after the
initial response.

**Phase 1 -- Prepare.** The coordinator validates the request, resolves
classes, and determines shard ownership (one replica per shard, least-loaded
strategy). It then sends Prepare to all nodes concurrently. Each participant
reserves an export slot (one active export at a time via CAS), starts
snapshotting all assigned shards in a background goroutine, and arms a 30 s
auto-abort timer. Prepare returns immediately; the snapshot may still be in
flight.

**Phase 2 -- Commit.** Sent to all nodes concurrently. Each participant waits
for its snapshot goroutine (`<-pending.done`), initializes the backend (e.g.
S3 client verification), then launches the scan+upload in a background
goroutine. The `Export()` HTTP response is sent only after all nodes have been
committed; scanning continues in background goroutines.

**Abort.** Any failure during Prepare or Commit triggers `abortAll()`: a
best-effort sweep using a fresh 15 s context. User-initiated cancel
(`DELETE /v1/export/{id}`) follows the same path and writes `CANCELED`
metadata. The 30 s auto-abort timer prevents leaked reservations if the
coordinator crashes between phases.

## Snapshots

Snapshots use **hard-linked LSM segment files** so the long-running scan never
holds locks and concurrent writes continue unblocked.

**Loaded shards:** Pause compaction and flush cycle, flush memtable (= the
point-in-time cutoff), hard-link `.db`/`.bloom`/`.cna` files into a snapshot
directory, resume both cycles. Concurrent writers are not blocked -- writes
landing after the flush are excluded.

_**Unloaded shards** (cold tenants): Hard-link directly from disk including WAL
(may contain unflushed data). Offloaded/frozen tenants are skipped with a
`SkipReason`._

**Reading:** `NewSnapshotBucket` opens the hard-linked directory as a read-only
bucket (`WithImmutable`, compaction disabled). The snapshot directory is removed
after the scan completes.

**Safety:** Snapshot directories use a `.snapshot-` prefix -- `NewBucket`
rejects it, `NewSnapshotBucket` requires it. Index startup removes the entire
`.snapshots/` directory to clean up orphans from crashes.

## Parallel Scan and Parquet Writing

Each shard snapshot is split into key ranges scanned by a worker pool
(`GOMAXPROCS * 2` workers), producing one Parquet file per range.

**Range computation:** `computeRanges` uses `QuantileKeys` to split the key
space. Range count is bounded between `count / maxObjectsPerRange` and
`count / minObjectsPerRange` (50K--500K objects, targeting ~2--3 GB files after
Zstd compression).

**Per-range pipeline:** Each `scanJob` creates an `io.Pipe` connecting a
`ParquetWriter` (10K-row buffer, Zstd, 8 MB page buffer) to a backend upload
goroutine. The scan seeks to its start key with a bucket cursor, deserializes
each object via `storobj.ExportFieldsFromBinary`, and writes a `ParquetRow`.
Cleanup goroutines shut down snapshot buckets and remove directories after all
ranges of a shard complete.

**File naming:** `{className}_{shardName}_{rangeIndex:04d}.parquet`. Collection
and tenant names are stored as file-level Parquet metadata, not as row columns.

## Status and Monitoring

### Files on the Backend

```
{homeDir}/
  export_metadata.json                              # source of truth
  node_{nodeName}_status.json                       # per node, written every 10 s
  {className}_{shardName}_{rangeIndex:04d}.parquet
```

### States

Export-level: `STARTED` → `TRANSFERRING` (live only, never persisted) →
`SUCCESS` / `FAILED` / `CANCELED`.
Shard-level: `TRANSFERRING`, `SUCCESS`, `FAILED`, `SKIPPED`.

### Progress Reporting

Lock-free: `ParquetWriter.Flush` → `onFlush` callback →
`ShardProgress.objectsWritten.Add` (atomic). The status writer goroutine
(every 10 s) calls `SyncAndSnapshot()` to copy atomics into the JSON-visible
field and writes the result to the backend.

### Status Assembly and Metadata Promotion

`Scheduler.Status()` and `Participant.tryPromoteMetadata()` both read per-node
status files, check liveness, and promote metadata to a terminal state if all
nodes are done. Both use the same logic so the race is benign -- last writer
produces the same result.

## Concurrency

| Primitive | Purpose |
|-----------|---------|
| `mu` (Mutex) | Guards prepared state, abort timer, cancel func, pending snapshot, active slot |
| `activeExport` | One-export-at-a-time slot (CAS in Prepare, cleared in `clearAndRelease`) |
| `pending.done` (channel) | Snapshot goroutine → Commit handoff |
| `exportWg` (WaitGroup) | Graceful shutdown waits for in-flight exports |
| `ShardProgress.objectsWritten` (atomic) | Lock-free progress counter between scan workers and status writer |

**Context hierarchy:**
`shutdownCtx` → `snapshotCtx` (per Prepare) and `shutdownCtx` → `exportCtx`
(per Commit, canceled on Abort or sibling failure).

**Sibling health:** Two background goroutines per export -- status flush
(every 10 s) and sibling check (every 60 s, reads status files with
`IsRunning` RPC fallback). On sibling failure: cancel local export, best-effort
abort to remaining siblings.

## Startup and Shutdown

Startup (`configure_api.go`): Create `Participant` → register cluster API
routes → create `Scheduler` → register REST handlers.

Shutdown: `Scheduler.StartShutdown` rejects new exports, then
`Participant.StartShutdown` cancels `shutdownCtx`, then `Participant.Shutdown`
waits on `exportWg`. In-flight exports detect cancellation on the next cursor
step and exit; metadata promotion still runs before the goroutine returns.

## File Map

| File | Role |
|------|------|
| `usecases/export/scheduler.go` | 2PC coordinator, metadata I/O, status assembly |
| `usecases/export/participant.go` | Per-node: slot, snapshot, scan orchestration, sibling monitoring |
| `usecases/export/parallel_scan.go` | Key ranges, scan jobs, range writer pipeline |
| `usecases/export/parquet_writer.go` | Batched Parquet writing with Zstd |
| `usecases/export/types.go` | Request/response types, `Selector` and `BackendProvider` interfaces |
| `usecases/export/transport.go` | `ExportClient` and `NodeResolver` interfaces |
| `entities/export/status.go` | Status and shard-status enums |
| `adapters/repos/db/export.go` | `DB.SnapshotShards`, shard ownership, snapshot dispatch |
| `adapters/repos/db/lsmkv/bucket_snapshot.go` | `CreateSnapshot`, `NewSnapshotBucket`, hard-link logic |

