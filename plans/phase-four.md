# Phase 4: Expand FSM Operations (Simplified — No Streaming RPC)

## Context

Phases 1-3 of per-shard RAFT object replication are complete. Currently only `TYPE_PUT_OBJECT` flows through RAFT consensus. Phase 4 adds the remaining 5 write operations so that **all** object mutations are RAFT-replicated: delete, merge, batch put, batch delete, and add references.

**Key simplification vs. original plan:** Batch puts use the same unary `Apply` RPC as everything else. Batches are chunked at the replicator level and each chunk is applied via a separate `r.apply()` call (which handles leader routing). No streaming RPC, no new proto service methods, no new server handler.

---

## Implementation Steps

### Step 1: Update Proto Definitions

**File:** `cluster/shard/proto/messages.proto`

**1a.** Uncomment the 5 enum values in `ApplyRequest.Type` (lines 26-30):
```
TYPE_DELETE_OBJECT = 2;
TYPE_MERGE_OBJECT = 3;
TYPE_PUT_OBJECTS_BATCH = 4;
TYPE_DELETE_OBJECTS_BATCH = 5;
TYPE_ADD_REFERENCES = 6;
```

**1b.** Add 5 new request messages:

| Message | Fields | Rationale |
|---------|--------|-----------|
| `DeleteObjectRequest` | `string id`, `int64 deletion_time_unix`, `uint64 schema_version` | Simple scalars |
| `MergeObjectRequest` | `bytes merge_document_json`, `uint64 schema_version` | `MergeDocument` has `map[string]interface{}` → JSON bytes |
| `PutObjectsBatchRequest` | `repeated bytes objects`, `uint64 schema_version` | Reuses `storobj.MarshalBinary()` per object |
| `DeleteObjectsBatchRequest` | `repeated string uuids`, `int64 deletion_time_unix`, `bool dry_run`, `uint64 schema_version` | Simple scalars |
| `AddReferencesRequest` | `bytes references_json`, `uint64 schema_version` | `BatchReference` has nested refs → JSON bytes |

**1c.** No changes to `ShardReplicationService` — no new RPCs. Everything uses the existing unary `Apply`.

**1d.** Regenerate: `cd cluster/shard/proto && PATH=$PATH:~/go/bin buf generate`

---

### Step 2: Expand `shard` Interface

**File:** `cluster/shard/fsm.go` (lines 32-43)

Add 5 methods matching existing `*Shard` signatures:

```go
DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error
MergeObject(ctx context.Context, merge objects.MergeDocument) error
PutObjectBatch(ctx context.Context, objects []*storobj.Object) []error
DeleteObjectBatch(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects
AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error
```

New imports: `"time"`, `"github.com/go-openapi/strfmt"`, `"github.com/weaviate/weaviate/usecases/objects"`

---

### Step 3: Add FSM Dispatch + Handlers

**File:** `cluster/shard/fsm.go`

**3a.** Add 5 cases to `switch req.Type` (line 118):

```go
case shardproto.ApplyRequest_TYPE_DELETE_OBJECT:
    applyErr = f.deleteObject(shard, &req)
case shardproto.ApplyRequest_TYPE_MERGE_OBJECT:
    applyErr = f.mergeObject(shard, &req)
case shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH:
    applyErr = f.putObjectsBatch(shard, &req)
case shardproto.ApplyRequest_TYPE_DELETE_OBJECTS_BATCH:
    applyErr = f.deleteObjectsBatch(shard, &req)
case shardproto.ApplyRequest_TYPE_ADD_REFERENCES:
    applyErr = f.addReferences(shard, &req)
```

**3b.** Implement 5 handler methods (all return `error`):

- **`deleteObject(shard, req)`** — unmarshal `DeleteObjectRequest`, convert UUID string + time, call `shard.DeleteObject()`. Only convert time when `DeletionTimeUnix != 0` (preserves zero-time semantics).

- **`mergeObject(shard, req)`** — unmarshal `MergeObjectRequest`, JSON-unmarshal `MergeDocument`, call `shard.MergeObject()`.

- **`putObjectsBatch(shard, req)`** — unmarshal `PutObjectsBatchRequest`, deserialize each via `storobj.FromBinary()`, call `shard.PutObjectBatch()`, **return first non-nil error**.

- **`deleteObjectsBatch(shard, req)`** — unmarshal `DeleteObjectsBatchRequest`, convert UUIDs + time, call `shard.DeleteObjectBatch()`, **return first `.Err` from results**.

- **`addReferences(shard, req)`** — unmarshal `AddReferencesRequest`, JSON-unmarshal `[]BatchReference`, call `shard.AddReferencesBatch()`, **return first non-nil error**.

---

### Step 4: Add Replicator Methods (Non-Batch)

**File:** `cluster/shard/replicator.go`

Add 3 non-batch methods on `*replicator` overriding the embedded `Replicator` interface. Each follows the `PutObject` pattern (line 111): serialize → `proto.Marshal()` → `s2.Encode()` → `ApplyRequest` → `r.apply()`.

| Method | Matches interface | Notes |
|--------|-------------------|-------|
| `DeleteObject` | `Replicator.DeleteObject` (line 49) | Marshal `DeleteObjectRequest`, handle zero deletion time |
| `MergeObject` | `Replicator.MergeObject` (line 56) | `json.Marshal(doc)` → `MergeObjectRequest.merge_document_json` |
| `AddReferences` | `Replicator.AddReferences` (line 46) | `json.Marshal(refs)` → `AddReferencesRequest.references_json` |

**Helper functions:**
```go
func duplicateError(err error, n int) []error
func duplicateBatchSimpleError(err error, uuids []strfmt.UUID) objects.BatchSimpleObjects
```

New imports: `"encoding/json"`, `"github.com/weaviate/weaviate/usecases/objects"`

---

### Step 5: Batch Put — Chunking + Unary Apply

**Simplified approach:** Each chunk is a separate `r.apply()` call (which internally routes to leader or forwards). No streaming RPC needed.

**5a. Chunking logic** (add to `replicator.go`):

```go
const defaultMaxBatchChunkBytes = 2 * 1024 * 1024 // 2MB per chunk

// chunkObjectBytes splits serialized objects into chunks where each chunk's
// total size stays under maxBytes. Returns chunks of the original byte slices.
func chunkObjectBytes(objectBytes [][]byte, maxBytes int) [][][]byte
```

Accumulates objects into a chunk until adding the next would exceed `maxBytes`. Single objects larger than `maxBytes` get their own chunk (at least one object per chunk).

**5b. `PutObjects` replicator method:**

```go
func (r *replicator) PutObjects(ctx, shard, objs, cl, schemaVersion) []error {
    // 1. Serialize all objects via MarshalBinary()
    // 2. Chunk by size: chunkObjectBytes(objBytes, defaultMaxBatchChunkBytes)
    // 3. For each chunk:
    //    a. Build PutObjectsBatchRequest{Objects: chunk, SchemaVersion}
    //    b. proto.Marshal → s2.Encode → ApplyRequest{TYPE_PUT_OBJECTS_BATCH}
    //    c. r.apply(ctx, req)  ← standard unary call, handles leader routing
    //    d. If error → return duplicateError(err, len(objs))
    // 4. Return make([]error, len(objs))  // all nil on success
}
```

Each chunk goes through the existing `r.apply()` → `forwardToLeader()` path. No streaming, no new server code.

---

### Step 6: Batch Delete Replicator Method

**File:** `cluster/shard/replicator.go`

Uses the existing unary `r.apply()` (no chunking — payloads are small UUIDs):

| Method | Notes |
|--------|-------|
| `DeleteObjects` | **Skip RAFT when `dryRun=true`** (read-only → delegate to backing replicator). For `dryRun=false`, marshal `DeleteObjectsBatchRequest`, call `r.apply()`, expand error to `[]objects.BatchSimpleObject`. |

---

### Step 7: Expand `ShardRaftReplicator` Interface

**File:** `adapters/repos/db/shard_raft_replicator.go` (lines 23-36)

Add 5 new methods:
```go
DeleteObject(ctx, className, shardName string, id strfmt.UUID, deletionTime time.Time, schemaVersion uint64) error
MergeObject(ctx, className, shardName string, doc *objects.MergeDocument, schemaVersion uint64) error
PutObjects(ctx, className, shardName string, objs []*storobj.Object, schemaVersion uint64) []error
DeleteObjects(ctx, className, shardName string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) objects.BatchSimpleObjects
AddReferences(ctx, className, shardName string, refs []objects.BatchReference, schemaVersion uint64) []error
```

---

### Step 8: Regenerate Mocks

```bash
source ~/.bash_profile && make mocks
```

Regenerates `cluster/shard/mocks/mock_shard.go` with the 5 new methods.

---

### Step 9: Tests

**File:** `cluster/shard/store_test.go`

**9a.** Add builder helpers (following `buildPutObjectApplyRequest` pattern):
- `buildDeleteObjectApplyRequest(t, className, shardName, id, deletionTime)`
- `buildMergeObjectApplyRequest(t, className, shardName, doc)`
- `buildPutObjectsBatchApplyRequest(t, className, shardName, objs)`
- `buildDeleteObjectsBatchApplyRequest(t, className, shardName, uuids, deletionTime, dryRun)`
- `buildAddReferencesApplyRequest(t, className, shardName, refs)`

**9b.** FSM handler tests:

| Test | Verifies |
|------|----------|
| `TestStore_Apply_DeleteObject` | Called with correct UUID and time |
| `TestStore_Apply_DeleteObject_ShardError` | Error propagated |
| `TestStore_Apply_MergeObject` | MergeDocument fields survive JSON round-trip |
| `TestStore_Apply_MergeObject_ShardError` | Error propagated |
| `TestStore_Apply_PutObjectsBatch` | All objects deserialized correctly |
| `TestStore_Apply_PutObjectsBatch_PartialError` | First error propagated |
| `TestStore_Apply_DeleteObjectsBatch` | UUIDs and time correct |
| `TestStore_Apply_DeleteObjectsBatch_ShardError` | First error propagated |
| `TestStore_Apply_AddReferences` | References survive JSON round-trip |
| `TestStore_Apply_AddReferences_ShardError` | Error propagated |

**9c.** Chunking tests:
- `TestChunkObjectBytes_SingleChunk` — all objects fit in one chunk
- `TestChunkObjectBytes_MultipleChunks` — objects split across chunks
- `TestChunkObjectBytes_LargeObject` — single object exceeds chunk size → own chunk
- `TestChunkObjectBytes_Empty` — empty input → empty output

**9d.** Compression round-trip tests in `compression_test.go` for each new operation type.

---

## Files Modified

| File | Change |
|------|--------|
| `cluster/shard/proto/messages.proto` | Uncomment 5 enum values, add 5 request messages (no new RPCs) |
| `cluster/shard/proto/messages.pb.go` | Regenerated |
| `cluster/shard/proto/messages_grpc.pb.go` | Regenerated (unchanged — no new RPCs) |
| `cluster/shard/fsm.go` | Expand shard interface (+5 methods), 5 dispatch cases + 5 handlers |
| `cluster/shard/replicator.go` | Add 5 replicator methods, chunking logic, helpers |
| `cluster/shard/server.go` | **No changes** (existing `Apply` handler works for all types) |
| `adapters/repos/db/shard_raft_replicator.go` | Add 5 methods to interface |
| `cluster/shard/mocks/mock_shard.go` | Regenerated |
| `cluster/shard/store_test.go` | Add 5 builders + 10 FSM tests + chunking tests |
| `cluster/shard/compression_test.go` | Add compression round-trip tests |

---

## Verification

1. **Regenerate proto**: `cd cluster/shard/proto && PATH=$PATH:~/go/bin buf generate`
2. **Regenerate mocks**: `source ~/.bash_profile && make mocks`
3. **Build**: `go build ./cluster/shard/...`
4. **Unit tests**: `go test -race -count 1 ./cluster/shard/...`
5. **Lint**: `source ~/.bash_profile && golangci-lint run ./cluster/shard/...`
6. **Interface check**: `go build ./adapters/repos/db/...` (verify expanded interface compiles)
