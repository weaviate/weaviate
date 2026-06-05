# Phase 3: Write Path Integration

## Context

This is Phase 3 of RAFT-based object replication (`plans/phases.md`). Phases 1a-c (WAL-less LSM infrastructure) and Phase 2 (snapshot-flush safety) are already complete on the `obj-raft/phase-3` branch:

- `nopCommitLogger` exists (`lsmkv/commitlogger_nop.go`)
- `WithWALDisabled()` bucket option exists (`lsmkv/bucket_options.go:290`)
- `createNewActiveMemtable()` uses `nopCommitLogger` when `walDisabled` (`lsmkv/bucket.go:1308-1320`)
- WAL recovery skip implemented (`lsmkv/bucket_recover_from_wal.go:44-46`)
- `FlushMemtables()` on FSM shard interface + Shard implementation + FSM snapshot integration all done

**Goal:** Wire WAL-less mode into the shard write path so RAFT-replicated shards create all LSM buckets without WALs and skip unnecessary WAL flush calls. Non-RAFT shards must be completely unaffected.

---

## Step 1: Shard RAFT Detection

**File:** `adapters/repos/db/shard.go`

Add an unexported method to `*Shard`:

```go
// isRaftReplicated returns true if this shard is backed by a RAFT cluster
// for replication. When true, durability is provided by the RAFT log instead
// of per-bucket WALs.
func (s *Shard) isRaftReplicated() bool {
    return s.index.Config.RaftReplicationEnabled && s.index.raft != nil
}
```

**Why this works:** `index.raft` is set in `NewIndex()` (`index.go:407-413`) before any shards are created. By the time `makeDefaultBucketOptions()` is called during shard init (`shard_init.go:146`), the index-level RAFT field is already populated.

**Why unexported:** Only shard-internal code needs this check. The `ShardLike` interface does not need to be modified.

---

## Step 2: Propagate WAL-Disabled to Bucket Creation

**File:** `adapters/repos/db/shard_bucket_options.go`

In `makeDefaultBucketOptions()` (line 20), conditionally append `lsmkv.WithWALDisabled()` when the shard is RAFT-replicated:

```go
func (s *Shard) makeDefaultBucketOptions(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption {
    options := []lsmkv.BucketOption{
        // ... existing options ...
    }

    if s.isRaftReplicated() {
        options = append(options, lsmkv.WithWALDisabled())
    }

    // ... existing switch on strategy ...

    return append(options, customOptions...)
}
```

**Coverage:** This single change covers ALL bucket types because every bucket is created through `makeDefaultBucketOptions`:
- Object bucket (`shard_init_lsm.go:149`)
- Property buckets — filterable, searchable, rangeable (`shard_init_properties.go:113,121,148`)
- ID, creation time, update time properties (`shard_init_properties.go:198,257,264`)
- Dimensions bucket (`shard_init_properties.go:217`)
- Vector index buckets via `MakeBucketOptions` (`shard_init_vector.go:79-82`)
- Reindexer buckets (`inverted_reindexer_map_to_blockmax.go:1110`)
- Also covered by `overwrittenMakeDefaultBucketOptions()` (line 66) which delegates to `makeDefaultBucketOptions`

---

## Step 3: Guard WAL Flush Calls

### 3a: Add helper method on Shard

**File:** `adapters/repos/db/shard.go`

```go
// writeWALs flushes all WAL buffers to disk. For RAFT-replicated shards,
// this is a no-op since durability is provided by the RAFT log.
func (s *Shard) writeWALs() error {
    if s.isRaftReplicated() {
        return nil
    }
    return s.store.WriteWALs()
}
```

### 3b: Update single-operation write paths

Replace `s.store.WriteWALs()` with `s.writeWALs()` in three files:

| File | Line | Method |
|------|------|--------|
| `adapters/repos/db/shard_write_put.go` | 78 | `putOne()` |
| `adapters/repos/db/shard_write_delete.go` | 87 | `DeleteObject()` |
| `adapters/repos/db/shard_write_merge.go` | 110 | `merge()` |

Each is a simple replacement: `s.store.WriteWALs()` → `s.writeWALs()`.

### 3c: Batch operations — no changes needed

The batch `flushWALs()` methods on the batcher structs (`objectsBatcher`, `deleteObjectsBatcher`, `referencesBatcher`) access the store via the `ShardLike` interface: `ob.shard.Store().WriteWALs()`.

**No guard needed here** because:
1. All buckets already use `nopCommitLogger` (from Step 2), so `WriteWALs()` → `bucket.WriteWAL()` → `memtable.writeWAL()` → `nopCommitLogger.flushBuffers()` → `return nil`. The entire call chain is already a no-op.
2. The overhead (lock acquisition + bucket iteration with no-op calls) is negligible in the context of a batch operation.
3. The batch `flushWALs()` methods also flush vector index queues and property length trackers, which must continue regardless of RAFT mode.
4. Avoiding `ShardLike` interface changes keeps the blast radius minimal.

---

## Step 4: Unit Tests

**File:** `adapters/repos/db/shard_write_put_test.go` (or new test file `adapters/repos/db/shard_raft_wal_test.go`)

Tests to add:

1. **`isRaftReplicated()` returns correct value** — verify it returns `true` when `RaftReplicationEnabled && raft != nil`, and `false` otherwise.

2. **WAL-disabled option propagation** — verify that `makeDefaultBucketOptions()` includes `WithWALDisabled()` for RAFT-replicated shards and does not include it for non-RAFT shards.

3. **`writeWALs()` guard** — verify that `writeWALs()` skips `store.WriteWALs()` for RAFT-replicated shards and calls it for non-RAFT shards.

For testing patterns, look at existing shard tests in the `db` package — shards are created with `NewShard()` using an `Index` that has `Config.RaftReplicationEnabled` and `raft` fields.

---

## Files Modified (Summary)

| File | Change |
|------|--------|
| `adapters/repos/db/shard.go` | Add `isRaftReplicated()` and `writeWALs()` methods |
| `adapters/repos/db/shard_bucket_options.go` | Add `WithWALDisabled()` in `makeDefaultBucketOptions()` when RAFT-replicated |
| `adapters/repos/db/shard_write_put.go` | `s.store.WriteWALs()` → `s.writeWALs()` (line 78) |
| `adapters/repos/db/shard_write_delete.go` | `s.store.WriteWALs()` → `s.writeWALs()` (line 87) |
| `adapters/repos/db/shard_write_merge.go` | `s.store.WriteWALs()` → `s.writeWALs()` (line 110) |
| `adapters/repos/db/shard_raft_wal_test.go` | New test file for RAFT WAL integration |

---

## Verification

1. **Build:** `go build ./adapters/repos/db/...`
2. **Unit tests:** `go test -count 1 -race ./adapters/repos/db/...`
3. **LSM tests:** `go test -count 1 -race ./adapters/repos/db/lsmkv/...` (regression — should be unaffected)
4. **Shard RAFT tests:** `go test -count 1 -race ./cluster/shard/...`
5. **Verify no `.wal` files** for RAFT-replicated shards in integration/manual test
6. **Verify `.wal` files still exist** for non-RAFT shards (regression check)
