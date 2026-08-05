# Phase 2: Snapshot-Flush Safety

## Context

With Phase 1 complete (WAL-less LSM infrastructure, shard RAFT config, log compression), we need to enforce the critical safety invariant that makes WAL elimination safe: **RAFT log entries must not be truncated until all their data is flushed to LSM segments.**

The fix: flush all memtables to disk **before** the snapshot is persisted. This is the bridge between RAFT durability and LSM durability.

### Why Persist(), not Snapshot()

The initial implementation placed `FlushMemtables()` inside `FSM.Snapshot()`. This is problematic because hashicorp/raft's threading model calls `Snapshot()` on the **same goroutine** as `Apply()` — meaning the flush blocks all new applies for its entire duration. The hashicorp/raft documentation is explicit:

> "The Snapshot implementation should return quickly, because Apply can not be called while Snapshot is running. Generally this means Snapshot should only capture a pointer to the state, and any expensive IO should happen as part of FSMSnapshot.Persist."
>
> "Apply and Snapshot are always called from the same thread, but Apply will be called concurrently with FSMSnapshot.Persist."

**Source:** `fsm.go:29-36` in `github.com/hashicorp/raft@v1.7.2`

`Persist()` runs on a separate `runSnapshots()` goroutine and does NOT block `Apply()`. This is the correct place for the flush.

### Why the safety invariant is still maintained

The invariant is: *all data from log entries up to `lastAppliedIndex` must be in LSM segments before log truncation.*

1. `Snapshot()` captures `lastAppliedIndex = N` and a reference to the shard, then returns immediately
2. After `Snapshot()` returns, applies resume: N+1, N+2, ...
3. `Persist()` calls `FlushMemtables()` — flushes **everything** currently in memtables (entries 1..N and any newer ones)
4. After Persist completes, RAFT truncates logs up to index N (minus trailing logs)
5. All entries up to N are guaranteed in segments because the flush was a superset

---

## Implementation Steps

### Step 1: Add `FlushMemtables` to the FSM's `shard` interface

**File:** `cluster/shard/fsm.go`

Add `FlushMemtables(ctx context.Context) error` to the existing interface:

```go
type shard interface {
    PutObject(ctx context.Context, obj *storobj.Object) error
    Name() string
    FlushMemtables(ctx context.Context) error
}
```

Note: This is the FSM's local `shard` interface (unexported), not the adapter-level `ShardLike`. Only `*Shard` is passed to `SetShard` (via `shard_init.go:187`), so only `*Shard` needs to satisfy it.

### Step 2: Implement `FlushMemtables` on `*Shard`

**File:** `adapters/repos/db/shard.go` (new method on the `Shard` struct)

Delegate to the existing, battle-tested `lsmkv.Store.FlushMemtables()` (`lsmkv/store_backup.go:74`):

```go
func (s *Shard) FlushMemtables(ctx context.Context) error {
    return s.store.FlushMemtables(ctx)
}
```

This is the same call path used by `Shard.HaltForTransfer()` in `shard_backup.go:68`. The underlying implementation deactivates flush cycle callbacks, flushes every bucket's active memtable to a segment, then reactivates callbacks.

### Step 3: Add `shard` field to `FSMSnapshot`

**File:** `cluster/shard/fsm.go`

Capture the shard reference in the snapshot so `Persist()` can call `FlushMemtables`:

```go
type FSMSnapshot struct {
    className        string
    shardName        string
    nodeID           string
    lastAppliedIndex uint64
    log              logrus.FieldLogger
    shard            shard
}
```

### Step 4: Make `FSM.Snapshot()` lightweight only capturing state of shard

**File:** `cluster/shard/fsm.go`

`Snapshot()` captures the shard reference and `lastAppliedIndex`, then returns immediately. No I/O:

```go
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
    f.log.Info("creating snapshot")

    f.mu.RLock()
    shard := f.shard
    f.mu.RUnlock()

    return &FSMSnapshot{
        className:        f.className,
        shardName:        f.shardName,
        nodeID:           f.nodeID,
        lastAppliedIndex: f.lastAppliedIndex.Load(),
        log:              f.log,
        shard:            shard,
    }, nil
}
```

### Step 5: Call `FlushMemtables` from `FSMSnapshot.Persist()` on referenced shard

**File:** `cluster/shard/fsm.go`

`Persist()` flushes memtables before writing the snapshot data. On flush error, it calls `sink.Cancel()` (the `raft.SnapshotSink` contract for aborting) so RAFT will not truncate the log:

```go
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
    defer sink.Close()

    if s.shard != nil {
        ctx := context.Background()
        if err := s.shard.FlushMemtables(ctx); err != nil {
            sink.Cancel()
            return fmt.Errorf("flush memtables before snapshot persist: %w", err)
        }
    }

    snap := shardSnapshotData{ ... }
    // ... encode + write to sink
}
```

Key behaviors:
- If shard is nil (bootstrap/startup before `SetShard`), skip flush — metadata-only snapshot is still valid
- If flush fails, `sink.Cancel()` + return error — RAFT will NOT truncate the log and will retry later
- If flush succeeds, all applied entries are guaranteed in LSM segments — safe to truncate
- Runs on the snapshot goroutine, so `Apply()` continues processing new entries concurrently

### Step 6: Regenerate mocks

Run `make mocks` to regenerate `cluster/shard/mocks/mock_shard.go` with the `FlushMemtables` method.

### Step 7: Add/update tests

**File:** `cluster/shard/store_test.go`

Tests use a `fakeSnapshotSink` (implements `raft.SnapshotSink` via `io.WriteCloser` + `ID()` + `Cancel()`) to exercise the `Persist()` path:

1. **`TestFSM_Snapshot_FlushesMemtables`** — Create FSM with mock shard, call `Snapshot()` then `Persist()` with a fake sink, verify `FlushMemtables` was called during Persist and sink was not cancelled
2. **`TestFSM_Persist_FlushError_CancelsSink`** — Mock `FlushMemtables` returning an error, call `Persist()`, verify it returns the error and the sink was cancelled
3. **`TestFSM_Snapshot_NilShard_Succeeds`** — FSM without `SetShard` called, verify both `Snapshot()` and `Persist()` succeed (no flush attempted)

These tests operate directly on the FSM (not through the full RAFT cluster) for isolation and speed.

---

## Files Modified

| File                                 | Change                                                                        |
| ------------------------------------ | ----------------------------------------------------------------------------- |
| `cluster/shard/fsm.go`              | Add `shard` field to `FSMSnapshot`; move flush from `Snapshot()` to `Persist()` |
| `adapters/repos/db/shard.go`        | Add `FlushMemtables(ctx) error` method to `Shard` struct                      |
| `cluster/shard/mocks/mock_shard.go` | Regenerated via `make mocks`                                                  |
| `cluster/shard/store_test.go`       | 3 snapshot-flush tests exercising the `Persist()` path; `fakeSnapshotSink` helper |

**Not modified:** `ShardLike` interface, `LazyLoadShard` — the FSM's `shard` interface is separate, and only `*Shard` is passed to the FSM.

---

## Verification

1. **Unit tests:** `go test ./cluster/shard/ -run "TestFSM_Snapshot|TestFSM_Persist" -v -count 1`
2. **All shard RAFT tests pass:** `go test ./cluster/shard/ -v -count 1`
3. **Compilation check:** `go build ./...` — ensures `*Shard` satisfies the expanded interface
4. **Existing LSM tests unaffected:** `go test ./adapters/repos/db/lsmkv/ -count 1`
5. **Linting:** `source ~/.bash_profile && golangci-lint run ./cluster/shard/ ./adapters/repos/db/`
