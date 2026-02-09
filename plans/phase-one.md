# Phase 1: WAL-Less LSM Infrastructure + RAFT Tuning + Compression

## Context

With the introduction of per-shard RAFT-based object replication (`cluster/shard/`), every ingested object is durably stored in three locations: the RAFT log, LSM WAL files, and LSM segments. The WAL and RAFT log serve overlapping crash-recovery purposes. A single `PutObject` triggers 15+ redundant WAL writes (one per bucket: objects, properties, etc.) that are unnecessary when the RAFT log already contains the full object.

Phase 1 builds the foundational components to eliminate this duplication in later phases. It introduces a no-op commit logger, a bucket-level WAL toggle, WAL recovery skip, shard-tuned RAFT snapshot parameters, and RAFT log entry compression. **No behavioral changes to existing shards yet** — this phase only creates the building blocks.

---

## Sub-Phase 1a: No-Op Commit Logger

**Goal:** Create `nopCommitLogger` satisfying the `memtableCommitLogger` interface with all methods as no-ops.

### File: `adapters/repos/db/lsmkv/commitlogger_nop.go` (new)

Create a new file with:
- `nopCommitLogger` struct (empty)
- All 10 interface methods as no-ops returning `nil`/zero values:
  - `writeEntry` -> `nil`
  - `put` -> `nil`
  - `append` -> `nil`
  - `add` -> `nil`
  - `walPath` -> `""`
  - `size` -> `0`
  - `flushBuffers` -> `nil`
  - `close` -> `nil`
  - `delete` -> `nil`
  - `sync` -> `nil`
- Compile-time assertion: `var _ memtableCommitLogger = (*nopCommitLogger)(nil)`

**Reference:** Interface definition at `commitlogger.go:34-45`. Pattern precedent at `adapters/repos/db/vector/hnsw/commit_logger_noop.go`.

### Test: `adapters/repos/db/lsmkv/commitlogger_nop_test.go` (new)

- Verify compile-time interface satisfaction
- Verify each method returns expected zero/nil values
- Verify no file system side effects (no `.wal` files created)

---

## Sub-Phase 1b: Bucket WAL Toggle

**Goal:** Add `WithWALDisabled()` bucket option that causes `createNewActiveMemtable()` to use `nopCommitLogger`.

### File: `adapters/repos/db/lsmkv/bucket_options.go`

Add after existing options (e.g., after `WithBM25Config` at line 275):

```go
func WithWALDisabled() BucketOption {
    return func(b *Bucket) error {
        b.walDisabled = true
        return nil
    }
}
```

### File: `adapters/repos/db/lsmkv/bucket.go`

1. Add `walDisabled bool` field to the `Bucket` struct.
2. Modify `createNewActiveMemtable()` (line 1304-1319). Replace the commit logger creation:

```go
func (b *Bucket) createNewActiveMemtable() (memtable, error) {
    path := filepath.Join(b.dir, fmt.Sprintf("segment-%d", time.Now().UnixNano()))

    var cl memtableCommitLogger
    if b.walDisabled {
        cl = &nopCommitLogger{}
    } else {
        var err error
        cl, err = newLazyCommitLogger(path, b.strategy)
        if err != nil {
            return nil, errors.Wrap(err, "init commit logger")
        }
    }

    mt, err := newMemtable(path, b.strategy, b.secondaryIndices, cl,
        b.metrics, b.logger, b.enableChecksumValidation, b.bm25Config,
        b.writeSegmentInfoIntoFileName, b.allocChecker, b.shouldSkipKey)
    if err != nil {
        return nil, err
    }

    return mt, nil
}
```

### Test: `adapters/repos/db/lsmkv/bucket_wal_disabled_test.go` (new)

- Create a bucket with `WithWALDisabled()`, write data, verify no `.wal` files in directory
- Create a bucket without the option, write data, verify `.wal` files exist (regression)
- Verify `Put`/`Get` round-trip works correctly with WAL disabled

---

## Sub-Phase 1c: Skip WAL Recovery When Disabled

**Goal:** Early-return in `mayRecoverFromCommitLogs()` when WAL is disabled.

### File: `adapters/repos/db/lsmkv/bucket_recover_from_wal.go`

Add guard at the very top of `mayRecoverFromCommitLogs()`, after the context check (line 41), before WAL file scanning (line 43):

```go
func (b *Bucket) mayRecoverFromCommitLogs(ctx context.Context, sg *SegmentGroup, files map[string]int64) (err error) {
    if err := ctx.Err(); err != nil {
        return errors.Wrap(err, "recover commit log")
    }

    // WAL-less buckets have no commit logs to recover from.
    // Durability is provided by the RAFT log instead.
    if b.walDisabled {
        return nil
    }

    // ... rest of existing code unchanged ...
}
```

### Test

Add a test case to `commitlogger_nop_test.go` or `bucket_wal_disabled_test.go`:
- Create a bucket with WAL disabled, write data, flush to segments, restart bucket, verify data is readable (no WAL recovery needed since data is in segments)

---

## Sub-Phase 1d: Shard RAFT Snapshot Configuration

**Goal:** Add `TrailingLogs` to the shard RAFT config chain, and introduce shard-specific environment variables with tuned defaults.

### Config propagation chain (4 files):

#### 1. `cluster/shard/store.go`

Add `TrailingLogs` field to `StoreConfig` (after line 82):

```go
TrailingLogs uint64
```

Apply it in `raftConfig()` (after line 237):

```go
if s.config.TrailingLogs > 0 {
    cfg.TrailingLogs = s.config.TrailingLogs
} else {
    // Shard-level default: keep fewer trailing logs than schema-level.
    // WAL cleanup has zero trailing entries; RAFT needs some until
    // out-of-band state transfer is implemented (Phase 5).
    cfg.TrailingLogs = 4096
}
```

> **Note on `TrailingLogs=0` semantics:** HashiCorp RAFT's `DefaultConfig()` sets `TrailingLogs=10240`. Setting it to `0` means "delete all logs after snapshot." Since we use the `> 0` guard pattern for other fields, we need a different approach for `TrailingLogs` to allow the explicit value `0`. Use a `*uint64` pointer or a separate boolean. However, for Phase 1d, we'll set a shard-level default of `4096` (which is always `> 0`), so the simple pattern works. We'll revisit when Phase 5d reduces it to 0.

#### 2. `cluster/shard/raft.go`

Add `TrailingLogs` to `RaftConfig` (after line 39):

```go
TrailingLogs uint64
```

Propagate in `GetOrCreateStore()` (line 123-136), add to `StoreConfig`:

```go
TrailingLogs: r.config.TrailingLogs,
```

#### 3. `cluster/shard/registry.go`

Add `TrailingLogs` to `RegistryConfig` (after line 57):

```go
TrailingLogs uint64
```

Propagate in `GetOrCreateRaft()` (line 144-154), add to `RaftConfig`:

```go
TrailingLogs: reg.config.TrailingLogs,
```

#### 4. `usecases/config/environment.go`

Add shard-specific environment variable parsing. Find where `RAFT_TRAILING_LOGS` is parsed (around line 1172) and add nearby in the `Raft` struct and `parseRAFTConfig()`:

Add fields to `Raft` struct (after existing fields around line 591):

```go
ShardSnapshotThreshold uint64
ShardSnapshotInterval  time.Duration
ShardTrailingLogs      uint64
```

Add parsing in `parseRAFTConfig()`:

```go
if err := parsePositiveInt(
    "SHARD_RAFT_SNAPSHOT_THRESHOLD",
    func(val int) { cfg.ShardSnapshotThreshold = uint64(val) },
    1024,
); err != nil {
    return cfg, err
}

if err := parsePositiveInt(
    "SHARD_RAFT_SNAPSHOT_INTERVAL",
    func(val int) { cfg.ShardSnapshotInterval = time.Second * time.Duration(val) },
    30,
); err != nil {
    return cfg, err
}

if err := parsePositiveInt(
    "SHARD_RAFT_TRAILING_LOGS",
    func(val int) { cfg.ShardTrailingLogs = uint64(val) },
    4096,
); err != nil {
    return cfg, err
}
```

#### 5. Wiring point

Find where `RegistryConfig` is constructed (in `adapters/handlers/rest/configure_api.go`) and wire the shard-specific config fields directly. The shard-specific env vars (`SHARD_RAFT_*`) have their own independent defaults (1024/30s/4096) tuned for per-shard RAFT clusters, so no fallback to schema-level values is needed.

### Test: `cluster/shard/store_test.go`

- Verify `raftConfig()` produces correct `TrailingLogs` value
- Verify shard defaults (1024/30s/4096) differ from schema defaults (8192/120s/10240)

---

## Sub-Phase 1e: RAFT Log Entry Compression

**Goal:** Compress RAFT log entries at the application level using `s2` (improved Snappy).

### File: `cluster/shard/proto/messages.proto`

Add compressed flag to `ApplyRequest` (after line 37):

```protobuf
message ApplyRequest {
  // ... existing fields ...
  bytes sub_command = 5;
  bool compressed = 6;
}
```

### Regenerate protobuf code

Run from `cluster/shard/proto/`:

```bash
buf generate
```

This uses `buf.gen.yaml` and `buf.yaml` already present in that directory.

### File: `cluster/shard/replicator.go`

In `PutObject()` (lines 111-136), compress `subCmd` after marshaling:

```go
import "github.com/klauspost/compress/s2"

// ... in PutObject(), after proto.Marshal(putReq) ...

compressed := s2.Encode(nil, subCmd)

req := &shardproto.ApplyRequest{
    Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECT,
    Class:      r.class,
    Shard:      shard,
    SubCommand: compressed,
    Compressed: true,
}
```

### File: `cluster/shard/fsm.go`

In `Apply()` (lines 81-117), decompress before dispatching:

```go
import "github.com/klauspost/compress/s2"

// ... after proto.Unmarshal(l.Data, &req) ...

if req.Compressed {
    decompressed, err := s2.Decode(nil, req.SubCommand)
    if err != nil {
        f.log.WithError(err).Error("failed to decompress sub_command")
        return Response{Version: l.Index, Error: fmt.Errorf("decompress: %v", err)}
    }
    req.SubCommand = decompressed
}
```

> **Backwards compatibility:** The `compressed` field defaults to `false` in protobuf. Old uncompressed entries replay correctly — the FSM only decompresses when `req.Compressed == true`. This makes rolling upgrades safe.

> **Why `s2` over `zstd`:** `s2` is a subpackage of `github.com/klauspost/compress` (already in `go.mod` at v1.18.0). `s2` is optimized for speed over ratio — ideal for the hot write path. The codebase uses `zstd` elsewhere (backup, async replication) where ratio matters more than latency.

### Test: `cluster/shard/compression_test.go` (new)

- Round-trip test: compress in replicator format -> decompress in FSM format -> verify object equality
- Test that uncompressed entries (`Compressed=false`) are handled correctly (rolling upgrade safety)
- Benchmark: measure compression ratio and speed on typical object payloads

---

## Verification

### Unit tests

```bash
go test ./adapters/repos/db/lsmkv/... -run "TestNopCommitLogger|TestBucketWALDisabled"
go test ./cluster/shard/... -run "TestRaftConfig|TestCompression"
```

### Build

```bash
CGO_ENABLED=0 go build -tags netgo ./cmd/weaviate-server
```

### Lint

```bash
golangci-lint run ./adapters/repos/db/lsmkv/... ./cluster/shard/...
```

### Existing tests (regression)

```bash
go test ./adapters/repos/db/lsmkv/...
go test ./cluster/shard/...
```

---

## File Summary

| Sub-Phase | File                                                   | Action                                                                |
|-----------|--------------------------------------------------------|-----------------------------------------------------------------------|
| 1a        | `adapters/repos/db/lsmkv/commitlogger_nop.go`          | **Create** — `nopCommitLogger` type                                   |
| 1a        | `adapters/repos/db/lsmkv/commitlogger_nop_test.go`     | **Create** — unit tests                                               |
| 1b        | `adapters/repos/db/lsmkv/bucket_options.go`            | **Edit** — add `WithWALDisabled()`                                    |
| 1b        | `adapters/repos/db/lsmkv/bucket.go`                    | **Edit** — add `walDisabled` field, modify `createNewActiveMemtable()`|
| 1b-c      | `adapters/repos/db/lsmkv/bucket_wal_disabled_test.go`  | **Create** — integration tests                                        |
| 1c        | `adapters/repos/db/lsmkv/bucket_recover_from_wal.go`   | **Edit** — add early-return guard                                     |
| 1d        | `cluster/shard/store.go`                               | **Edit** — `TrailingLogs` in `StoreConfig` + `raftConfig()`           |
| 1d        | `cluster/shard/raft.go`                                | **Edit** — `TrailingLogs` in `RaftConfig` + propagation               |
| 1d        | `cluster/shard/registry.go`                            | **Edit** — `TrailingLogs` in `RegistryConfig` + propagation           |
| 1d        | `usecases/config/environment.go`                       | **Edit** — shard RAFT env vars + parsing                              |
| 1d        | `adapters/handlers/rest/configure_api.go`              | **Edit** — connect env vars to `RegistryConfig`                       |
| 1e        | `cluster/shard/proto/messages.proto`                   | **Edit** — add `compressed` field                                     |
| 1e        | `cluster/shard/proto/messages.pb.go`                   | **Regenerate** via `buf generate`                                     |
| 1e        | `cluster/shard/proto/messages_grpc.pb.go`              | **Regenerate** via `buf generate`                                     |
| 1e        | `cluster/shard/replicator.go`                          | **Edit** — compress `subCmd` with `s2`                                |
| 1e        | `cluster/shard/fsm.go`                                 | **Edit** — decompress in `Apply()`                                    |
| 1e        | `cluster/shard/compression_test.go`                    | **Create** — round-trip + compat tests                                |

## Implementation Order

1. **1a** first (nop logger) — no dependencies
2. **1b** next (WAL toggle) — depends on 1a
3. **1c** next (skip recovery) — depends on 1b's `walDisabled` field
4. **1d** in parallel with 1a-c (independent config work)
5. **1e** in parallel with 1a-c (independent proto/compression work)
