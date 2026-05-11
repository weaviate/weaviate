# Single-replica backup for replicated collections

## Context

Today a backup of a replicated collection archives **every replica of every shard**
independently under `{backupID}/{nodeName}/...`. For an RF=3 collection that is 3×
the storage and transfer of the actual data, even though converged replicas are
byte-identical.

The recently-landed async-checkpoint feature (PR #10926) gives us a way to *prove*
two replicas hold identical shard data: a checkpoint's `Root` digest converges to the
same value across all replicas of a shard once async replication has settled. This
plan uses that to back up each converged shard from **one** replica, while shards that
cannot be proven identical fall back to today's all-replica behavior.

**Outcome:** an opt-in `dedupeReplicas` backup mode that, for converged shards, stores a
single copy and — at restore — fans that copy out to every replica node so the restored
cluster is fully replicated immediately.

### Locked design decisions
1. **Restore = fan out.** The single archived shard copy is restored to every node in
   that shard's `BelongsToNodes`. No reliance on post-restore async-rep healing.
2. **Non-converged shards = per-shard fallback.** Converged shards use one replica;
   shards that don't converge within a timeout back up all their replicas (today's
   behavior). Backup always succeeds and is always complete.
3. **Opt-in.** New backup-request flag `dedupeReplicas` (default false). Existing
   backup/restore behavior is byte-for-byte unchanged when the flag is off.

## Async-checkpoint API this builds on (already implemented)

- `DB.CreateAsyncCheckpoints(ctx, className, cutoffMs, shards) error` — best-effort fan-out.
- `DB.GetAsyncCheckpointNodeStatuses(ctx, className, shards) (map[shard][]replica.AsyncCheckpointNodeStatus, error)`.
- `DB.DeleteAsyncCheckpoints(ctx, className, shards) error` — idempotent.
- `replica.AsyncCheckpointNodeStatus{ Node string; CutoffMs int64; CreatedAt time.Time; Root hashtree.Digest }`.
- A shard is **converged** when every node entry reports an identical non-zero `Root`
  digest AND identical `CreatedAt`. (`Root == hashtree.Digest{}` ⇒ no active checkpoint.)
- DB methods live in `adapters/repos/db/replication.go` (~L262–282).

## Implementation

### 1. Opt-in flag (`dedupeReplicas`)

- `openapi-specs/schema.json` → `BackupConfig`: add `"DedupeReplicas": {"type": "boolean", "default": false, "x-nullable": false}`.
- Run `./tools/gen-code-from-swagger.sh` to regenerate `entities/models/backup_config.go`.
- Thread it: REST backup handler → `backup.Request` (`usecases/backup/transport.go`,
  add `DedupeReplicas bool`) → `coordinator.Backup`.
- `planDesignatedShards` (below) only runs when `req.DedupeReplicas` is true.

### 2. Protocol change — per-class shard allowlist

`backup.NodeDescriptor` (`entities/backup/descriptor.go:36`) carries only `Classes []string`.
Add an optional, wire-compatible field:

```go
// nil ⇒ back up all local shards (legacy behavior)
ShardAllowList map[string][]string `json:"shardAllowList,omitempty"` // class -> shards this node archives
```

- Add the parallel field to `Request` (`usecases/backup/transport.go`), populated when the
  coordinator fills `reqChan` in `canCommit` (`coordinator.go` ~L491+) from each group's
  allowlist.
- Node consults it in `Index.descriptor` → `readSchema` (`adapters/repos/db/backup.go:748-766`):
  after the existing `IsLocalShard` filter, if an allowlist exists for the class, intersect
  the local shard set with it. Thread the allowlist from `Request` → `Handler.OnCanCommit`
  (`usecases/backup/handler.go`) → `DB.BackupDescriptors` → `idx.descriptor`.
- Old coordinator sends nil ⇒ unchanged. Old node ignores the field ⇒ degrades to
  all-replica (safe, just not deduped) — but version-gate in §6.

### 3. Backup-side orchestration (Coordinator)

New method `planDesignatedShards(ctx, classes, cutoffMs)` called inside `coordinator.Backup`
(`usecases/backup/coordinator.go:163`) **before** `canCommit` (L198) — convergence polling
must not run inside the 8s `_TimeoutCanCommit` budget.

Steps:
- **(a) Create checkpoints.** For each requested class, read sharding state (extend the
  `Selector` interface, `coordinator.go:68`, with `ShardingState(ctx, class) (*sharding.State, error)`
  backed by `DB.schemaReader`). Skip classes where every `Physical[shard].BelongsToNodes`
  has len ≤ 1 (RF=1 — no-op). For replicated classes call `DB.CreateAsyncCheckpoints`.
  The coordinator needs the async-checkpoint API: add an `AsyncCheckpointer` interface
  (the three `replication.go` methods), implemented by `*DB`, injected alongside `selector`.
- **(b) Poll convergence.** Loop `GetAsyncCheckpointNodeStatuses` with a bounded budget
  (new const, e.g. `_TimeoutCheckpointConverge = 2 * time.Minute`, poll ~5s, `select` on
  `ctx.Done()`).
- **(c) Designate.** For each converged shard pick a deterministic node:
  `slices.Sort(BelongsToNodes)[0]` (sorted-first ⇒ stable across coordinator restarts).
  Build `map[class]map[shard]designatedNode`.
- **(d) Non-converged shards** are left out of the map ⇒ fall through to all-replica.
- **(e) Cleanup.** `DeleteAsyncCheckpoints` for every class must run on **all** exit
  paths: the `canCommit` error return (`coordinator.go:199-201`) and the end of the async
  `commit` goroutine `f` (L220-234). Put the delete in `f`'s `defer` block plus the early
  error returns. Idempotency makes double-delete safe.

Then `groupByShard` (`coordinator.go:804`): for designated shards add **only** the
designated node to that class's group and populate `ShardAllowList`; non-designated nodes
get the shard removed from their allowlist. The leader-backs-up-all rule (~L828) must also
honor the allowlist.

### 4. Descriptor metadata for fan-out

No new descriptor field strictly required:
- `ShardDescriptor.Node` (`entities/backup/descriptor.go:249`) already records which single
  node archived the copy — the fan-out **source**.
- `ClassDescriptor.ShardingState` carries serialized `sharding.State` with `BelongsToNodes`
  per shard — the fan-out **target list**.

Optionally add `ShardDescriptor.DedupedReplicas bool` for explicit validation/clarity;
it is derivable, so treat as nice-to-have.

### 5. Restore-side fan-out

Today each node restores from `{backupID}/{itsOwnNodeName}/`. For a deduped shard archived
under node X, replica nodes Y and Z must read X's archive prefix.

- Restore must fan to **all** `BelongsToNodes` regardless of who archived: in the restore
  `canCommit` path (`coordinator.Restore`, `coordinator.go:240+`), every node in a shard's
  `BelongsToNodes` becomes a restore participant.
- Hook in `restorer.restoreOne` (`usecases/backup/restorer.go:214`): split per-shard. For
  each `ShardDescriptor`, if the executing node `r.node` is in `BelongsToNodes` but
  `ShardDescriptor.Node != r.node`, build the `nodeStore`/`fileWriter` pointing at
  `{backupID}/{ShardDescriptor.Node}/` instead of the node's own prefix. `newFileWriter`
  (`restorer.go:229`) takes one `store` today — make it select the store per shard.
- Run fan-out target node names through `DistributedBackupDescriptor.ToMappedNodeName`
  (`descriptor.go:150`) so `NodeMapping` still works.

**Risk:** the `{backupID}/{nodeName}/` layout assumes a node reads only its own subtree;
cross-node prefix reads are new. Confirm the object-store backend (`BackupBackend`) allows
any node to list/read another node's prefix (same bucket — should be fine, but verify path
sanitization / no per-node ACL).

### 6. Edge cases

- **RF=1**: detected in `planDesignatedShards`; skip checkpoints entirely, behavior unchanged.
- **Multi-tenancy**: each tenant is a `Physical` shard with its own `BelongsToNodes`; the
  per-shard designation map handles tenants naturally. Inactive/FROZEN tenants have no
  checkpoint ⇒ treated as non-converged ⇒ all-replica fallback.
- **Backup aborted mid-flight**: checkpoint cleanup via the `defer` in goroutine `f` and the
  abort path; `DeleteAsyncCheckpoints` is idempotent.
- **Replica node down during backup**: never reports checkpoint status ⇒ shard never
  converges ⇒ all-replica fallback (a down node also can't be backed up — pre-existing).
- **Topology change before restore**: archived `BelongsToNodes` is stale; map via
  `ToMappedNodeName`. If the restore cluster has fewer nodes than `BelongsToNodes`, fan-out
  restores to the available subset — document as a known gap (no post-restore healing exists
  today, so a deduped restore into a smaller cluster is under-replicated until manual repair).
- **Version skew**: gate `dedupeReplicas` on all participants supporting the new
  `ShardAllowList` field — reuse the existing backup version/`ServerVersion` check; if any
  participant is too old, reject the deduped backup with a clear error rather than silently
  producing an all-replica backup.

## Suggested PR / commit decomposition

1. `Selector.ShardingState` + `AsyncCheckpointer` interface + `*DB` wiring (no behavior change).
2. `NodeDescriptor.ShardAllowList` + `Request` field + `readSchema` allowlist filtering
   (wire-compatible, default nil — fully backward compatible).
3. `planDesignatedShards` + checkpoint create/poll/delete in `coordinator.Backup`;
   `groupByShard` honors the designation map.
4. Restore fan-out: per-shard store selection in `restorer`, all-`BelongsToNodes` restore
   participants.
5. `dedupeReplicas` opt-in flag: openapi + regenerated models + REST threading + version gate.
6. Acceptance test + docs.

## Files to modify

| File | Change |
|---|---|
| `openapi-specs/schema.json` | `BackupConfig.DedupeReplicas` flag |
| `entities/models/backup_config.go` | regenerated |
| `usecases/backup/transport.go` | `Request.DedupeReplicas`, `Request.ShardAllowList` |
| `entities/backup/descriptor.go` | `NodeDescriptor.ShardAllowList` (+ optional `ShardDescriptor.DedupedReplicas`) |
| `usecases/backup/coordinator.go` | `planDesignatedShards`, `Selector.ShardingState`, `AsyncCheckpointer`, `groupByShard`, restore participant fan-out |
| `usecases/backup/handler.go` | thread allowlist into `OnCanCommit` |
| `usecases/backup/restorer.go` | per-shard store selection in `restoreOne`/`newFileWriter` |
| `adapters/repos/db/backup.go` | `readSchema` allowlist filter; implement `Selector.ShardingState` + `AsyncCheckpointer` |
| REST backup handler (`adapters/handlers/rest/.../backups`) | accept + thread `dedupeReplicas` |

## Verification

**Unit**
- Convergence predicate: identical `Root`+`CreatedAt` ⇒ converged; zero digest or mismatch ⇒ not.
- Deterministic designated-node pick (sorted-first).
- `readSchema` allowlist filtering (nil ⇒ all; subset ⇒ filtered).
- `groupByShard` with a mix of converged and non-converged shards.
- `DistributedBackupDescriptor` JSON round-trip with the new field read by old/new decoders.

**Acceptance** — extend `test/acceptance/replication/async_replication/` (3-node testcontainer):
- RF=3 collection, wait for async-rep convergence, backup with `dedupeReplicas=true`:
  assert backup ≈ 1/3 size and exactly one node subtree per shard.
- Restore, then assert all 3 nodes serve identical data (fan-out worked).
- Non-converged variant: write continuously during backup ⇒ assert all-replica fallback
  for the still-moving shards.
- Restart the coordinator mid-backup ⇒ assert checkpoints are cleaned up (no stragglers
  via `GetAsyncCheckpointStatus`).

**Local gates** before each push: `go build ./...`, `go vet ./...`,
`./tools/linter_go_routines.sh`, `golangci-lint run ./usecases/backup/... ./adapters/repos/db/...`.
