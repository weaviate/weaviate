# Runtime reindex — deferred finalize and per-migration generation

This document explains two load-bearing design decisions in the runtime
reindex feature (the in-process bucket migration triggered by `PUT
/v1/schema/{class}/indexes/{prop}`):

1. **Why directory renames are deferred to next-restart finalize.**
2. **Why each migration takes a per-node generation suffix on every
   sidecar bucket dir and tracker dir.**

Both decisions came out of debugging Sev 1 data-loss bugs
(weaviate/weaviate#10675 in particular). The intent is that anyone
touching this code in the future understands the *why* before changing
the *what*.

## TL;DR

- `Bucket.dir` for the live main bucket can legitimately point at a
  sidecar-suffixed path on disk (`property_<p>_<index>__<suffix>_<N>/`).
  This is the in-memory state immediately after `SwapBucketPointer`;
  the physical dir rename to canonical `property_<p>_<index>/` happens
  at the next startup via `FinalizeCompletedMigrations`.
- **Never** add a code path that calls `Store.FinalizeBucketSwap` (or
  equivalent) while the bucket is live and serving queries. The rename
  step rewrites every segment path in memory and creates a race with
  ongoing reads/compactions/flushes.
- A new migration on the same `(prop, indexType)` tuple is given a
  fresh generation number `N+1` so its sidecar dirs don't collide with
  the prior migration's deferred-finalize state.

## Why renames are deferred

After a successful `runtimeSwap`, the in-memory state on each node is:

- `bucketsByName[mainName]` → the bucket instance that was the *ingest*
  bucket.
- That bucket's `dir` field still points at the ingest dir
  (`property_<p>_<index>__<ingestSuffix>_<N>/`); no filesystem rename
  has happened.
- The old main bucket has been `Shutdown`'d and its dir renamed to
  `…__<backupSuffix>_<N>/` for crash safety.

The natural next step would be: rename the live bucket's dir from
`…__<ingestSuffix>_<N>/` to canonical `property_<p>_<index>/`. There is
even a primitive — `Store.FinalizeBucketSwap` — that does exactly this
(flush memtable, `os.Rename` the dir, rewrite every segment path in
memory, swap in a fresh active memtable).

**It is not safe to call at runtime.** Its godoc spells this out:

> IMPORTANT: This MUST only be called during startup, before the bucket
> serves any queries. It renames the bucket's on-disk directory and
> rewrites all in-memory segment paths. Calling this on a live,
> query-serving bucket will cause data races, stale file handles, and
> potential data loss.

Specifically:

- Step 3 (`os.Rename(currentDir, canonicalDir)`) and step 4
  (`updateBucketDir` rewriting `bucket.disk.dir` plus every segment's
  in-memory `.path`) form a non-atomic window. A concurrent compaction
  reading `bucket.disk.dir` sees the old path, tries to write a new
  segment there post-rename, and fails with `ENOENT`. Frontend Claude
  observed exactly this in production (`rename ...l0.s5.db
  ...deleteme: no such file`).
- Even pausing compactions doesn't fix it cleanly: reads in flight also
  hold paths from the consistent-view-of-segments snapshot, and any
  defer-callback that touches `bucket.disk.dir` mid-update would crash
  or scribble onto the wrong path.
- The cost-benefit is wrong: the rename is purely a tidiness step. The
  bucket already serves correct data from the ingest-named dir; the
  rename only matters across a process restart, so deferring it to
  next-startup `FinalizeCompletedMigrations` (when no buckets are
  loaded) is correct and zero-risk.

So **do not** "fix" the cosmetic on-disk name at runtime. The deferred
rename is intentional.

## Why per-migration generation suffixes

Once the first design decision is in place, a second problem appears:
back-to-back migrations on the same property.

After `T_N` tidied:

- `bucketsByName[mainName]` → the gen-N ingest bucket.
- That bucket's `dir` still references `…__<ingestSuffix>_<N>/`.
- The next migration `T_(N+1)` starts. If it reused the same suffix
  (no generation), it would attempt to create a new ingest bucket at
  the same path the live main is currently serving from →
  `GlobalBucketRegistry` collision and/or two `*Bucket` structs
  pointing at the same physical directory.

The fix is to give each migration its own gen-suffixed sidecar paths:

```
T_(N+1) on prop=text:
  reindex dir : property_text_searchable__retokenize_reindex_<N+1>
  ingest dir  : property_text_searchable__retokenize_ingest_<N+1>
  backup dir  : property_text_searchable__retokenize_backup_<N+1>
  tracker dir : .migrations/searchable_retokenize_text_<N+1>
```

Now there is no path collision with the gen-N state still on disk.
`T_(N+1)`'s `runtimeSwap` proceeds normally; its `SwapBucketPointer`
replaces the gen-N bucket pointer with the gen-N+1 one; the old gen-N
bucket is shut down and its dir is renamed to
`property_text_searchable__retokenize_backup_<N+1>/`.

### Generation is per-node, not in the RAFT payload

Each node computes its own gen by scanning its own disk. The RAFT
payload (`ReindexTaskPayload`) does **not** carry the gen. Different
nodes may use different gens for the same RAFT task — and that's
correct: the gen is purely a per-node implementation detail of the
deferred-finalize. A node that restarted between `T_N` and `T_(N+1)`
will have promoted gen N to canonical at startup, so on that node
`T_(N+1)` picks gen 1; a node that didn't restart picks gen N+1. The
cluster-wide logical state still converges via the regular
swap-then-flip pipeline.

### Trim at end of swap keeps depth bounded

After `T_N` tidies, the per-shard `runtimeSwap` does an in-process trim
that deletes every older gen's sidecar dirs (reindex / ingest / backup)
and tracker dir. The invariant: at any time, on disk for any `(prop,
indexType)`, there is **at most one tidied generation plus at most one
in-flight generation** (the trim runs at end of swap, so two tidied
gens can only coexist if the trim crashed between
`markTidied` and its os.RemoveAll loop).

`FinalizeCompletedMigrations` at next startup handles every shape
defensively:

- For each namespace (strategy prefix + props suffix), find the highest
  tidied gen `T` and promote it (`os.Rename` the ingest dir to
  canonical, remove its backup).
- Clean up every gen `< T` regardless of tidied state (their data was
  superseded).
- Leave gens `> T` alone — they're in-flight, recovery handles them via
  `DiscoverInFlightReindexTasks` reading the gen from the dir name.

## What's safe to change

- `runtimeSwap` internals (the merge / prepend / pointer-swap pipeline)
  can be reworked freely **as long as the post-swap on-disk shape stays
  the same** (live main keeps the ingest-suffixed dir until next-restart
  finalize).
- `FinalizeCompletedMigrations` can become smarter (e.g. recovering
  from partial trim states) — it already groups dirs by namespace and
  picks the highest tidied per namespace.
- The trim logic at the end of `runtimeSwap` is best-effort; if it
  fails to remove a stale dir, restart-finalize cleans up. Adding more
  paranoia (atomic deletes, partial-failure recovery) is fine.

## What's not safe to change

- **Do not** call `Store.FinalizeBucketSwap` at runtime to rename the
  live bucket's dir. This is the single biggest landmine in the
  reindex code path — every previous attempt to "clean up the
  cosmetic naming" has produced a Sev 1.
- **Do not** rename the live main bucket's physical directory while
  it is loaded, by any means. The mmap'd segments inside survive
  `os.RemoveAll` (POSIX unlink-while-open) and can serve reads from
  cached pages, but new segment writes will land in a missing dir and
  silently lose data — Frontend Claude's reproduction in #10675 is
  exactly this failure mode.
- **Do not** put the generation in the RAFT payload to "make all nodes
  agree". The whole point of the deferred-finalize design is that each
  node's on-disk state is its own — forcing cluster-wide agreement on a
  per-node implementation detail would re-introduce the collisions the
  per-node gen was created to avoid.

## Files of interest

- `adapters/repos/db/inverted_reindex_finalize.go` — the multi-gen
  `FinalizeCompletedMigrations`, `nextMigrationGeneration`,
  `maxMigrationGeneration` helpers.
- `adapters/repos/db/inverted_reindex_strategy_dir_names.go` —
  `genSuffix`, `parseMigrationDirName`, plus the per-strategy migration
  dir prefix constants.
- `adapters/repos/db/inverted_reindex_task_generic.go` — `runtimeSwap`
  (search for the "Step 7: Trim older generations" block).
- `adapters/repos/db/lsmkv/store.go` — `SwapBucketPointer` (the
  in-memory swap that registers the deferred state),
  `FinalizeBucketSwap` (the startup-only physical rename).
- `adapters/repos/db/reindex_provider.go` — `createReindexTasks` (where
  the gen is computed per-shard at task start) and `OnGroupCompleted`
  rehydrate path (where the gen is recovered from disk).

## Tests of interest

- `test/acceptance/reindex_multinode/round_trip_test.go` — the original
  word→field→word pin for #10675.
- `test/acceptance/reindex_multinode/restart_matrix_test.go` — every
  restart × migration-count cell (R0/R1/R1b/R2/R2b/R3/R5 + two rolling
  restart variants).
- `test/acceptance/reindex_multinode/round_trip_adjacent_test.go` —
  adjacent journeys (multi-round, different tokenizations,
  filterable-only, searchable-only, enable-then-change, MT,
  concurrent-different-props).
- `adapters/repos/db/inverted_reindex_finalize_test.go` — unit tests
  for `parseMigrationDirName`, `nextMigrationGeneration`, multi-gen
  `FinalizeCompletedMigrations` paths.
