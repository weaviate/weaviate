# Deferred reindex simplifications

Three simplifications that the multi-agent scout pass identified as
worthwhile but that I deliberately did NOT apply autonomously on
branch `runtime-reindex-wip`. Each touches either a crash-safety path
or the hottest write hook, and warrants a human reviewer in the loop.

## 1. Unify `swapIngestAndBackupBuckets` and `unswapIngestAndBackupBuckets`

**File:** `adapters/repos/db/inverted_reindex_task_generic.go` (~lines 1125–1223)

**Current shape.** Two ~50-line mirror-image methods:
- `swapIngestAndBackupBuckets`: `main → backup`, `ingest → main`, `markSwappedProp`.
- `unswapIngestAndBackupBuckets`: `main → ingest`, `backup → main`, `unmarkSwappedProp`, guarded by inverted `IsSwappedProp` checks.

**Proposed.** A `renameProps(ctx, shard, props, direction)` helper parameterized on three rename triples and a mark/unmark fn. Saves ~50 lines.

**Why deferred.** Rename order matters for crash recovery. The
`recoverRuntimeSwapBuckets` switch reads `mainExists` / `backupExists`
to decide which recovery recipe to run, and that decision depends on
the exact rename sequence each direction performs. A reorder during
refactor would silently change which recovery branch fires on a
post-crash startup.

**Risk gating.** Needs a reviewer who understands the on-disk
state-transition diagram to sign off, plus a crash-during-swap
acceptance test that exercises every sentinel boundary in both
directions.

## 2. Share Add/Delete callback boilerplate via `withPropBucket`

**Files:** Add+Delete callbacks in all seven `inverted_reindex_strategy_*.go` files
(e.g. `enable_filterable.go:101–139`, `rangeable.go:95–137`, `roaringset.go:80–122`).

**Current shape.** Every callback opens with 4–6 lines of identical
boilerplate: optional `HasFilterableIndex` / `HasSearchableIndex` gate,
`propsByName` membership check, `bucketNamer` → `shard.store.Bucket(bucketName)`.
The per-strategy unique part is the inner per-item loop.

**Proposed.** Helper `withPropBucket(propsByName, bucketNamer, gate, fn)` that
returns the closure. Each strategy provides only the inner per-item loop body.

**Why deferred.** These are the hottest hooks in the system — they run on
every Add/Delete on every property targeted by an in-flight reindex,
across every shard. Concurrency correctness and per-strategy gate
readability matter more here than line count. A helper that obscures
which gate fires (or worse, that gets the gate wrong on one strategy
and we don't notice until production) is a regression.

**Risk gating.** Worth doing, but only with a reviewer who has the
per-strategy semantics paged in. Add benchmarks before and after to
confirm no allocation regression in the hot path.

## 3. Inline `readPropsToReindex` into `getPropsToReindex` with a `bool` flag

**File:** `adapters/repos/db/inverted_reindex_task_generic.go` (~lines 1497–1523)

**Current shape.** Two methods, near-identical bodies. `readPropsToReindex`
returns `[]string{}` if no props saved; `getPropsToReindex` instead calls
`findPropsToReindex` + saves. Called inconsistently across the file
(`OnBeforeLsmInit` uses `read`, `OnAfterLsmInit` uses `get`).

**Proposed.** One method with an explicit `discoverAndSave bool` arg.

**Why deferred.** The two callers serve materially different shapes: the
`read` callers don't have a `shard` handy (they run before LSM init)
and don't want one; the `get` caller has the shard and wants discovery.
A bool-parameterized unified method forces every `read` caller to
either invent a `shard` or accept a `nil` parameter that the helper has
to defensively check. The simplification trades clarity for a marginal
line saving.

**Recommendation.** Probably leave as-is. If the two-method duplication
ever does become a maintenance burden, the cleaner refactor is to
move discovery out of `getPropsToReindex` entirely (let the
`OnAfterLsmInit` callsite do it explicitly), then collapse what's left.

---

_Each of the three deferrals above was re-evaluated for the v1.38
Preview merge of runtime reindex and kept deferred. Items 1 and 2
remain risk-gated on the same crash-recovery / hot-path concerns the
original deferral documented; item 3 stays as-is per its own
recommendation. Future re-evaluations should append a dated note rather
than rewriting this footer — the deferral history is the value._
