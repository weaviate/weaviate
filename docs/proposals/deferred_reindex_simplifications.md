# Deferred reindex simplifications

Two simplifications that the multi-agent scout pass identified as
worthwhile but that I deliberately did NOT apply autonomously on
branch `runtime-reindex-wip`. Each touches either a crash-safety path
or the hottest write hook, and warrants a human reviewer in the loop.

## 1. Share Add/Delete callback boilerplate via `withPropBucket`

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

## 2. Inline `readPropsToReindex` into `getPropsToReindex` with a `bool` flag

**File:** `adapters/repos/db/inverted_reindex_task_generic.go` (~lines 1497–1523)

**Current shape.** Two methods, near-identical bodies. `readPropsToReindex`
returns `[]string{}` if no props saved; `getPropsToReindex` instead calls
`findPropsToReindex` + saves. Called inconsistently across the file (the
per-shard run hooks use `read`, `OnAfterLsmInit` uses `get`).

**Proposed.** One method with an explicit `discoverAndSave bool` arg.

**Why deferred.** The two callers serve materially different shapes: the
`read` callers only want the props that were already recorded and must
not discover new ones; the `get` caller wants discovery.
A bool-parameterized unified method forces every `read` caller to
either invent a `shard` or accept a `nil` parameter that the helper has
to defensively check. The simplification trades clarity for a marginal
line saving.

**Recommendation.** Probably leave as-is. If the two-method duplication
ever does become a maintenance burden, the cleaner refactor is to
move discovery out of `getPropsToReindex` entirely (let the
`OnAfterLsmInit` callsite do it explicitly), then collapse what's left.

---

_Both deferrals above were re-evaluated for the v1.38 Preview merge of
runtime reindex and kept deferred. Item 1 remains risk-gated on the
same hot-path concern the original deferral documented; item 2 stays
as-is per its own recommendation. Future re-evaluations should append a
dated note rather than rewriting this footer — the deferral history is
the value._
