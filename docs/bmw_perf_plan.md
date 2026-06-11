# BlockMax WAND query-time performance plan

Prioritized improvements to the BlockMax WAND (BMW) keyword-search hot path,
ordered by measured impact and gated by adversarial verification of correctness.

## Combined result: whole branch vs main

Same machine, same 897MB `climate_fever` `property_text_searchable` segments,
identical deterministic queries, `BenchmarkBMWCompare` (`bm25_blockmax_baseline_test.go`)
run in a `main` git worktree vs this branch, `-count=6` (variance <3%):

| workload | metric | main | branch | Δ |
|----------|--------|------|--------|---|
| 10-term OR, no filter | ns/op | 480,425 | 327,370 | **−31.9%** |
|                       | B/op  | 251,300 | 71,060  | **−71.7%** |
|                       | allocs/op | 2,170 | 863  | **−60.2%** |
| 10-term OR, filter 0.1 | ns/op | 272,176 | 227,558 | **−16.4%** |
|                        | B/op  | 233,800 | 70,245  | **−70.0%** |
|                        | allocs/op | 1,475 | 862 | **−41.6%** |

Results are bit-for-bit identical to main (7000 queries across OR/filtered/AND;
see Validation). The aggregate is the sum of Tier 1 (allocations), Tier 2 (sort +
branches), the atomic refcount, and Tier 3 (dense propLengths).

### Re-benchmark after rounds 3–4 (2026-06-09)

Same data (now at `data-weaviate-0/`), fresh `main` worktree (`a746884eb6`, the
established comparison base), interleaved **min-of-5 × 2s** per config (machine
under background load, so minimums; the direct main↔branch dump/compare was re-run
first — bit-identical incl. the 2%-tombstone config where main checks at advance
and the branch defers to scoring):

| workload | main | branch | Δ |
|----------|------|--------|---|
| OR typical (size 5, limit 10) | 254 µs | 113 µs | **−55%** (2.2×) |
| OR worst-case 10-term, limit 10 | 25.4 ms | 15.9 ms | **−37%** (1.6×) |
| OR worst-case 10-term, limit 100 | 29.8 ms | 16.6 ms | **−44%** (1.8×) |
| OR worst-case + filter 10% | 12.5 ms | 10.2 ms | **−18%** |
| OR worst-case + tombstones 2% | 30.3 ms | 16.9 ms | **−44%** (1.8×) |
| AND typical (size 5, limit 10) | 33.1 µs | 23.2 µs | **−30%** (1.4×) |

The tombstone row shows the deferred-tombstone change at work: with 2% deletes the
branch stays at its no-tombstone speed (16.9 vs 15.9 ms) while main pays the
per-advanced-doc probe (30.3 vs 25.4 ms).

### Higher limit (top-100) — gains grow with limit

Same setup, `BMW_LIMIT=100` (lower WAND threshold → more docs scored/enqueued):

| metric | main | branch | Δ |
|--------|------|--------|---|
| serial ns/op | 954,131 | 482,504 | **−49.4%** |
| serial B/op | 325,000 | 92,985 | −71.4% |
| serial allocs/op | 4,334 | 863 | **−80.1%** |
| parallel q/s @14 cores | 9,679 | 22,771 | **2.35×** |

The branch's edge is *bigger* at limit 100 than at limit 10 (−49% vs −32%
serial). Reason: `main`'s allocs/op scale with limit (2,170 → 4,334) because the
old per-match `sort.Sort` boxed the slice into a `sort.Interface` on every
accepted doc; more accepted docs ⇒ more allocations/GC. The branch's `sortByID`
allocates nothing, so allocs/op stay flat at 863 regardless of limit. Results
remain bit-identical to main at limit 100.

### Parallel (concurrent queries)

`BenchmarkBMWCompareParallel` (`b.RunParallel`) at increasing `-cpu`, 10-term OR,
no filter, sequential main/branch runs (no cross-contention), avg of 2:

| cores | main ns/op | branch ns/op | branch latency Δ | main q/s | branch q/s |
|-------|-----------|-------------|------------------|----------|------------|
| 1  | 522,922 | 335,361 | −35.9% | 1,912 | 2,982 |
| 4  | 132,013 | 81,658  | −38.1% | 7,575 | 12,246 |
| 8  | 70,223  | 42,253  | −39.8% | 14,240 | 23,667 |
| 14 | 57,869  | 30,747  | **−46.9%** | 17,280 | **32,523** |

The branch's lead widens with parallelism (1.56× single-thread → 1.88× at 14
cores) and it scales better — 1→14-core speedup 9.0× (main) vs 10.9× (branch),
i.e. 65% → 78% scaling efficiency. That extra headroom is the atomic refcount
(no per-query segment-refcount mutex) plus ~70% fewer bytes / ~60% fewer allocs
(less GC/runtime-lock contention). Peak throughput at 14 cores: **1.88×** main.

### 100 concurrent clients (oversubscribed)

`TestBMWConcurrentLoad`, 100 worker goroutines on GOMAXPROCS=14 (the realistic
"100 simultaneous queries" regime), 8s, no filter:

| | main q/s | branch q/s | main p50 | branch p50 | main p95 | branch p95 | main p99 | branch p99 |
|--|---------|-----------|---------|-----------|---------|-----------|---------|-----------|
| limit 10  | 15,876 | **30,342** (1.91×) | 433µs | 161µs | 5.5ms | 2.2ms | 55ms | 39ms |
| limit 100 | 9,407  | **19,761** (2.10×) | 666µs | 261µs | 27ms | 3.6ms | 206ms | 75ms |

~2× throughput and 30–87% lower median/p95/p99 latency — the allocation cut
sharply reduces GC pressure, which dominates under oversubscription. The extreme
tail (p99.9 ~0.8–1.1s, max ~2–3.7s) stays high on *both* — Go stop-the-world GC
pauses + scheduler latency at 100:14 oversubscription, which these changes don't
target (would need GOGC/GOMEMLIMIT tuning or bounding in-flight queries).

## Worst-case queries

`TestBMWSlowQueries` searches for consistently-slow queries by drawing terms from
the highest-document-frequency pool (where WAND prunes least) and timing each
over repeats. On `climate_fever`:

- **Mixed queries** (terms across freq tiers): median ~2ms, slowest ~9ms.
- **All-high-frequency** 15-term queries (pool = top ~40 terms, 8k–119k docs
  each, `BMW_SLOW_POOL=40`): **consistently 12–28ms (median ~18ms)** — ~10× a
  typical query. Each scores 20k–29k docs and decodes ~5k blocks; the block-max
  upper bounds never drop below the heap threshold so almost nothing is skipped.

Recurring high-frequency drivers: `since` (278k), `census` (168k), `women`
(119k), plus `films/present/complete/level/lord/21/turkey/fund/1912/1974`. These
are the queries to watch for tail latency; cost grows with query length and with
the frequency of the included terms (and with limit). The slow set is emitted as
a `BMW_TERMS=…` string for replay.

## How this was measured

Profiles come from the harness in
[`adapters/repos/db/lsmkv/bm25_blockmax_profile_test.go`](../adapters/repos/db/lsmkv/bm25_blockmax_profile_test.go),
which drives the real query path (`CreateDiskTerm` → `DoBlockMaxWand`) against a
folder of on-disk segments, samples representative terms, and prints a ranked
hotspot table.

Reference run: `climate_fever` `property_text_searchable`, 5.4M docs, 10-term OR
queries, no filter, tombstones present, 20s CPU profile.

```
BMW_SEGMENTS_DIR=/…/property_text_searchable BMW_QUERY_SIZE=10 \
BMW_NUM_QUERIES=1000 BMW_SEED=42 BMW_PROFILE_SECONDS=20 \
  go test -tags integrationTest -run TestProfileBlockMaxWand -v -timeout 30m ./adapters/repos/db/lsmkv/
```

Top self-time: `DoBlockMaxWand` 21%, `AdvanceAtLeast` 7.5%, `decodeReusable`
4.9%, `advanceOnTombstoneOrFilter` 3.3% (+ `sroar.*` ~6%), `sort.insertionSort`
5.1%, `runtime.mapaccess1_fast64` 2.9%, allocation/zeroing (`memclr`+`mallocgc`)
~16%. `CreateDiskTerm` (per-query term setup) is ~17% cumulative.

A separate multi-agent review verified 9 of 32 candidates as real; the rest were
dropped as not-real, off-hot-path, or unsafe-as-proposed.

## Priority table

| # | Change | Measured signal | Impact | Effort | Risk | Status |
|---|--------|-----------------|--------|--------|------|--------|
| 1 | Drop dead per-term decoder allocations (devirtualize decode) | alloc churn; `CreateDiskTerm` 17% cum + GC ~16% | High | Low | None | Tier 1 |
| 2 | `[]*BlockEntry` → `[]BlockEntry` value slice | alloc/zeroing ~16% | Med-High | Low | Low | Tier 1 |
| 3 | Devirtualize block decoders (folded into #1) | `decodeReusable` dispatch | Med | Low | Low | Tier 1 |
| 4 | Gate per-doc `Metrics` bookkeeping | `Score` per-doc writes | Low | Low | None | Tier 1 |
| 5 | Cap `combineResults` prealloc when `limit==0` | memory cliff (off hot-path) | Med (mem) | Low | None | Tier 1 |
| 6 | Replace in-loop `sort.Sort` with bounded re-insertion | `sort.insertionSort` 5.1% | Med | Med | Med | Tier 2 |
| 7 | `propLengths` map → dense/sorted slice | `mapaccess1_fast64` 2.9% | Med | High | Med | Tier 3 |
| 8 | Cheaper tombstone/filter probing | `advanceOnTombstoneOrFilter`+`sroar.*` ~10% | Med | High | Med | Stretch |

## Tier 1 — status: DONE

Implemented and measured. A/B on `climate_fever` `property_text_searchable`,
10-term OR, `BMW_SEED=42`, `BenchmarkBlockMaxWand/size=10 -benchmem -benchtime=5s`:

| Metric | Before | After | Δ |
|--------|--------|-------|---|
| ns/op | 524,910 | 468,783 | −10.7% |
| B/op | 251,441 | 153,780 | −38.8% |
| allocs/op | 2,199 | 1,828 | −16.9% |

The allocation profile confirms `VarInt*.Init` and per-block `DecodeBlockEntry`
allocations are gone. The profile then showed `SegmentBlockMax.reset` dominating
allocations (~59%) via its per-term scratch buffers.

### Follow-up (DONE): skip dead `blockDataBuffer` on the mmap path

`mmapContents` defaults to true, so `readFromMemory` is true on the common path,
where `loadBlockDataReusable` decodes straight from `s.contents` and never reads
`blockDataBuffer`. The 4KB-per-term buffer was pure dead weight; `reset` now only
allocates it when `!readFromMemory`. Cumulative A/B vs the pre-Tier-1 baseline:

| Metric | Before (HEAD) | After Tier 1 + mmap-skip | Δ |
|--------|---------------|--------------------------|---|
| ns/op | 524,910 | 464,262 | −11.6% |
| B/op | 251,441 | 90,865 | −63.9% |
| allocs/op | 2,199 | 1,778 | −19.1% |

Remaining `reset` allocations are the two `BLOCK_SIZE` `uint64` decode slices
(`DocIds`/`Tfs`, ~2KB/term) + two small structs — a `sync.Pool` candidate, but it
needs lifecycle wiring (return buffers via the `CreateDiskTerm` release callback)
and carries use-after-release risk, so it is deferred behind Tier 2.

## Tier 1 (low/no risk — implemented together)

### 1 + 3. Devirtualize decoders, kill per-term decoder allocations
`NewSegmentBlockMax` built a fresh `[]VarEncEncoder[uint64]` and called
`Init(BLOCK_SIZE)` on each per *(term × segment)* per query. `Init` allocates
`values []uint64` and `buf []byte` (~1KB each, ×2) that the query path never
reads — `DecodeReusable` writes into the caller's buffers. The interface call
also blocked inlining.

Fix: a stateless `varenc.GetDecodeFunc(codec) func([]byte, []uint64)`; store two
`decodeDocIds`/`decodeTfs` func fields on `SegmentBlockMax` assigned once. No
per-term encoder instance, no `Init`, no interface dispatch. Write/compaction
path (separate encoders, which do need `Init`) is untouched.

Correctness: none — the decode funcs delegate to the same `decodeReusable`.

### 2. `[]*terms.BlockEntry` → `[]terms.BlockEntry`
`loadBlockEntries` allocated one heap `*BlockEntry` per 128-doc block
(~7,800 for a 1M-doc term), scattered and pointer-chased in the WAND skip loop.

Fix: one contiguous `make([]terms.BlockEntry, blockCount)` + in-place
`terms.DecodeBlockEntryInto`. Indexed field reads are unchanged. Serialization
helpers keep their own pointer slices (write path).

Correctness: low — no pointer identity / nil-entry sentinels relied upon.

### 4. Gate per-doc `Metrics` bookkeeping
`Score` incremented `BlockMetrics` counters per scored doc; no production code
reads them. Guard behind an unexported `collectBlockMetrics` bool (default
false; the profiling harness sets it true for its per-query stats).

Correctness: none.

### 5. Cap `combineResults` prealloc for unlimited queries
With `limit==0`, `wandBlock` sets `limit` to the sum of all term counts
(millions); `combineResults` then preallocated four slices at
`limit*len(allIds)` → multi-GB spike. Bound capacity by the actual row count.

Correctness: none — capacity hints don't change results.

## Tier 2 — WAND loop (status: DONE)

CPU-focused loop/branch pass over `DoBlockMaxWand` + `SegmentBlockMax`:

1. **`sort.Sort(results)` → `Terms.sortByID()` (concrete insertion sort).**
   `sort.Sort` dispatched `Less`/`Swap` through `sort.Interface` *and* boxed the
   `Terms` slice into an interface value — a heap allocation on **every matched
   doc**. The concrete insertion sort (term list is small + nearly-sorted)
   inlines the `idPointer` compares and allocates nothing.
2. **Dead store removed:** `upperBound -= …` in the match branch (the value was
   never read before `upperBound` is reset the next iteration).
3. **`AdvanceAtLeast`:** the "last block, still past it" disjunct is provably
   always-false after the skip loop → reduced to a single `>= len` bounds check.
4. **`AdvanceAtLeastShallow`:** the post-loop exhaustion `if` is dead (the loop
   returns internally on overrun) → removed.

A/B isolating #1 (climate_fever, 10-term OR, same seed): **421,628 → 381,936
ns/op (−9.4%), 1,776 → 862 allocs/op (−51%)** — the ~914 fewer allocs/op are the
per-match interface boxing. Combined with #2–#4, the turn moved ~464µs→382µs/op
(~18%). CPU profile confirms `sort.insertionSort`/`Terms.Less`/`Terms.Swap` are
gone (replaced by `Terms.sortByID` at 3.71%). Correctness: BM25F block + inverted
suites pass under `-race`.

Follow-up safe loop/branch wins:

5. **`advanceOnTombstoneOrFilter`:** read the doc id once per skipped doc instead
   of re-indexing the slice in each of the up-to-three membership checks — helps
   the filtered/tombstoned path (where this method is ~40% cumulative).
6. **`DoBlockMaxWand` prune branch:** fused the two `[0, pivotPoint]` scans
   (heaviest-term + smallest block-max-id) into one pass.

## Validation: main-vs-branch baseline (scores + rankings)

`bm25_blockmax_baseline_test.go` (`TestBMWBaseline`, dump/compare modes) records
the ranked top-K (docID, score) for a deterministic query set and diffs a
baseline against the current code. It is self-contained and uses only APIs
present on main, so the same file runs in a `main` git worktree to produce the
baseline. Determinism across processes comes from sorting sampled terms before
any rng use (a Go map's iteration order is not stable).

Ran on `climate_fever` (5.4M docs), comparing this whole branch (Tier 1 + atomic
refcount + Tier 2 + #5/#6) against `main`:

| set | queries | identical | ranking Δ | doc-set Δ | max \|Δscore\| |
|-----|---------|-----------|-----------|-----------|----------------|
| OR, size 5, no filter | 3000 | 3000 | 0 | 0 | 0 |
| OR, size 10, filter 0.1 | 2000 | 2000 | 0 | 0 | 0 |
| AND, size 4 | 2000 | 2000 | 0 | 0 | 0 |

Bit-for-bit identical — the optimizations are pure performance, no result change.

## DoBlockMaxWand deep-dive (status: DONE)

Line-profiled the worst case (10 all-high-frequency terms — block-max pruning
fails, ~900k docs decoded/query) and applied verified, bit-identical changes:

1. **Inline `Idf()` → `.idf`** in the pivot/prune scans (it wasn't inlining; ~3% self).
2. **`iterations%100000` → countdown** (removes a per-iteration 64-bit modulo, ~1.5%).
3. **Prune-branch re-sort → `Terms.reinsertRight`** (search_segment.go): the old
   loop had **no early break** and rescanned the whole tail every iteration (its
   `exhausted`-swap branch, 8.7s self, only shuffled MaxUint64 sentinels). After
   `AdvanceAtLeast` only one element moved, and only rightward, so a break-on-settle
   re-insertion suffices. Verified bit-identical by exhaustive enumeration (n≤6) +
   20M random trials.
4. **Tombstone empty→nil normalization** in `NewSegmentBlockMax`: `memTombstones`
   is always a non-nil `sroar.NewBitmap()`, so with no in-memory deletes it's a
   non-nil **empty** bitmap that paid a `Contains` per advanced doc (~8.4s). Nil-ing
   genuinely-empty bitmaps lets the existing guard skip them — eliminated ~13% of
   self time (all `sroar.*` probes) on this dataset.
5. **upperBound loop**: dropped the per-element `exhausted` guard (exhausted terms
   contribute `currentBlockMaxId==MaxUint64` → no shallow advance, and
   `currentBlockImpact==0` → no-op add).
6. **Struct field reorder**: hot scalars (`idPointer/idf/currentBlockMaxId/
   currentBlockImpact/exhausted/decoded/freqDecoded`) co-located at the top, gated-off
   `Metrics` pushed to the tail.

Results, climate_fever, vs the pre-deep-dive branch:
- **Worst-case** (10 high-freq, conc=14): **446 → 628 q/s (+41%)**.
- **Normal** (10-term mixed, no filter): **~327 → ~273 µs/op (−16.5%)**.
- Bit-identical to main across 8000 queries (OR/filtered/AND/limit=100); BM25F +
  inverted + refcount suites pass under `-race`.

(Dropped as speculative/unsafe by the verifier: full struct-of-arrays, galloping
block search, the 2-bitmap merge — deferred, redundant with #4 here.)

## DoBlockMaxWand deep-dive, round 2 (status: DONE)

Re-profiled the worst case after round 1 (`AdvanceAtLeast` 15%, `reinsertRight`
12%, pivot scan still dominant) and ran a second agent deep-dive. Kept:

1. **`ShouldEnqueue` reads `items[0].Dist` directly** instead of via `Top()`,
   which returned the whole `Item` by value (a multi-word copy) on a path hit per
   scored doc + per iteration (priorityqueue/queue.go).
2. **Unify the matched-*else* swap-bubble into `reinsertRight`** (move-based, same
   permutation as the old break-on-settle swap bubble).

Result vs the post-round-1 branch: normal 10-term **273 → ~255 µs/op (−6.5%)**,
worst case **617 → 665 q/s (+7.8%)**, bit-identical to main (8000 queries),
`-race` clean.

**Reverted (negative result, documented):** binary-searching the `AdvanceAtLeast`
in-block doc scan and block-entry skip loop. Both were verified bit-identical and
the line profile made them look like obvious wins (~8.7s + ~1.8s linear scans),
but they **regressed the worst case 617 → 474 q/s (−23%)**. Reason: in dense
all-high-frequency scanning the advances are mostly *small* (1–2 docs), so the
linear scan touches a couple of *sequential* cache-warm elements, whereas binary
search does log(N) *random* accesses — disastrous for the block-entry array
(~156KB, cache misses) and net-negative even within a 128-doc block. The profile
line was hot from *call-count × tiny-work*, not work-per-call; binary search adds
per-call overhead exactly where the work per call is already minimal. Lesson:
"linear scan is hot → binary search" is wrong when the hotness is call frequency,
not scan length — measure, don't assume.

**Attempted + reverted (negative result): dense filter bitset.** The per-advanced-doc
`filterDocIds.Contains` (a sroar keys-b-tree search) is the residual filtered-path
cost. Tried converting the filter `AllowList` to a dense `[]uint64` (O(1) membership,
built once per query). Measured (filter 0.1, limit 100): worst-case filtered serial
**12.0 → 8.6 ms (−29%)** ✓, but a typical filtered query **132 µs → 3,034 µs (23×
slower)** ✗ — the O(cardinality) build (~2.9 ms to materialize a 540k-bit set) dwarfs
a light query's actual work. Same trap as the binary searches: an unconditional build
only pays off when #probes ≫ cardinality. A non-regressing version needs lazy/adaptive
building (build only after a query proves heavy), which is concurrency-delicate (a
query's per-segment terms run in parallel goroutines) — a larger, separate change, or
better a monotonic-cursor `Contains` added to sroar itself (its container internals are
unexported, so it can't be done from lsmkv). Reverted; the filter probe stays as-is.

Deferred (verifier-flagged marginal / cache-miss-bound): pivot-scan `liveCount`
(the L90 `exhausted` load shares a cache line with the `idf` read, so removing it
saves a branch, not the miss), and `reinsertRight` binary-search+memmove (same
small-displacement trap as the reverted searches).

## Loop & conditional opt, round 3 (status: DONE)

Ran a 52-agent workflow (6 finders by lens — WAND main loop, AND loop, advance
methods, score/decode, compiler BCE, sort/ordering — each finding refuted by a
3-lens adversarial panel: bit-identical-vs-main, perf-reality, edge-cases). 15
findings, 13 rejected. **Kept (all bit-identical, validated vs HEAD≡main over 2000
queries each: AND/OR, limit 10/100, tombstones 0/2%, ±filter — 0 doc-set changes,
0 score drift; integration `-race` incl. real deletes):**

1. **Drop `DoBlockMaxAnd`'s per-iteration O(n) exhausted scan.** AND is terminal
   the instant any term exhausts (an exhausted term sits at `MaxUint64` forever, so
   `isCandidate` can never be true again and the heap is frozen) — so return at the
   advance sites (shallow-advance loop + candidate loop; `results[0]` was already
   guarded) instead of rescanning all terms each iteration. **AND +2.2–2.5%.**
2. **BCE reslice + redundant-load removal in the two `DoBlockMaxWand`
   `[0,pivotPoint]` loops** (`candidates := results[:pivotPoint+1]`; the prune loop
   also stops re-loading `results[i]` twice). **OR worst-case ~2%** (4/5 interleaved
   rounds faster, min −2.5%).
3. **Drop the provably-always-true `blockEntryIdx < len` conjunct** from
   `AdvanceAtLeastShallow`'s block-scan loop (the in-loop bounds return guarantees
   it) — dead-code removal.
4. **Cache `results[firstNonExhausted]`** in the WAND matched-branch tombstone gate.

**Reverted (negative result):** scan-then-`copy()`(memmove) `reinsertRight` — split
the interleaved compare+move into a scan plus a bulk shift. **Regressed worst-case
17.0 → 18.2 ms (+7%).** The interleaved compare+move loop is already optimal; the
moves were never the cost, and the extra `copy()` call + branch hurt. Same
measure-first lesson as the binary-search/dense-bitset reverts.

**Rejected by verifiers (don't re-explore):** cache `results[pivotPoint]` in the
pivot loop (compiler no-op — already loaded once); hoist tombstone/filter checks
into cached struct bools (premise false — `deferTombstoneToScore` is L1-resident;
plus a placement bug under `BMW_DEFER_TOMBSTONE=false`); precompute `1-b` /
`k1*(1-b)` into a struct field (off the critical path — fuses as the FMA additive
operand under the divide; decode-frequency only); fold the else-branch block-max-min
into the fused loop (pessimizes candidate iters). All instruction-level; no
end-to-end gain.

**Deeper profile finding (open).** On the OR worst case, `Terms.reinsertRight` is
**21% self** (the re-sort after each advance) and `DoBlockMaxWand` itself 34%, while
`sortByID` is only **1.86%** — so the verifier-"measure-first" rank (matched-branch
prefix-reinsert replacing `sortByID`) was **skipped**: the target is too small to
justify its subtlety. `reinsertRight` is the real lever but is already near-optimal
(the 30 `SegmentBlockMax` structs are L1-resident, so an SoA parallel-id array would
not cut the deref cost, and the memmove rewrite regressed); its cost is inherent
re-sort work × call frequency. Reducing it needs an algorithmic change to advance
fewer times, not a cheaper per-call sort.

## Loop & conditional opt, round 4 (status: DONE)

Follow-up review pass after round 3. **Kept (bit-identical vs HEAD≡main over the
full 7-config matrix — AND/OR, limit 10/100, tombstones 0/2%, 10% filter,
worst-case query: 0 doc-set changes, 0 score drift; `-race` + linters clean):**

1. **`DoBlockMaxAnd` ctx check: modulo → countdown** (same change the WAND loop
   already had; the 64-bit `iterations%100000` ran every iteration).
2. **`AdvanceAtLeast` scan hoisting**: both scan loops now run on locals (index in
   a register, slice headers loaded once) and the `decoded=false/freqDecoded=false`
   stores happen once after the block-skip loop instead of on every skipped block.
   The in-block scan's `docIDs`/`blockDataSize` are read *after* `decodeBlock`
   (it rewrites the buffer in place and updates the size).
3. **`docInfos` allocation moved into the scoring path** — it was allocated for
   every `ShouldEnqueue`-passing iteration, including non-aligned ones that never
   use it (explain-mode-only effect).

Measured (min-of-N interleaved, machine under background load so minimums used):
OR worst-case **23.09 → 22.09 ms (−4.3%)**; AND size-8 **−4.5%**, AND size-5
**−11.5%** (countdown + the AND loop's heavy `AdvanceAtLeast` use both helping).

**Reverted (negative result):** `reinsertRight` body restructure caching the
`t[j+1]` load (`nxt := t[j+1]; if nxt.idPointer >= id break`) — min-of-6 showed it
*slower* than the interleaved compare+move original (22.46 vs 22.09 ms). Third
confirmation that `reinsertRight`'s loop is already optimal per-call; do not
revisit its body.

**Dropped during review (false premise — documented so it isn't re-derived):**
the "exhausted terms always sit at the tail" invariant is FALSE.
`AdvanceAtLeastShallow` in the upperBound loop can exhaust a term *in place*, and
the prune branches' `reinsertRight(nextList)` only re-sorts the advanced element —
so exhausted sentinels can sit mid-array until the next matched-branch `sortByID`.
Two candidate optimizations died on this: simplifying the matched-branch advance
loop's `!term.exhausted &&` condition, and fusing the score+advance loops (their
break conditions differ precisely in these mid-array-exhausted states; main's
behaviour there must be preserved verbatim).

## Round 5: full profiling + benchmark sweep (2026-06-10)

Six 20s CPU/alloc profiles (`/tmp/bmwprof_r5/`: OR_WC, OR_typical, OR_filtered,
OR_tomb2, AND_size5, OR_WC_conc14 incl. mutex/block), analyzed line-level by a
7-agent workflow, plus a fresh main-vs-branch benchmark sweep (min-of-N
interleaved, main worktree `a746884eb6`):

| benchmark | main | branch | Δ |
|---|---|---|---|
| serial OR typical (size 5, limit 10) | 280.7 µs | 119.6 µs | **−57%** |
| serial OR worst-case 10-term | 25.45 ms | 15.23 ms | **−40%** |
| serial AND typical (size 5) | 35.2 µs | 24.4 µs | **−31%** |
| parallel RunParallel (GOMAXPROCS=14) | 33.4 µs/op | 13.3 µs/op | **2.5×** |
| 100-worker load (8s) | 27,631 q/s | 65,802 q/s | **2.38×** |
| — p50 / p99 / p99.9 latency | 294µs / 26.7ms / 698ms | 107µs / 11.9ms / 344ms | ~2× better |

**Profile regimes.** The code has split into three cost regimes:

1. **Worst-case OR** (WC/tomb2/conc14): the WAND core is 58–64% flat
   (DoBlockMaxWand 33–36%, reinsertRight 21–26%, AdvanceAtLeast 12–14%,
   decodeReusable 8–9%) and is now **memory-bound on dependent pointer derefs**
   through `[]*SegmentBlockMax` + GC write barriers (disasm-confirmed). Every
   in-layout restructure tried in rounds 1–4 regressed → instruction-level
   headroom is spent; remaining levers are a data-layout change (SoA hot-field
   mirror) or fewer iterations (algorithmic).
2. **Setup-bound** (AND_size5 is ~75% setup, OR_typical ~26%): term construction
   does **three DiskTree descents per (term, segment)** for the same key
   (hasKey, getDocCount, NewSegmentBlockMax), reallocates `NodeKeyBuffer` per
   visited node (a cap-vs-len growth check at `disk_tree.go:68` — **62% of ALL
   allocated objects**), and builds memtable terms even when memtables are empty.
   AND allocates 30GB/20s; all of it setup, zero in the scoring loop.
3. **Filtered**: sroar `Contains` is 34.7% of CPU (~1.8B probes); 86% of each
   probe is a redundant key→container binary search although probes are monotonic
   ascending with a 99.97% same-container hit rate → a container-cursor cache in
   the sroar fork would amortize it (distinct from the rejected dense bitset:
   zero build cost).

**Concurrency: closed.** 0.11% mutex delay at conc 14, 100% of it runtime
allocator locks reached from setup allocations; zero application mutexes (the
atomic refcount holds under load); 10.1× parallelism on 10 P-cores.

**Deferred-tombstone validation in the profile:** OR_tomb2's
advanceOnTombstoneOrFilter is at OR_WC levels (3.8% vs 3.2% — no per-advance
probes); the residual tomb2 gap (61 vs 66 q/s) is the per-term
`ReadOnlyTombstones Clone+Or` in createDiskTermFromCV, hoistable to per-query.

**Ranked next targets** (1–4 are measured-confident; 7 is the only remaining
WAND-core lever and is prototype-gated):
1. Dedupe DiskTree descents: 1 `Get` per (term,segment) instead of 3 (est. −25–30%
   AND, −8% typical).
2. Zero-copy DiskTree.Get descent + fix the NodeKeyBuffer cap-vs-len growth check
   (est. −10–15% AND; removes 62% of allocated objects).
3. Cache decoded `[]terms.BlockEntry` per (segment, node) + pool the reset
   buffers (attacks 60–80% of alloc bytes everywhere; est. −8–12% OR_WC).
4. Hoist the memtable-tombstone Clone+Or out of the per-term loop (−3–5% tomb2,
   >90% of its alloc volume).
5. Container-cursor `Contains` in the sroar fork (est. −25–30% filtered).
6. Setup trivia: skip empty-memtable terms; `NewMinWithId` off-by-one regrow
   (queue.go:159).
7. SoA hot-field mirror for Terms (ids as contiguous `[]uint64`) — the only
   remaining lever on the memory-bound WAND core; high revert-risk, prototype.
8. advanceOnTombstoneOrFilter fast-path inlining via outlined slow loop (~1%).
9. Per-bitwidth decode kernels / fused decode-until-target (defer; complexity).

## Round 6: setup-path optimization (status: DONE)

Implements round-5 targets 1, 2, 4, 6 — the query-SETUP path (term construction),
which round-5 profiling showed dominates AND (~75%) and weighs on typical OR
(~26%). The WAND scoring loop is untouched. Deferred: target 3 (BlockEntry cache —
compaction lifecycle needs its own round), 5 (sroar fork), 7 (SoA prototype).

1. **One index descent per (term, segment) instead of three**
   (`getInvertedNodeAndDocCount`: bloom + `index.Get` + docCount read fused;
   `newSegmentBlockMax` accepts the prefetched node, nil = old path). Inverted
   segments where the key is absent skip construction outright; non-inverted
   segments keep the constructor-does-its-own-Get path verbatim (mixed-strategy
   buckets unaffected).
2. **Zero-copy `DiskTree.Get`**: node keys compared in place against the tree
   data; only the matched node's key is materialized (`bytes.Clone`). Kills both
   the per-Get buffer alloc and the per-node regrow (the old growth check
   compared len, not cap — 62% of ALL allocated objects on setup-bound loads).
   Serves all strategies; full lsmkv integration suite passed under `-race`.
3. **Memtable tombstones hoisted out of the per-term loop** — one
   `ReadOnlyTombstones` Clone+Or per query instead of per term (the bitmaps are
   invariant within a consistent view; all flushing terms share one read-only
   clone).
4. **Memtable terms built only when `getMap` finds the key** (was:
   always-construct + fillTerm; empty-memtable construction was 11%+ of typical's
   alloc bytes).
5. **topK heaps preallocate `limit+1`** (`InsertAndPop` holds limit+1 items
   between insert and pop; exact-limit capacity forced one regrow per query).

Measured (min-of-6 interleaved vs pre-change HEAD `1b5c9990eb`):

| workload | before | after | Δ ns/op | Δ B/op |
|---|---|---|---|---|
| AND typical (size 5) | 23.2 µs / 37.5 KB | 12.6 µs / 28.9 KB | **−46%** | −23% |
| OR typical (size 5) | 114.4 µs / 39.5 KB | 103.7 µs / 28.9 KB | **−9.4%** | −27% |
| OR worst-case + tombstones 2% | 16.87 ms / 6.12 MB | 16.37 ms / 0.96 MB | −2.9% | **−84%** |
| OR worst-case | 15.31 ms | ±noise | neutral | −8% |

(OR worst-case deltas are within the ±11% same-binary machine noise; setup is
~0.7% of that query, so neutrality is expected.)

**30-common-term stress, limit 1000** (top-30 most frequent sampled terms via the
new `TestBMWTopTerms` helper — incl. "that" at 938k docs ≈ 17% of the corpus, so
WAND pruning barely engages and this approximates a full-scoring scan;
min-of-5 interleaved):

| | time | B/op | allocs/op |
|---|---|---|---|
| main `a746884eb6` | 131.4 ms | 5.45 MB | 165,923 |
| branch (round 6) | 53.9 ms | 0.87 MB | **906** |
| Δ | **−59% (2.4×)** | **−84%** | **−99.5%** |

main's 166k allocs/op at limit 1000 is the per-accepted-doc `sort.Sort` boxing
plus per-query setup churn; the branch holds a flat ~900 allocs regardless of
limit. vs the pre-round-6 branch, time is neutral (setup ≈0.3% of a 50ms query)
and B/op drops a further −23% (1.13 → 0.87 MB). Bit-identical vs main confirmed
at this config (0 doc-set changes, 0 score drift).

QPS under concurrent load (`TestBMWConcurrentLoad`, same 30-term limit-1000
query, 10s):

| workers | side | q/s | p50 | p99 |
|---|---|---|---|---|
| 14 (cores) | main | 77 | 177 ms | 255 ms |
| 14 | branch | **190 (2.5×)** | 70 ms | 119 ms |
| 100 | main | 78 | 1.22 s | 1.98 s |
| 100 | branch | **201 (2.6×)** | 445 ms | 853 ms |

Throughput saturates at core count on both sides (CPU-bound; 100 workers only
multiply queue latency), so the 2.5–2.6× is pure per-query CPU reduction.

### Second dataset: ltk_desc (3GB single segment, multilingual product text)

Same 30-common-term limit-1000 shape on a structurally different corpus (one
3GB level-15 segment vs climate_fever's three; head term "eignet" 220k docs with
a fast Zipf drop-off vs climate_fever's 938k/17%-of-corpus head). Bit-identical
vs main (0 doc-set changes, 0 score drift). min-of-5 interleaved:

| | main | branch | Δ |
|---|---|---|---|
| serial time | 30.0 ms | 15.3 ms | **−49% (2.0×)** |
| serial B/op | 1.84 MB | 0.23 MB | **−88%** |
| serial allocs/op | 56,465 | 354 | **−99.4%** |
| QPS @14 workers | 181 q/s | 315 q/s | **1.74×** |
| p50 / p99 | 47.8 ms / 498 ms | 23.3 ms / 171 ms | p99 **−66%** |

main's concurrent p99 degrades far past its serial time (~10M allocs/s of GC
pressure at 181 q/s); the branch's flat ~350 allocs keeps the tail tight.

**Hotspot cross-check (20s profile, same shape):** identical hotspot *set* to
climate_fever — the five WAND-core functions, no new entries, no I/O/memmove,
setup negligible — but the ranking shifts with corpus shape:

| function | ltk_desc | climate_fever T30 |
|---|---|---|
| Terms.sortByID | **20.4% (#1)** | 13.5% (#3) |
| DoBlockMaxWand | 16.9% | 31.4% |
| Terms.reinsertRight | 9.8% | 18.3% |
| AdvanceAtLeast | 6.4% | 9.4% |
| decodeReusable | 4.1% | 6.9% |
| priorityqueue (insert/heapify/swap/Less) | ~3.9% | ~1.5% |

On ltk_desc the matched branch dominates (steeper Zipf → terms align constantly;
58.8k docs scored/query), so the per-match full `sortByID` of 30 terms becomes
the single largest cost. Two corpora now independently rank the matched-branch
re-sort as the top remaining target — prioritize the prefix-reinsert re-measure
and the SoA id-mirror (both make the re-sort cheaper or unnecessary).

### I/O & memory-copy attribution (30-term limit-1000 shape)

Is I/O or copying a bottleneck? **No, neither — measured:**

- **I/O: zero on the default path.** Segments this size are mmap'd
  (`readFromMemory`), and the whole disk→decoder chain is zero-copy:
  `loadBlockDataReusable` decodes straight from `s.contents`,
  `DecodeBlockDataReusable` just reslices. The 20s CPU profile contains **no
  read-syscall and no `runtime.memmove` frames at all**. Steady-state the 897MB
  segment is page-cache resident.
- **Forcing the syscall path (`BMW_PREAD=true`) costs only ~5%** (19 → 18 q/s)
  despite ~20k preads + 4KB kernel→user copies per query (~80MB/query,
  ~1.4GB/s of forced copying) — even deliberate I/O+copy barely moves this
  workload. (The pread profile shows 96% in `syscall.pread`; that is Darwin
  SIGPROF attribution skew at ~366k syscalls/s — trust the wall-clock delta.)
- **Intrinsic data movement is small**: varint decode (the actual decompression,
  2.59M docIDs/query) is 6.9%; GC traffic (madvise+memclr) ~3.5% after round 6.
  Decode-output bandwidth ≈ 0.4GB/s — nowhere near memory bandwidth.

The bottleneck is the WAND core's compare/sort logic, **memory-latency-bound**
(dependent pointer derefs), not bandwidth: DoBlockMaxWand 31.4% + reinsertRight
18.3% + sortByID 13.5% + AdvanceAtLeast 9.4% ≈ 79%.

**New shape-specific finding:** `sortByID` is 13.5% here vs 1.9% on the 10-term
worst case — 30 common terms co-occur in nearly every doc, so the matched branch
fires per scored doc and full-sorts 30 elements each time. The round-3
"matched-branch prefix reinsert" idea was skipped when sortByID measured 1.9%;
at 13.5% on this shape it deserves a re-measure, and it strengthens the SoA
hot-field-mirror target (round-5 rank 7).

**Validation:** bit-identical vs pre-change HEAD across 8 configs × 2000 queries
(OR/AND × tombstones 0/2% × limit 10/100, filter 10%, filter+tombstone combined) —
0 doc-set changes, 0 score drift; segmentindex + lsmkv unit suites; **full** lsmkv
integration suite `-race` (covers the new `DiskTree.Get` under every strategy);
inverted suite `-race`; linters clean. A 4-lens adversarial review workflow
(aliasing/ownership, lifecycle/concurrency, strategy/error paths, non-BMW
`DiskTree.Get` callers) confirmed zero critical/major findings.

Known intentional divergences from main (unobservable in valid use): a shared
read-only tombstone clone across flushing terms (was per-term clones; nothing
mutates them); inverted-absent-key terms skip construction instead of failing
inside the constructor; the empty-filter + key-in-memtable case no longer
nil-panics into the recover handler.

## Tombstone density of the test datasets (`TestBMWTombstoneStats`)

| dataset | docs stored | distinct tombstoned docIDs | deleted share |
|---|---|---|---|
| ltk_desc (6 segments, 14GB) | 244.5M | 63.7M | **26.1%** |
| climate_fever | 5.4M | 0 | 0% |

ltk per segment (a segment's bitmap targets *older* segments' docs, merged
forward — hence ratios >100% against its own doc count): l18 63.7M, l16 63.7M,
l15 40.3M, l13 29.9M, l12 26.6M, l5 80,192 — the newest segment's tombstones
exactly equal its own doc count, the signature of a pure-update workload (every
insert deletes a prior version). Implications: (a) real-world tombstone density
here is **13× the 2% synthetic rate** used to validate the deferred-tombstone
change — on main every advanced doc probes a 63.7M-entry sroar bitmap, on the
branch only scored candidates do, and that effect is already baked into the ltk
main-vs-branch numbers; (b) side observation (main behaviour, untouched): this
bucket's `GetAveragePropertyLength` count — used as N in idf — reports 2.18B vs
244.5M actually stored docs, i.e. the cached averagePropCount appears to
accumulate across updates/compactions without subtracting; worth a separate
look since it skews idf.

## Heap usage during querying (post-round-7)

inuse_space after `runtime.GC()`, T30 limit-1000 workload:

| | ltk (14GB, 244M docs) | climate_fever (5.4M docs) |
|---|---|---|
| total live heap | **8.75 GB** | 178 MB |
| propLengths maps (`gobenc.decodeFast`) | **8.04 GB (92%)** | 144.5 MB (81%) |
| dense propLengths arrays | 511 MB | 20.4 MB |
| tombstone bitmaps | 150 MB | — |
| per-query churn | ~500 objs / few MB | ~350 objs / 0.87 MB |

The live heap while querying is almost entirely **bucket-open state**: the
per-segment `map[uint64]uint32` property-lengths maps (~33B/entry of Go map
overhead for 244M entries). Per-query allocation is invisible after round 6.
The round-3 dense arrays only get built when a segment's docID span is ≤4/3 of
its entry count (`buildDensePropertyLengths`); on ltk only ~128M of span
qualified — the update-heavy history (26% deleted, survivors scattered across a
2.18B docID space) makes most segments too sparse — **and even when dense is
built, the map is retained as fallback**, so dense adds memory instead of
replacing it.

Ranked follow-ups (memory, not query-CPU): (1) free the map when the dense
array fully covers its key range (easy; recovers the entire map for dense
segments); (2) a compact sparse representation for the rest — sorted
docID+length pairs (12B/entry ≈ 2.9GB for ltk, 2.8× less than the map) or a
paged two-level array; trade a binary search per first-scored-doc against the
current map probe (propLengthOf was ~3% CPU — measure); (3) note that
compaction-time docID compaction would fix the sparsity at the root.

## Rounds 8–10: property-lengths memory overhaul (status: DONE)

The heap analysis showed 92% of live query-time heap was the per-segment
`map[uint64]uint32` property-lengths maps (~33B/entry; 8.04GB on ltk). Three
iterations, each measured:

**r8 (dense + map-drop):** widened the round-3 dense gate to 12.5% density and
released the map once dense was built (compaction re-decodes from disk on
demand; an audit workflow traced every consumer first — notable finds: the
reusable inverted cursor loaded the map and NEVER used it (dead code, removed —
every Like/range filter paid a full map load), zero-length entries provably
cannot exist, and `getPropertyLengths` must never return an empty map for a
non-empty segment or compaction silently writes corrupt block-max metadata).
Result: ltk 8.75 → 3.08GB, bit-identical, all suites green.

**r9 (pairs-only, dense removed) — NEGATIVE RESULT:** sorted parallel arrays
(`ids []uint64` + `lens []uint32`, 12B/entry exact) for ALL segments, decoded
straight into pre-allocated arrays by `gobenc.DecodePairs` (the map never
exists, even at open) and sorted by an LSD radix sort (insertion sort is O(n²)
here — gob emits random map order). Pairs beat the maps (ltk neutral) but lost
3.2× to dense on dense-shaped data (cf typical 173→527µs): scoring skips
hundreds of pairs entries per lookup, and ~log(gap) cache-missing probes can't
compete with dense's single indexed load. Dense is also *smaller* than pairs
above 33% occupancy.

**r10 (hybrid, shipped):** per segment, dense when span/3 ≤ count (≥33%
occupancy — no sort needed, fill from unsorted pairs), sorted pairs below; the
map never retained; both representations behind one `propLengthsView`.
Pairs lookup evolution (each step measured): cursor+gallop (+30% on rare-term
queries: huge gaps → ~30 probes), pure interpolation (fixed rare terms, hurt
frequent ones), final = linear-4 → bounded gallop (+4/+16/+64) → budgeted
interpolation → binary. The pairs are near-uniformly thinned docID sequences,
so interpolation lands within a few entries regardless of jump size.

| | main/r7 (maps+old dense) | r10 hybrid |
|---|---|---|
| ltk live heap | 8.55 GB | **2.01 GB (4.3×)** |
| ltk bucket open | minutes (map decode) | seconds (pairs decode + radix/dense fill) |
| cf typical / T30 / WC10 | — | parity with r7 (dense again) |
| ltk typical (rare terms) | 3.45 ms | **2.13 ms (−38%)** |
| ltk T30 (frequent terms) | — | ±5% (noise band) |

Validated: bit-identical matrix (9 configs × both datasets) vs r7, full lsmkv +
inverted suites `-race`, linters. `getPropertyLengths` reconstructs a transient
exact map from either representation for compaction-frequency callers.

## Round 7: matched-branch sort elision (status: DONE)

Both datasets' 30-common-term profiles ranked the matched branch's unconditional
`sortByID` as the top remaining cost (ltk 20.4%, cf 13.5%): matched-dense queries
re-sort all n terms per scored doc when only the advanced prefix moved.

**Change** (`DoBlockMaxWand`): a function-local `needFullSort` flag. It is set
when an `AdvanceAtLeastShallow` in the upperBound loop *actually moves*
(`blockEntryIdx` changed — moves and exhausts always increment it, the
early-return no-op never does), because shallow advances mutate idPointers (and
can exhaust terms) in place, leaving inversions/mis-placed sentinels that only a
full sort repairs. While the flag is clear, the matched branch restores order
with right-to-left `reinsertRight` over just the `advanced` prefix instead of
`sortByID`. Matched iterations never *really* shallow-advance (their live prefix
is aligned, so true block-max ≥ pivot), so matched-dense runs stay on the cheap
path; prune-heavy runs fall back to the full sort and lose nothing but a branch.

**Adversarial verification (3-agent refuter panel) found a real flaw in the
first version**: `decodeBlock`'s ENCODE_AS_FULL_BYTES path (docCount==1 terms)
never initializes `currentBlockMaxId` (stays 0), so the shallow guard over-fires
for live singleton terms in matched iterations — a *conservative* failure
(no-op call, but the naive flag-set defeated the fast path for any query with a
rare term). Fixed by the `blockEntryIdx`-change detection above. The panel also
*proved* the permutation-equivalence claim with a 2M-case property test
(right-to-left reinsertRight over a dirty prefix + sorted suffix == stable
insertion sort, incl. equal-id and sentinel cases) and traced every
idPointer/exhaust writer to confirm flag coverage. Two pre-existing main quirks
recorded (not touched): stale `currentBlockMaxId=0` makes prune-branch `next`
computations conservative for singleton terms; memtable terms' MaxImpactTf=0
zeroes their currentBlockImpact via SetIdf.

**Measured (min-of-N interleaved, r6 → r7):**

| workload | r6 | r7 | Δ |
|---|---|---|---|
| ltk 6-segment (14GB) T30, limit 1000 | 549.7 ms | 367.2 ms | **−33%** |
| cf T30, limit 1000 | 54.8 ms | 51.2 ms | **−6.5%** |
| ltk 6-segment typical (size 5) | 1.93 ms | 1.82 ms | −5.6% |
| cf typical (size 5) | 101.5 µs | 98.5 µs | −2.9% |
| cf worst-case 10-term (prune-heavy) | 15.84 ms | 15.53 ms | ~neutral |

**Validation:** bit-identical vs r6 across 10 configs × 2 datasets — including
the full 6-segment ltk corpus opened in place — twice (pre- and post-amendment;
~28k queries total, 0 doc-set changes, 0 score drift); full lsmkv + inverted
suites `-race`; linters clean.

## Tier 3 (status: DONE)

### 7. `propLengths` map → dense slice
Per-scored-doc `s.propLengths[s.idPointer]` was a `runtime.mapaccess1_fast64`
probe. Now each segment also builds, once, a dense `[]uint32` indexed by
`docID - propLengthsMin` (`buildDensePropertyLengths` in segment_inverted.go);
`SegmentBlockMax.propLengthOf` uses a single bounds-checked index and falls back
to the map only for sparse segments. Holes yield 0 in both paths, so results are
unchanged.

The map is **kept** (compaction `maps.Copy`s it wholesale, plus cursor/merge
readers), so the dense array is additive memory. To bound it, it is built only
when the docID range is ≥~75% dense (`span ≤ 4/3·entries`); sparse segments stay
map-only. Cost when built: `span·4` bytes/segment (≈4·entries when dense) —
roughly +25% on the property-length memory, a one-time per-segment allocation
(per-query allocs/op unchanged at ~860).

A/B isolating the Score lookup (climate_fever, 10-term OR, ~1000 docs scored per
query): **~420µs → ~354µs/op (≈ −16%)** (map: 433/407µs; dense: 367/342µs).
Correctness: BM25F + inverted pass under `-race`, and the main-vs-branch baseline
(OR/filtered/AND) stays **bit-identical**.

Note: the win scales with docs-scored-per-query; it is smaller for highly-pruned
queries. If per-segment memory is tight, the density gate can be tightened or the
dense build made opt-in.

### 8. Cheaper tombstone/filter probing
`advanceOnTombstoneOrFilter` → `sroar.Bitmap.Contains` per advanced doc. The
naive "skip nil-checks" fix was rejected (nil-checks are cheap; the bitmap probe
is the cost). A dense bitset representation was also prototyped and **rejected**
— it regressed typical filtered queries ~23× (the O(cardinality) build cost
dwarfs the per-probe saving).

#### 8a. Defer tombstone checks to scoring time (status: DONE)
Key insight: a **filter** prunes the WAND candidate space (a disallowed doc is
genuinely absent), so filter probes at advance time pay for themselves. A
**tombstone** does *not* — a deleted doc still occupies its slot in every posting
list and is still visited by the pivot logic; skipping it during advance just
forces *another* advance, so the per-advanced-doc `Contains` probe is pure
overhead. The check can therefore move out of `advanceOnTombstoneOrFilter` and
into the scoring branch of `DoBlockMaxWand`/`DoBlockMaxAnd`: a tombstoned pivot is
advanced past but never scored or enqueued. This is **bit-identical** to
advance-time skipping (rejecting a doc at scoring touches neither other docs'
scores nor the heap threshold `worstDist`), trades ~one-probe-per-advanced-doc for
~one-probe-per-scored-candidate (far fewer), and is a no-op when there are no
tombstones. Gated by `deferTombstoneToScore` (default **true**); filters stay at
advance time.

Measured on `climate_fever` (5.4M docs) with synthetic tombstones injected via the
active memtable (`BMW_TOMBSTONE_PCT`); advance-time forced with
`BMW_DEFER_TOMBSTONE=false`:

| scenario (10-term OR worst-case unless noted) | advance-time | deferred | Δ |
|---|---|---|---|
| no tombstones (production common case) | 16.92 ms | 16.97 ms | ~0 (neutral) |
| 2% tombstones | 24.19 ms | 17.95 ms | **−26%** |
| 5% tombstones | 22.84 ms | 17.69 ms | **−23%** |
| 10% filter + 2% tombstones | 11.49 ms | 11.15 ms | −3% |
| typical set (size 5, 2000q), 2% tombstones | 700 µs | 669 µs | −4.5% |

Allocations identical in every case. Deferred latency tracks the no-tombstone
baseline regardless of tombstone rate (the advance side no longer probes).

**Correctness:** bit-identical (2000-query baseline, OR + AND, limit 10/100,
±filter, 0 doc-set changes / 0 score drift); the no-tombstone case is also
bit-identical to forced advance-time, so flipping the default is provably a no-op
on tombstone-free data. The gate-off code path reduces to the pre-change code
(`DoBlockMaxWand` only reorders `InsertAndPop` ahead of the independent advance
loop), which was already == `main`. Real-tombstone path verified with
`TestTombstonesInverted_Compaction` (delete + compact + `DoBlockMaxWand`) and the
`inverted` search suite under `-race`. Every `SegmentBlockMax` consumer (disk
segments + flushing-memtable iterators carrying `activeTombstones`) flows through
`DoBlockMax*`, so the scoring-time re-check covers the whole production path.

Harness: `BMW_TOMBSTONE_PCT` injects synthetic deletes; `BMW_DEFER_TOMBSTONE`
overrides the default only when set, so dump/compare can A/B the two strategies.

## Filtered search + concurrency (harness extensions)

The harness now supports `BMW_FILTER` (build an `AllowList` of given selectivity),
`BMW_CONCURRENCY`, and a `BMW_SCALING` sweep that also captures mutex/block
profiles. Findings on `climate_fever` (5.4M docs, 10-term OR, GOMAXPROCS=14):

### Filter path
With a 10%-selectivity filter, `SegmentBlockMax.advanceOnTombstoneOrFilter →
sroar.Bitmap.Contains` is **~31% of CPU** (sroar `node.search`/`getContainer`/
`keyOffset`/`getValue` traversal) — the per-advanced-doc bitmap membership check
dominates. This reproduces the customer's residual BM25 cost (their
`generateSingleFilter`/`sroar` time) and matches Tier-3 item #8 (tombstone/filter
probing). A per-block filter short-circuit or a cheaper membership structure is
the lever; the naive nil-check removal does not help (the cost is the probe).

### Concurrency contention
Scaling efficiency (per-worker q/s vs single-worker):

| workers | q/s | efficiency |
|---------|-----|-----------|
| 1 | 4,521 | 100% |
| 8 | 33,766 | 93% |
| 14 | 42,117 | 67% |
| 28 | 42,930 | 34% |
| 56 | 39,745 | 16% |

Scales well to ~core count, then plateaus (CPU saturation) and *declines* past it
(real contention overhead). Mutex/block profiles pinpoint two sources:

1. **`segmentRefCounterLock` (`sync.Mutex`)** — `getConsistentViewOfSegments` and
   `ReleaseView` took it twice per query to inc/decRef every segment.
   **DONE:** `segment.refCount` is now an `atomic.Int64`; the mutex is removed.
   incRef still happens under `maintenanceLock.RLock` (ordering vs compaction
   preserved), and awaiting-drop segments only see refs decrease, so the
   lock-free atomic reads are safe. Verified: new `-race` unit tests
   (`TestSegmentRefCountAtomic`, `TestSegmentGroupConsistentViewConcurrent`),
   compaction/inverted/BM25F regression under `-race`, and the mutex profile no
   longer shows the lock. Scaling efficiency improved at/below core count
   (8 workers 93%→99%, 14 workers 67%→73%); past GOMAXPROCS the residual is CPU
   saturation + #2 below, not this lock.
2. **`runtime._LostContendedRuntimeLock`** (~73% of mutex delay at 28 workers) —
   NOT a located call site and not addressed by any change here. It is a Go
   runtime synthetic frame: each M can keep only one runtime-internal-lock
   contention stack, and the overflow is bucketed under this sentinel
   (runtime/mprof.go). It is a *symptom of oversubscription* — many more runnable
   goroutines than cores contending on scheduler/allocator/GC locks — and only
   shows up past GOMAXPROCS, where efficiency is already CPU-bound. Levers, none
   verified to move this frame: (a) bound concurrent queries to ≈ core count
   (the only thing that removes the oversubscription); (b) re-profile unfiltered
   in `go tool pprof` to see which real runtime stacks accompany it (the harness
   top-N hides them); (c) lower allocation/GC traffic *may* shrink the
   allocator-lock portion, but this is unproven for this frame.

## Measuring each change

Pin inputs and compare throughput (queries/s), the target function's flat %, and
`-benchmem` alloc/op for #1/#2/#5:

```
BASE="BMW_SEGMENTS_DIR=/…/property_text_searchable BMW_QUERY_SIZE=10 BMW_NUM_QUERIES=1000 BMW_SEED=42 BMW_PROFILE_SECONDS=20"
env $BASE go test -tags integrationTest -run TestProfileBlockMaxWand -v -timeout 30m ./adapters/repos/db/lsmkv/
env $BASE go test -tags integrationTest -run x -bench 'BenchmarkBlockMaxWand/size=10' -benchmem ./adapters/repos/db/lsmkv/
```

For per-function isolation, add in-memory micro-benchmarks built on the existing
`NewSegmentBlockMaxTest` (`segment==nil`, no disk) + `createBlocks` with
realistic Zipf postings: `BenchmarkDoBlockMaxWand`, `BenchmarkSegmentBlockMaxScore`,
`BenchmarkSegmentBlockMaxAdvance`, `BenchmarkSegmentBlockMaxDecodeBlock`.
