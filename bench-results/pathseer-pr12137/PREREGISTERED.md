# PathSeer (PR #12137) — pre-registered evaluation protocol

Written and committed **before any measurement run**. Author: Claude (review
session for abdel@weaviate.io). Date: 2026-08-18.

PR under test: weaviate/weaviate#12137, head `1c144e4c73`, single commit based
on `main` @ `e6e3aa9e89` (2026-08-07). Applies cleanly. Evaluated as submitted,
in a dedicated worktree (`weaviate-pr12137`). No modification to PathSeer's
logic in any arm reported as "the PR". Any variant we build (H4 reorder,
seeding graft) is labeled `ours-*` and reported separately.

## Claim under test

The PR claims +42.98% throughput at 90% recall and +43.62% at 95% recall vs
existing strategies, averaged over its BEIR ~500k workloads (selectivity 0.1 /
0.4, POS / NEG correlation).

## Code-read facts that condition the design (established before runs)

1. **PathSeer receives no entry-point seeding.** The allowlist seeding block
   (`search.go:1060-1089`, gated `allowList != nil && useAcorn`) is skipped
   because `acornSearch` is false under `filterStrategy: pathseer`. ACORN gets
   up to 10 allowlist members as extra entry points; PathSeer starts from the
   global entrypoint only. Every NEG-correlation comparison therefore
   conflates the traversal strategy with the seeding difference. We will
   report NEG gaps as "PathSeer-as-submitted vs ACORN-with-seeding" and not
   attribute them to the traversal mechanism alone.
2. **No nil-entrypoint guard.** `usePathseer` short-circuits strategy
   selection (`search.go:1023-1024`); ACORN's `entryPointNode == nil → RRE`
   fallback (`search.go:1026-1027`) is bypassed. A nil entry node under
   PathSeer enters the candidate heap, is popped, skipped
   (`search.go:324-328`), and the search silently returns empty results.
   To pin: deterministic unit test, not a benchmark.
3. **No selectivity gate.** `usePathseer := allowList != nil &&
   h.pathseerSearch.Load()` (`search.go:1021`). ACORN is gated by
   `acornFilterRatio` vs cache size (`search.go:207-219`); PathSeer applies to
   every filtered search at any selectivity, including 99% allowlists.
4. `config_test.go:800` is **green, not red**: `assert.Contains` passes
   because the new message contains the old one as a prefix. No positive
   `pathseer` config case exists. (Copilot's "will fail" claim is wrong.)

## Fairness rules

- Same base commit, same worktree, same binary for all three arms; strategy
  switched at runtime via `UpdateUserConfig` (both atomics, `config_update.go:115-116`).
- Same quantizer on every arm of a given comparison.
- `flatSearchCutoff = 0` in all graph arms (otherwise filters <40k members
  route to flat search, `search.go:88-89`, and no strategy is exercised). We
  note wherever production would have routed to flat.
- No tuning of either side. No dropped runs. Smoke runs (corpus limit ≤ 50k,
  used only to verify the harness and counters work) are excluded from
  analysis and never cited as results.
- Instrumentation is counting-only, env-gated (`PATHSEER_COUNTERS=1`), never
  shipped. A/A overhead check on one cell (counters on vs off, same seed): if
  median latency differs >2%, timing passes rerun with counters off.

## Protocol

Single-threaded, in-process (adapted `cmd/filteredscan` harness in the PR
worktree), settle-before-measure: 1 untimed warmup pass, then 3 timed passes;
report medians across passes; per-query latencies and counters logged to CSV.
k=10, recall@10 against exact ground truth. M and efConstruction at Weaviate
defaults.

### Stage 1 — the PR's operating point

Corpus: BEIR-Cohere 1024-dim dot (`beir-cohere-dot-filtered[-negative].hdf5`),
subsampled to 500,000 rows (uniform stride, seed 42), **float32 uncompressed**
(closest to the PR's setup). Queries: first 1000 test queries. Filters:
category-union bitmaps at selectivity 0.10 and 0.40; POS = query's own
category plus random categories to target size; NEG = random categories
excluding the query's own (seed 42). Exact GT by brute force over each
allowlist. Arms: sweeping / acorn / pathseer × ef ∈ {64, 128, 256, 512}.

**Decision:** compute QPS at interpolated recall 0.90 and 0.95 per (sel, corr)
cell. PathSeer *reproduces* if its QPS-at-recall gain over the best of
(sweeping, acorn) is ≥ +20% in at least 2 of the 4 cells; *does not
reproduce* if < +10% in all cells; otherwise *partial*. If it reproduces, the
finding is real at their operating point and we say so; the review then
focuses on our regime, correctness, and maintenance cost.

### Stage 2 — our regime

Corpus: wiki-dpr-10m-e5b (10M × 768, cosine, all-normalized), **rq1-centered**
(the quantizer of every prior round, so numbers are directly comparable to
`bench-results/filtered-scan/wikidpr-10m-acorn.csv`). Queries: 1000 NQ.
Filters: all 27 global bitmaps (10k → 5M, four families) + all 300 per-query
anticorr (τ ∈ {0, 0.01, 0.03}, ~9.9M members) + two constructed sub-ef
filters (32 and 300 random members, exact GT, for H3). τ=0 judged by
similarity regret, not set recall.

Grid: acorn and pathseer at ef ∈ {64, 128, 256, 512} on everything.
Sweeping (pre-registered cost bound, its low-selectivity slowness is already
established): full ef grid at size ≥ 400k; ef ∈ {64, 128} and first 200
queries at size < 400k. Reported as such.

### Stage 3 — quantizer axis (only if stages 1–2 are clean)

Stage 1's BEIR 500k slice rerun with rq1-centered; plus microbenchmarks of
`O_filter` (ns per `allowList.Contains`) and `O_dist` (ns per distance, f32
vs rq1) to report the actual `R_p = O_filter/O_dist` per configuration.
Prediction: rq1 shrinks `O_dist` ~an order of magnitude, moving Weaviate
toward the band where the paper's own model prefers distance-first, so
PathSeer's edge (if any) shrinks under rq1.

## Hypotheses and decision thresholds

- **H1 (two-hop window is a one-shot).** Metric: `pop_index_when_candidate_
  heap_first_full`. Confirmed if median ≤ 3 at ef=64/M=32 and the median
  grows with ef. Additional pre-registered observation: `maxSecondOrder =
  maximumConnectionsLayerZero` caps *examined* second-order edges per pop at
  2M (64), i.e. roughly one first-order neighbor's list — we predict
  `two_hop_nodes_examined ≈ 64 × window_length`, far below "all two-hop
  neighbors".
- **H2 (converges to sweeping, not ACORN, in the dispersed regime).** Metric:
  per-query `distance_computations`. Confirmed at a given filter if
  pathseer/sweeping ∈ [0.8, 1.2] while |pathseer/acorn − 1| > 1.0 for
  selectivity < 1%. Chart: distance computations vs cardinality, three arms.
- **H3 (degenerate below ef).** Confirmed if `prefilter_skips == 0` on the
  32-member filter at every ef, and on the 300-member filter for ef=512.
- **H4 (visited-before-prefilter recall hazard).** Metric: count of nodes
  marked visited by `CheckAndVisit` (`search.go:530`), then skipped by the
  prefilter (`search.go:534-547`), then re-encountered as neighbor of a later
  *passing* candidate. Materially above zero = mean ≥ 1 per query in any
  non-degenerate cell. If material, quantify recall cost with `ours-reorder`
  variant (visit-mark only after prefilter passes), same seeds, labeled ours.
- **H5 (gap structure is global).** On induced member subgraphs at ~0.4%
  (40k), ~1% (100k), 4% (400k): sample ≥ 200 member pairs (GT neighbor →
  nearest member seeds), BFS over the full graph, classify shortest
  member-to-member connecting paths by consecutive-excluded-run length.
  Decision: if at 40k ≥ 95% of sampled pairs remain connected when paths are
  limited to excluded-runs ≤ 1 (PathSeer's constraint) the failure mode in
  this band is navigability, not disconnection; report the fraction cut at
  runs ≥ 2, which is exactly what PathSeer loses vs full two-hop ACORN.
- **H6 (does it move the routing threshold?).** Compare pathseer vs acorn at
  ef128 on sizes ~55k, ~238k, ~531k (this round, same harness/quantizer as
  the crossover round). *Material improvement* = +0.02 recall@10 at
  equal-or-lower p50, or equal recall at ≥ 1.5× lower p50. If material, the
  scan-vs-graph crossover (~600k single-thread / ~200-250k saturated) must be
  re-derived with PathSeer as a third arm and we say so in the PR comment
  regardless of what it does to our proposal. If not material, PathSeer
  changes nothing in the region where the scan wins by 89–156×.

## Deliverables

`combined.csv` (one row per dataset × filter × family × τ × strategy × ef ×
pass, with recall, latency percentiles, all §2 counters), three plots
(recall-vs-throughput per cardinality band; distance computations vs
cardinality; two-hop window length distribution), `FINDINGS.md` with every
hypothesis marked confirmed / refuted / inconclusive and the §1a answers.
Refuted hypotheses stay in the document.
