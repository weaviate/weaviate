# PathSeer (PR #12137) — findings

Status: FINAL (2026-08-19). All stages complete. Plots in `plots/`, raw data
in `data/`, protocol in `PREREGISTERED.md` (committed before the first run).

Protocol: `PREREGISTERED.md` (committed before the first run). Raw data in
`data/`. PR head `1c144e4c73` on main @ `e6e3aa9e89`; evaluated as submitted.

## §1a code-read answers (file:line refer to the PR head)

1. **Seeding: PathSeer does not get it.** The up-to-10 allowlist entry-point
   seeds are gated on `useAcorn` (`search.go:1060`), which is false when
   `filterStrategy: pathseer` (the two atomics are mutually exclusive,
   `config_update.go:115-116`). Every NEG comparison against ACORN therefore
   conflates traversal mechanism with seeding. Decomposed by our labeled
   `pathseer-seeded` variant (below).
2. **`entryPointNode == nil`: no PathSeer guard, but the exposure is a race
   window, not a standing state.** `entrypointDistWithRepair`
   (`search.go:961-986`, base code) repairs dead entrypoints before strategy
   selection; the nil re-read at `search.go:1017-1019` can only be nil if the
   node is deleted between repair and re-read. In that window ACORN falls to
   RRE *and is rescued by its seeds*; PathSeer (like sweeping on main today)
   pops the nil node, drops it (`search.go:324-328`), and returns an empty
   result set silently. Not deterministically pinnable without injection
   hooks; pre-existing behavior for sweeping. Flagged as an asymmetry the PR
   should close (cheapest fix: give PathSeer the same nil→RRE fallback).
3. **No selectivity gate.** `usePathseer := allowList != nil &&
   h.pathseerSearch.Load()` (`search.go:1021`). ACORN is gated by
   `acornFilterRatio` (default 0.4) vs cache size (`search.go:207-219`) and
   by the entrypoint-neighborhood ratio (RRE routing, `search.go:1049`);
   PathSeer runs on every filtered search at any selectivity.
4. **`config_test.go:800` is green, not red** (Copilot's claim is wrong):
   `assert.Contains` passes because the new message contains the old one as a
   prefix. There is still no positive `pathseer` config case — coverage gap,
   not a failure.
5. **The eval harness itself needed `AcornFilterRatio` set explicitly**
   (`hnsw.Config`, default env value 0.4); with the zero value ACORN is
   silently disabled entirely. Worth knowing for anyone reproducing the PR's
   numbers in-process.

## Stage 1 — BEIR-Cohere 500k, float32, dot (the PR's operating point)

500,000 rows subsampled (seed 42) from beir-cohere-dot-filtered, 1000
queries, category-union filters at selectivity 0.10/0.40 × POS/NEG, exact GT,
ef ∈ {64,128,256,512}, medians of 3 passes after warmup. Full table:
`data/stage1-beir500k.csv`.

QPS at interpolated recall (pre-registered decision metric):

| cell | @0.90: pathseer vs best-other | @0.95 | what "best-other" actually ran |
| --- | --- | --- | --- |
| pos10 | 1362 vs 1321 (**+3.1%**) | +3.1% | acorn (188 acorn / 812 rre) |
| pos40 | 1402 vs 1199 (**+16.9%**) | +16.9% | "acorn" arm ran **sweeping** (ratio gate: 0.43 > 0.4) |
| neg10 | 114 vs 532 (**−78.6%**) | −61.4% | acorn (969 acorn / 31 rre), with seeding |
| neg40 | 362 vs 89 (**+304.8%**) | +300.4% | "acorn" arm ran **sweeping** (ratio gate) |

(Timing from the clean rerun, `-trackburned=false`; the earlier tracked run
gave the same structure with pathseer ~9% slower on NEG cells.)

**Seeding decomposition (labeled ours, `pathseer-seeded`):** grafting
ACORN's 10 allowlist entry-point seeds onto PathSeer makes it *worse* on
both axes (neg10 ef128: 14.5→16.2 ms at −0.002 recall). ACORN's neg10
dominance is therefore attributable to its member-only expansion traversal
(distances computed only to members), not to seeding — the NEG comparison
against ACORN stands as a genuine traversal-mechanism result.

**Verdict on the claim: partial reproduction, with a decisive qualifier.**
PathSeer's +43% average is real *only against sweeping*. Where the
production ACORN path actually engages (neg10), PathSeer is 64–80% *slower*
at matched recall: ACORN with seeding does the same job with ~4× fewer
distance computations (ef64: 1,973 vs 8,059). Where selectivity exceeds
`acornFilterRatio` and Weaviate would run sweeping today (the 0.4 cells),
PathSeer is a large genuine win in NEG (+266%) and a modest one in POS
(+16.5%). The PR's averaged claim mixes these regimes.

## Stage 3 — the quantizer axis (BEIR 500k, rq1 vs float32)

Same corpus/filters/protocol as Stage 1, plain rq1 (bits=1, default rescore).
QPS-at-recall gains for pathseer vs best(sweeping, acorn), f32 → rq1:

| cell | @0.90 | @0.95 |
| --- | --- | --- |
| pos10 | +3.1% → +21.3% | same |
| pos40 | +16.9% → +8.5% | same |
| neg10 | −78.6% → −68.5% | −61.4% → −54.7% |
| neg40 | **+304.8% → +206.5%** | **+300.4% → +122.0%** |

Cost model measured directly: `O_filter` (sroar Contains on the real 52.7k
bitmap) = **16.9 ns**; `O_dist` (f32 dot, 1024 dims, SIMD) = **109.7 ns** →
`R_p = 0.154`. Under rq1 the per-distance traversal cost drops to 0.45× (
matched sweeping-slope ratio across all 16 family×ef cells), giving an
estimated pure `O_dist(rq1) ≈ 49 ns` and `R_p(rq1) ≈ 0.34` — a 2.2× shift
toward the filter-dominated side of the paper's axis. The measured effect is
exactly the predicted direction: PathSeer's headline NEG-vs-sweeping edge
shrinks by a third at 0.90 recall and by ~60% at 0.95 when the quantizer we
actually ship is enabled. (pos10's small gain grew — in that cell acorn
mostly routes to RRE and rescore costs dominate; the NEG cells are the ones
the PR's claim rests on.)

## Hypotheses

### H1 — the two-hop window is a one-shot: **CONFIRMED**

Median pop index at which `candidateHeapWasFull` latches: **2–3 at ef=64**
(all families), growing to 9–21 at ef=512, never unset. Deciding numbers in
`data/stage1-beir500k-perquery.csv` (`pop_idx_cand_full`). Additionally,
`maxSecondOrder = maximumConnectionsLayerZero` (64) caps *examined*
second-order edges per pop: measured `two_hop_examined / secondorder_allocs ≈
63`, i.e. the "two-hop expansion" reads roughly **one first-order neighbor's
edge list** before the budget is gone. The PR text's "expansion phase" is, in
implementation, a 2–3-pop accident of ef and M with a one-neighbor budget.

### H2 — dispersed-regime convergence to sweeping: **REFUTED**

PathSeer converges to *neither* strategy (fig2). At ef128 on 10M its distance
count is 7–11% of sweeping's across the whole 10k–500k dispersed band
(random_10k: 130k vs 1.56M) while sitting 10–675× above ACORN's. The
mechanism the hypothesis assumed ("a passing candidate still computes all its
neighbours, so the prefilter rarely bites") is outweighed by the fact that in
a dispersed filter almost every *popped* candidate fails the filter, so the
DOS branch fires on nearly every expansion once results fill. Above ~1M
members the three curves converge (at 5M the acorn arm *is* sweeping via the
ratio gate, and pathseer does ~0.5–0.7× its distances).

### H3 — allowlist smaller than ef: **CONFIRMED, degenerate**

`data/stage2-subef.csv`: with 32 members, `prefilter_skips = 0` at every ef,
and with 300 members at ef=512 — the activation condition
`results.Len() >= ef` is unsatisfiable when `|filter| < ef`. PathSeer then
degenerates to a full-graph sweep: **9,999,704 distance computations,
~19.5 s per query** on the 10M index (sweeping shows the identical pathology;
its early-exit is also conditioned on a full result heap). Production is
protected only by `flatSearchCutoff` (40k default) routing such filters to
flat search. The heuristic is conditioned on the wrong variable — it should
key on `min(ef, |allowlist|)` or on selectivity, not on `results.Len() >= ef`
alone. Cost forced a bounded protocol here (20 queries, ef {64,512}, 1 timed
pass): at 20 s/query the pre-registered 1000-query grid was infeasible; the
sweeping ef64 cell from the main run (200 queries × 4 passes, ~4.3 h) is in
`stage2-wikidpr10m.csv` as corroboration.

### H4 — visited-before-prefilter burn: **CONFIRMED as frequent; recall cost real but small**

Unique prefilter-skipped nodes later re-encountered from a *passing* parent,
per query (Stage 1, pass 1): neg10 ef64 mean **212** (max 646), ef512 mean
1,065 (max 2,853); pos10 ef64 mean 15. The `pathseer-reorder` variant
(ours: prefilter before visit-marking, same seeds) recovers **+0.002 to
+0.007 recall@10** in NEG (largest at neg40 ef64: 0.9496 → 0.9563) at the
price of ~5–9% more distance computations — roughly frontier-neutral, but
it removes an order-dependence (recall depending on which parent reaches a
node first) that the PR should not ship. Copilot's review raised this
correctly; our numbers size it.

Also: the burned-map instrumentation itself cost ~6% latency on NEG cells
(in-process A/A, `stage1-aa.csv`), so Stage-2/3 timing runs disable it
(`-trackburned=false`); plain counters are within the pre-registered 2%.

### H5 — gap structure: **CONFIRMED — ACORN's low-selectivity failure is navigability, not disconnection**

BFS over the dumped 10M layer-0 graph, 900 GT co-member pairs per filter
(`data/gapscan-*.txt`), classified by the longest allowed run of consecutive
excluded nodes:

| filter (sel) | induced subgraph (run 0) | run ≤ 1 (PathSeer's cut) | run ≤ 2 (ACORN's two-hop) |
| --- | --- | --- | --- |
| topical_10k (0.1%) | 0.264 | 0.948 | **1.000** |
| topical_40k (0.4%) | 0.662 | 0.963 | **1.000** |
| topical_100k (1%) | 0.569 | 0.946 | **1.000** |
| topical_400k (4%) | 0.797 | 0.998 | **1.000** |
| topical_1m (10%) | 0.781 | 1.000 | **1.000** |

Two-excluded-hop bridging connects **100% of pairs at every selectivity** —
so ACORN's measured recall collapse on 10M (0.06–0.56 at ≤55k) is
*navigability* (the greedy member-only frontier cannot find the paths), not
disconnection. Effective member degree at 0.1%: 2.2 direct, 13.2 via one
excluded bridge, 203.6 via two. PathSeer's `pass→fail→fail→pass` cut costs
only ~4–5% of pairs below 100k and ~0 above — its graph restriction is mild;
its recall advantage over ACORN comes from keeping non-members in the
navigable frontier (and paying distances for them). This also settles the
brief's 62k question: at that scale the structure percolates; the deficit is
search, not graph.

### H6 — routing threshold: **CONFIRMED — PathSeer is the best graph strategy at every cardinality on 10M, and the crossover must be re-derived**

At target recall@10 = 0.90 (ef-interpolated, `scripts/20_stage2_analysis.py`),
**pathseer is the cheapest graph strategy on 27 of 27 global filters**. ACORN
never reaches 0.90 below 175k members and, where it does, is 2–6× slower than
pathseer (e.g. 2M: 9.7–13.8 ms vs 2.1–3.6 ms). Highlights (mean ms to 0.90):
55k: sweeping 369 / acorn — / pathseer 115; 238k: 67 / — / 15; 531k: 36 / — /
10; 1M: 6–21 / 15–27 / 2.9–7.1; 5M: ~1.4–2.3 all (gate routes acorn to
sweeping). Anticorr (9.9M members, NEG): pathseer ~2× cheaper at matched
recall at τ=0.01, ~3.7× cheaper at matched regret at τ=0 (judged by regret,
pre-registered), neutral at τ=0.03.

Consequences for the scan routing threshold: (1) the graph-side arm of the
crossover should be PathSeer, not ACORN, on this workload class; (2)
quality-first routing is unchanged — the scan's 0.99+ recall at 3–30 ms below
~600k is out of reach of every graph strategy measured here; (3)
throughput-first routing at a 0.90 target moves the crossover down: pathseer
reaches 0.90 at 15 ms @ 238k and 10 ms @ 531k, competitive with the scan's
~15–30 ms in that band, so the threshold needs re-deriving with pathseer as
the third arm (it was pre-registered that we would say this regardless of
which way it landed).

### Retroactive correction to our own benchmark history

The `strategy_used` instrumentation exposed that in-process harnesses must
set `hnsw.Config.AcornFilterRatio` explicitly (the 0.4 default lives in env
config, not in the struct); with the zero value `acornEnabled` is always
false. The crossover round's `cmd/filteredscan` (commit `4c96b5ae4e`) did not
set it, so the "acorn" arm of `bench-results/filtered-scan/wikidpr-10m-acorn.csv`
**actually measured sweeping**. The server-mode pressure round is unaffected
(real ACORN — hence 0.806@62k there vs "0.979" in-process). That CSV should
be re-labeled, and any conclusion drawn specifically about in-process ACORN
re-checked; this round supersedes it for graph-strategy comparisons.

## Bottom line for the PR review

1. **The claim reproduces only against sweeping.** Where ACORN actually
   engages (NEG, ≤ acornFilterRatio selectivity, navigable band), PathSeer is
   60–80% slower at matched recall. Where the ratio gate routes to sweeping
   (selectivity > 0.4) or where ACORN's recall collapses (dispersed filters
   on 10M), PathSeer is a real and sometimes large win (+300% at neg40;
   best-of-graph on 27/27 filters at 0.90 recall on wiki-dpr 10M).
2. **On our 10M workload PathSeer dominates ACORN outright**, because
   ACORN's member-only frontier fails navigability at low selectivity (H5)
   — this is the strongest argument *for* the PR, and it was not the PR's
   own argument.
3. **Required changes before merge:** (a) H4 visited-before-prefilter
   reorder (removes order-dependence; ~frontier-neutral); (b) a selectivity /
   `min(ef, |allowlist|)` gate so PathSeer cannot degenerate into a 19.5 s
   full-graph sweep (H3) — production's flat-search cutoff happens to mask
   it, but `forbidFlat` callers and future config changes would not;
   (c) `SearchByVectorDistance` should either honor pathseer or the config
   docs should say it doesn't; (d) positive `pathseer` case in
   `config_test.go` + deterministic traversal tests; (e) the per-pop
   `secondOrderBuf` allocation (Copilot's point) — pool it; (f) single
   atomic for the strategy enum instead of two booleans; (g) nil-entrypoint
   RRE fallback parity.
4. **The "two-hop expansion" as implemented is nearly vestigial** (H1): a
   2–3-pop window with a one-neighbour budget. Either document it as a
   cheap entry-boost or make it a real, tunable phase; its description in
   the PR text does not match the code's behavior.
5. Under our production quantizer (rq1) the sweeping-relative gains shrink
   (Stage 3, R_p 0.154 → ~0.34) but the qualitative picture holds.

## Protocol deviations (all pre-run or forced, none data-driven)

1. PREREGISTERED said Stage 2 uses "rq1-centered"; the PR base predates
   centered RQ (that is our `feat/filtered-scan` addition), so Stage 2/3 use
   plain rq1 (bits=1). Same quantizer on every arm within every comparison,
   which is the invariant that matters. Cross-round comparison to
   `wikidpr-10m-acorn.csv` (rq1-centered) is therefore indicative only; H6
   is decided intra-round.
2. The burned-map A/A result (8–12% on NEG cells, biasing *against*
   pathseer) forced clean-timing reruns of Stage 1 and Stage 3 with
   `-trackburned=false`. The originals are kept as `*-tracked.csv`; counter
   values (which the map does not affect) remain valid from the tracked
   runs. No runs dropped.
3. The sub-ef filters were cut from the main Stage-2 sweep after the first
   cell measured 19.3 s/query (the pre-registered 1000-query grid would have
   taken weeks) and rerun with a bounded budget (20 queries, ef {64,512},
   1 timed pass, `stage2-subef.csv`). The H3 metric (`prefilter_skips == 0`)
   is exact regardless of query count; the completed 200-query sweeping cell
   from the main run corroborates the timing. All 327 other filters
   completed the full pre-registered grid.
