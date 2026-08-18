# PathSeer (PR #12137) — findings

Status: DRAFT — Stage 1 complete; follow-ups (A/A, reorder, seeded, rq1) and
Stage 2 (wiki-dpr 10M) in flight. Numbers below are final for Stage 1.

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
| pos10 | 1364 vs 1387 (**−1.7%**) | −1.7% | acorn (188 acorn / 812 rre) |
| pos40 | 1401 vs 1203 (**+16.5%**) | +16.5% | "acorn" arm ran **sweeping** (ratio gate: 0.43 > 0.4) |
| neg10 | 104 vs 527 (**−80.3%**) | 321 vs 290 → −64.2% | acorn (969 acorn / 31 rre), with seeding |
| neg40 | 323 vs 88 (**+266%**) | +264% | "acorn" arm ran **sweeping** (ratio gate) |

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

### H2 — dispersed-regime convergence to sweeping: Stage 1 says REFUTED at 10%, Stage 2 (0.1–1%) pending

At neg10 (10% selectivity) pathseer does 8.1k dist comps at ef64 vs sweeping
46k and acorn 2.0k — it converges to *neither*; the prefilter does engage
(results fill at median pop ~242) and cuts ~5.7× vs sweeping. The truly
dispersed regime (0.1–1% on 10M) is what the hypothesis is about — pending.

### H3 — allowlist smaller than ef: pending (subef_32 / subef_300 filters in Stage 2)

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

### H5 — gap structure: pending (gapscan on the 10M layer-0 dump)

### H6 — routing threshold: pending (Stage 2)

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
