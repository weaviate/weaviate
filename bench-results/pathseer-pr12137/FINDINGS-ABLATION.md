# PathSeer prefilter ablation — findings

Status: FINAL (2026-08-20). Protocol: `PREREGISTERED-ABLATION.md` (committed
pre-run, `49c66f732f`). Data: `data/ablation.csv` (8,593 rows) +
`data/ablation-*-perquery.csv.gz`. Plot: `plots/fig4-ablation-frontiers.png`.
All cells ran the full pre-registered grid; the timebox drop order was not
needed.

## Verdict

**condition earns its keep.**

Per the pre-registered tie rule, the PR's compound condition wins materially
in five non-degenerate cells, all on the broad/anticorrelated side:

| cell | guard vs PR: ΔQPS@0.90 | worst matched-ef Δrecall | rule outcome |
| --- | --- | --- | --- |
| beir-f32 neg10 | −0.0% | −0.0025 | TIE |
| beir-f32 neg40 | −1.2% | −0.0032 | TIE |
| beir-f32 pos40 | +13.7% | −0.0018 | guard ahead |
| beir-rq1 neg10 | +0.4% | −0.0021 | TIE |
| beir-rq1 neg40 | −2.3% | −0.0065 | within margins |
| beir-rq1 pos40 | +5.0% | −0.0023 | guard ahead |
| wiki random_10k (0.1%) | +1.3% | −0.0001 | TIE |
| wiki 238k | +0.3% | −0.0095 | within margins |
| wiki 531k | +2.3% | **−0.0145** | **PR wins** |
| wiki 2M | +11.5% | **−0.0516** | **PR wins** |
| wiki 5M | −7.1% | **−0.0577** | **PR wins** |
| wiki anticorr τ=0.01 | neither reaches 0.90 | **−0.11** | **PR wins** |
| wiki anticorr τ=0.03 | **−26.2%** | **−0.10** | **PR wins** |

The split is systematic, not noise. Where members are scarce (dispersed band,
BEIR NEG at 10–40%), almost every popped parent fails the filter, so
"prefilter from failing parents" and "prefilter from every parent" coincide —
guard ties (and, in pure POS, is ahead because it skips the per-pop parent
check). Where members are plentiful and the *excluded* set is what the query
must traverse — anticorrelated filters, whose excluded 100k are precisely the
query's nearest neighborhood — the parent-fails condition preserves bridges
through the excluded core from passing parents. Guard cuts every non-member
once results fill and loses up to 0.11 recall@10 at matched ef. That is the
PR's named workload, and its condition is what delivers it.

## Predictions

- **PA1 — REFUTED as stated.** ours-no-twohop ties the PR (±3%) in 9 of 13
  cells but loses **−8.1% (f32) / −7.3% (rq1) at neg40** — the two-hop burst
  contributes causally in the dense-NEG band (it seeds members early when the
  entry region is depleted). It is vestigial in the dispersed band and mildly
  negative at pos40-rq1/anticorr τ=0.03 (+6.5/+6.8% without it).
- **PA2 — CONFIRMED.** guard's distance count at random_10k is 0.986–0.990
  of the PR's at every ef (well within ±10%), and both sit within 2.5% of the
  ef/s prediction (65,596 ≈ 64,000 at ef64).
- **PA3 — answered.** Guard ties everywhere the members are scarce; the PR
  wins materially in the broad/anticorrelated band via passing-parent
  bridges (table above). The condition is inert at low selectivity and
  load-bearing at high selectivity.
- **PA4 — CONFIRMED, loudly at random_10k.** puredos computes **zero
  distances** there (the non-member entry's neighbors are all skipped; the
  traversal dies instantly): recall 0.0005, empty-results failure mode. At
  neg40 it plateaus at 0.43–0.56 recall vs guard/PR ≥0.95 — materially below
  guard, even at 40% density. Crossing non-member territory is essential;
  the guard is not superfluous.
- **PA5 — CONFIRMED.** In the dispersed band, cost = distance count ×
  a stable per-node constant: **1.29–1.39 µs/node** (PR and guard identical
  across all efs, rq1, M1 Max single-thread). Graph-side model:
  `T_graph ≈ (ef/s) × 1.31 µs`. Against the scan's measured 56M members/s
  (`T_scan ≈ m/56k ms`), equating gives the closed-form crossover
  `m* = sqrt(73.4 × ef × N)`: **m* ≈ 217k members at ef=64, ≈ 306k at
  ef=128** on N=10M — matching the empirically observed throughput-first
  crossover band from the closed round.

## Caveats

- **The guard inherits H3's degeneracy by construction** (probe: subef_32,
  ef64: 19.3 s/query, 10M distances, `prefilter_skips=0` — results can never
  fill). Any single-condition counter-proposal needs the same
  `min(ef, |allowlist|)`/selectivity gate we asked of the PR.
- Sweeping was left unbounded on BEIR (as in the closed round) and bounded
  on wiki below 400k (as in the closed round); the brief's bound phrasing
  was interpreted as "as the closed round did".
- BEIR cells pool 22 filters per family weighted by query count; wiki cells
  are per filter; anticorr pools 10 pivots per τ.

## Ready-to-paste paragraphs

**(a) For the John message (thresholds decision):**

> The graph arm we should ship is PathSeer as submitted in PR 12137 (with
> the fixes we already listed: the visited/prefilter reorder, a
> min(ef, |allowlist|) gate, and the small mechanical cleanups), not a
> simplified variant. We ablated it: a single-condition prefilter ties it on
> every scarce-member cell but gives up 0.01–0.11 recall@10 at matched ef on
> broad and anticorrelated filters — the parent-fails condition is what
> preserves paths through excluded regions, and anticorrelated filters are
> exactly the case where that matters. For the routing threshold, the
> ablation also gives us the closed form we wanted: in the starved band the
> graph side costs (ef/s) × 1.31 µs per query single-threaded, so against
> the scan's 56M members/s the throughput-first crossover is
> m* = sqrt(73.4 · ef · N) ≈ 217k members at ef=64 (306k at ef=128) on 10M —
> the constant-threshold era can end; both sides now have measured formulas.

**(b) For the PR #12137 follow-up comment:**

> We ran the ablation we promised on the compound skip condition. Verdict:
> it earns its keep. A variant that prefilters one-hop neighbours gated only
> by `results.Len() >= ef` (no parent-pass check, no two-hop) ties PathSeer
> within ±3% QPS-at-0.90-recall on every scarce-member workload we tested
> (BEIR NEG 10%/40% under f32 and rq1, and a 0.1% filter on a 10M corpus,
> where both do ef/s distance computations within 2.5% of each other). But
> on broad and anticorrelated filters (0.5M–9.9M members on 10M), dropping
> the parent-pass check costs 0.01–0.11 recall@10 at matched ef — with
> anticorrelated filters, where the excluded set is the query's own
> neighbourhood, showing the largest loss. So the "skip only when the popped
> parent also fails" clause is doing real work precisely on the negatively
> correlated workloads PathSeer targets, and we're keeping it. Two
> ablation-informed notes for the revision: the two-hop burst matters only
> in the dense-NEG band (−8% QPS without it at 40% NEG; inert in the
> dispersed band — consider documenting it as an entry-boost rather than a
> phase), and the prefilter's `results.Len() >= ef` activation needs a
> `min(ef, |allowlist|)` gate, since any filter smaller than ef currently
> degenerates to a full-graph sweep (19.5 s/query on 10M in our
> measurement) — that applies to our simplified variant just as much.
