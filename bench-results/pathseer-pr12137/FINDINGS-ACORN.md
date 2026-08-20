# Sparse-NEG at 10M — findings (the ACORN decision)

Status: FINAL (2026-08-20). Protocol: `PREREGISTERED-ACORN.md` (committed
pre-run, `808b7c2f25`). Data: `data/neg-sparse-graph.csv` (+ per-query gz),
`data/neg-sparse-scan.csv`. Plot: `plots/fig5-negsparse-frontiers.png`.
All six cardinality points (including both optional ones) ran the full
pre-registered grid; the drop order was not needed.

## Verdict

**deprecation supported** — with one construction caveat that the honest
record requires, stated below.

Under the pre-registered rule, engaged ACORN (ratio 1.0, real `acorn:200`
per point, zero RRE routing) beats PathSeer **nowhere** in the decision band
(300k–4M): neither reaches recall 0.90 anywhere, and ACORN's recall maxima
match or trail PathSeer's at every point (300k: 0.130 vs 0.154; 1M: 0.056
vs 0.064; 2M: 0.036 vs 0.040; 4M: 0.020 vs 0.018). By the materiality rule
("reaching a recall the other cannot") ACORN wins no cell.

The recall numbers themselves demand interpretation: **this family is a
similarity tie band by construction**, and set-recall@10 is degenerate on it
for every method, the scan included. Uniform sampling beyond rank K=100k
puts the true top-10 members ~2.5 ranks apart at 4M (4th-decimal cosine
gaps) — below the ordering resolution of graph traversal, of rq1-quantized
scanning, and of any candidate-selection stage that must commit before exact
rescoring. The evidence: recall *decreases* with member count for every arm
(more members = denser ties) while **similarity regret is 1e-4-grade
everywhere and converges across arms** (graphs 0.00010–0.00021 at ef512;
scan 0.00003–0.00007). Every method returns answers of equivalent quality
with different identities. Per the τ=0 precedent (pre-registered in all
three protocols), the quality-matched reading is the meaningful one:

| point (band) | acorn ef512 | pathseer ef512 | pathseer advantage |
| --- | --- | --- | --- |
| 300k | 73.8 ms @ regret 0.00021 | 83.9 ms @ 0.00015 | ~tie (ps better quality, +14% cost) |
| 1M | 52.1 ms @ 0.00015 | 33.9 ms @ 0.00012 | **1.5× faster, better quality** |
| 2M | 57.6 ms @ 0.00014 | 25.0 ms @ 0.00012 | **2.3×** |
| 4M | 62.9 ms @ 0.00012 | 19.8 ms @ 0.00011 | **3.2×** |

Both readings agree: inside the decision band ACORN is never materially
ahead, and at matched quality PathSeer dominates the band's upper half.

**The caveat:** this family is *unstructured* sparse-NEG (uniform members).
BEIR neg10 — ACORN's one win — has *structured* members (natural
categories, clustered). This round therefore establishes that ACORN has no
territory on unstructured NEG at 10M; it does not re-test the structured
case at 10M scale. BEIR neg10 remains a real, 500k-scale, out-of-band
datapoint in ACORN's favor, and deprecation should cite this boundary
honestly.

## Predictions

- **PN1 (ratio repro) — outcome: inverts.** At 1M/10% ACORN's BEIR-neg10
  advantage does not reappear in sign: pathseer ≥ acorn on both readings
  (recall max 0.064 vs 0.056; 1.5× faster at matched regret). The premise
  "induced degree is the axis" is incomplete: BEIR-neg10's members are
  clustered, this family's are uniform — member-set *structure*, not just
  degree, carries ACORN's advantage.
- **PN2 (scale stress) — CONFIRMED.** At 50k, ACORN recall < 0.7 at every
  ef (max 0.202 at ef512) — in fact the lowest of the three arms (pathseer
  reaches 0.488, sweeping 0.186 within its ef-128 bound).
- **PN3 (the flip) — REFUTED.** There is no flip anywhere in 50k–4M; ACORN
  never leads. Its surviving territory on this family is empty.
- **PN4 (scan under NEG) — REFUTED as stated, mechanism differs.** The scan
  does not hold ≥0.99 set-recall (0.86 at 50k falling to 0.05 at 4M) — but
  B1=32768 ≈ B1=4096 (e.g. 0.046 vs 0.022 at 4M), so this is **not** the
  τ=0.01 B1-retention mechanism; it is rq1 resolution inside the tie band
  (the exact top-10 don't survive quantized candidate selection into the
  700-deep rescore). Meanwhile the scan's regret (0.00003–0.00007) is the
  best of any arm — on quality it remains the leader at every point. The
  routing table gets a NEG asterisk of a different kind: on tie-band-like
  filters, set-recall is the wrong SLO for *every* method; regret is the
  metric.
- **PN5 (bubble crossing) — REFUTED in ranking.** Per-pop cost: acorn
  **101–177 µs** (its expansion prices the crossing in filter checks —
  hundreds of thousands of Contains on 10M-range bitmaps per query),
  pathseer ~2.3–3.9 µs, sweeping ~4.8 µs. ACORN's crossing is cheap in
  distances (seeding, 71–521 pops) but not in time at 10M scale: at 1M–4M
  its absolute latency is 2.6–3.2× pathseer's at ef512.

## Bookkeeping

- Production-gate record: ratios are 0.005–0.40; the 0.4 gate uses strict
  `>` (`search.go:214`), so production would engage ACORN at every point
  including exactly 4M/10M. Engaged-at-1.0 also disables the per-query RRE
  entrypoint routing (same field), which at production 0.4 could route some
  4M queries to RRE — boundary noted; `strategy_used` recorded `acorn:200`
  at every point in this round.
- The scan ran under its shipping configuration (feat/filtered-scan,
  centered rq1), as in all prior scan rows; graph arms on the PR base (plain
  rq1). Same-arm comparisons are within-configuration; scan-vs-graph joins
  inherit the closed round's deviation note.
- Sweeping bounded below 400k as always (ef {64,128}); 1M–4M full grid.

## Ready-to-paste paragraphs

**(a) John — closing decision 2 (ACORN):**

> ACORN: deprecation supported, with one honest boundary. We built the
> sparse-NEG family the 10M grid was missing (50 per-query filters × six
> cardinalities, members uniform outside the query's 100k near field) and
> ran engaged ACORN against PathSeer across 300k–4M. It wins nowhere: its
> best recall matches or trails PathSeer at every point, and at matched
> answer quality PathSeer is 1.5–3.2× faster over the band's upper half.
> Its one remaining datapoint is BEIR neg10 — structured (clustered-member)
> NEG at 500k, below the scan threshold, where the scan owns the traffic
> anyway. So: nothing in the routing table routes to ACORN; keep the code
> behind the config flag through one release for external users, deprecate
> in docs now, remove when PathSeer lands. If someone brings a structured
> sparse-NEG workload at 10M scale we have not measured that exact cell —
> it is the only place a surprise could still live.

**(b) The internal ACORN-collapse issue:**

> Adding the sparse-NEG ef curves to the collapse issue: engaged ACORN
> (ratio forced to 1.0, seeding active, zero RRE routing) is the weakest of
> the three graph strategies on unstructured sparse-NEG at 10M — recall
> 0.03→0.20 over ef 64→512 at 50k members vs PathSeer's 0.12→0.49 on the
> identical filters, and it never crosses 0.21 at any ef at any cardinality
> from 50k to 4M. Combined with the closed round's POS-family numbers
> (0.06–0.56 at ≤55k, H5 gapscan showing 100% pair connectivity under
> two-hop bridging), the diagnosis stands: this is navigability of the
> member-only frontier, not graph disconnection, and it does not improve
> with ef — "severe degradation" would recover with budget; this doesn't.
> Answer for the issue title: collapse.

**(c) Routing doc — the NEG-sparse band:**

> NEG-sparse band owner: the scan below ~300k members (it is the quality
> leader at every cardinality we measured — regret 3e-5–7e-5 vs the graphs'
> 1e-4–2e-4 — and holds its latency curve), PathSeer above it (1.5–3.2×
> faster than any alternative at matched quality from 1M up; crossover
> per the m\* = √(73.4·ef·N) formula). ACORN appears nowhere in the table.
> One asterisk, and it applies to every engine equally: on filters whose
> admitted set forms a similarity tie band (uniform membership beyond a
> distance horizon — our neg-sparse family is the extreme case), set-recall
> is not a meaningful SLO; every method returns equivalent-quality answers
> with different identities (regret ~1e-4 across the board while recall@10
> falls to 0.02). Quality SLOs for such workloads should be stated as
> similarity regret, and the B1=32k retention fix is not implicated (B1
> 4096 vs 32768 differ by <2.5 recall points here — this is quantizer
> resolution, not prefix retention).
