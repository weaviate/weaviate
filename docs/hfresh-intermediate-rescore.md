# HFresh multivector: intermediate full-FDE rescore stage

## Problem (verification)

The single-vector HFresh path rescores its RQ1 candidates against the full
stored vectors before returning (`search.go`: RQ1 pool at
`q := NewResultSet(rescoreLimit)` l.77 → fetch full vector via `vectorForId`
l.171 → `SingleDist` → `rescored := NewResultSet(k)` l.169). The
multivector path skips that stage entirely: `searchByFDE` ranks the scanned
entries by 1-bit RQ distances only (`q := NewResultSet(rerankBudget)`
l.479) and hands the top-rerankBudget ids straight to MaxSim
(`SearchByMultiVector` l.373-378, "Step 5" l.526-529). The legacy muvera
branch of `SearchByVector` (l.159-166) has the same shape.

The recall study (artifacts/recall-study/, X2/X3/X5) attributes the
dominant recoverable loss to exactly this cut: ~4.9pp of the C funnel at
rerank 350 is 1-bit quantization error, and re-ranking the RQ1 top-M with
better distances recovers essentially all of it (X5: RQ4 ≈ exact-FDE at
every cut; cascade M=2048 within 0.3pp of a full-pool re-rank).

## Target flow

```
RQ1 scan  →  top-M by RQ1 (M derived, see below)
          →  rescore the M against the FULL stored FDE
             (asymmetric: float query FDE vs stored FDE)
          →  top-rerankBudget  →  MaxSim  →  top-k
```

`rerankBudget` keeps its exact meaning: how many candidates reach MaxSim.
External knobs are unchanged; the stage is always on (it is a pipeline
correction, not an optional feature). A single unexported test seam
(`disableIntermediateRescore`, checked once per search) exists so A/B
benchmarks can compare against the previous behavior.

## Sizing M (derived, not a new knob)

`M = max(intermediateRescoreFactor × rerankBudget, intermediateRescoreMinPool)`
with factor = 6 and min pool = 1024. Justification from X5's cascade table
(RQ1-topM re-ranked with a near-exact second stage, lifestyle/119K):

| rerank/cut | M=1024 | M=2048 | M=4096 | full pool |
|---|---|---|---|---|
| 350 | 20.42% C | 20.08% | 19.82% | 19.77% |
| 512 | 17.34% C | 16.46% | 16.14% | 16.16% |

At the default rerank=350, factor 6 lands on M≈2100 — the X5-validated
M=2048 point, within ~0.3pp of a full-pool re-rank; factor 4 (M=1400) sits
between the 1024 (+0.65pp) and 2048 (+0.31pp) points. The 1024 floor covers
small rerank values with the smallest M X5 measured. M is naturally capped
by the scanned pool size (the ResultSet simply does not fill).

X5's M-sensitivity is a first-stage property (does RQ1-top-M contain the
good candidates), so it transfers from the RQ4 simulation to this
exact-FDE second stage unchanged.

## Rescore distance

Stored FDEs are the RAW encoder output (`AddMulti` persists `encoded`
before the index normalizes its own copy), and the query FDE arrives
normalized. The stage therefore computes the cosine with the norm folded
in — `dist = 1 − dot(q̂, d)/‖d‖`, two SIMD dot products per candidate, no
normalization pass, no allocation (the FDE bytes are decoded into a
per-search scratch buffer). This mirrors the `maxSimScoreCosine` fold
introduced for #276 and keeps parity with how the index itself defines
FDE distance (cosine over normalized FDEs). l2-squared collections use the
provider distance directly on the raw FDE.

## Source of truth & extension point

This phase reads the full float FDEs already stored in
`<id>_muvera_vectors` — zero new storage. The fetch+distance is isolated in
one method (`rescoreFDECandidates`) so that when the RQ8-as-truth product
decision lands, only the source changes (a code bucket instead of the float
bucket, and an RQ distancer instead of the fold), not the pipeline shape.

**IO accounting (honest number):** a full FDE is 2560 float32 = **10.2KB**
(not 5KB), so the stage adds M × 10.2KB ≈ **21.5MB/query at rerank=350**
(M≈2100) — comparable to the MaxSim stage's ~17.5MB. This is the explicit
price of the zero-new-storage phase; an RQ8 code source would cut it 4× to
~5.4MB, an RQ4 source 8× to ~2.7MB (X5's cascade IO table). Local latency
will not reflect these bytes (OS page cache); treat them as capacity-planning
accounting.

## Interaction with previous fixes

- **#276 normalization:** untouched — this stage operates on FDEs, never on
  tokens; the token normalization happens before encoding (AddMulti /
  SearchByMultiVector) and inside MaxSim, both outside this stage.
- **#277 / budgets:** `rerankBudget = max(k, rescoreLimit)` semantics are
  preserved; M ≥ rerankBudget by construction, so results are never capped
  below the requested k.
- **searchProbe sentinel/routing:** unchanged; the stage consumes whatever
  the routing produced.

## Expected effect (from the study, to be validated by measurement)

Recall at probe 1024 should move from 71.3% (rerank 350) toward the
exact-FDE pool ceiling of ~76.0-76.3% (ceiling 77.4% global minus ~1pp pool
residue minus ~0.3pp M gap), and from 75.1% toward ~80.9% at rerank 512.
Latency adds ~M × (bucket read + 2 SIMD dots) ≈ 5-15ms/query at rerank 350.
