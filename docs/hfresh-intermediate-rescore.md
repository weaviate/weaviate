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

## Measured results (lifestyle/119K, in-process lab calibrated ±0.5pp vs e2e)

661 queries, recall@10 vs exact GT, A/B via the test seam on the same build:

| probe | rerank | without stage | with stage | Δ | exact-FDE ceiling | distance |
|---|---|---|---|---|---|---|
| 512 | 350 | 69.4% | 73.7% | +4.3pp | 77.4% | 3.7pp |
| 512 | 512 | 72.9% | 77.0% | +4.1pp | 81.2% | 4.2pp |
| 1024 | 350 | 71.3% | **76.1%** | **+4.8pp** | 77.4% | **1.3pp** |
| 1024 | 512 | 75.1% | **79.8%** | **+4.7pp** | 81.2% | **1.4pp** |

At probe 1024 the pipeline sits 1.3-1.4pp from the global exact-FDE
ceiling — the residue is the ~1pp routing loss (A) plus the ~0.3pp M gap,
exactly the study's predicted decomposition. The remaining distance at
probe 512 is that probe's own routing residue.

Latency: p50 +9-13ms (+20-27%) at probe 1024 (39.5→48.4ms at rerank 350,
49.9→63.0ms at rerank 512); at probe 512/rerank 350 the stage was
latency-neutral in this run. Measured with a warm page cache — the added
IO is invisible locally and must be budgeted from the accounting below.

**IO added per query (accounting):** M × 10.24KB full-FDE reads —
rerank 350 → M=2100 → **21.5MB**; rerank 512 → M=3072 → **31.5MB**;
reference: the MaxSim stage reads ~17.5MB (350 × ~50KB objects). This is
the explicit zero-new-storage trade; the RQ8/RQ4 source cuts it 4×/8×.

## e2e recall (recreation/10K, threshold 0.80)

Official e2e protocol run (`test_recall_hfresh_multivector`, this build):
**FAILS at its first sweep point — probe=16 → recall 77.03% < 0.80** (the
test asserts per-probe and aborts, so probes 32-512 were not exercised by
the e2e itself).

Full in-process A/B curve (same protocol as the lifestyle lab; note the
in-process rebuild has a different layout than the server ingest, so
absolute values differ from the e2e — the e2e's 77.0% at probe 16 vs the
lab's 68.6% — while the with/without delta is layout-independent):

| probe | without stage | with stage | Δ |
|---|---|---|---|
| 16 | 66.5% | 68.6% | +2.1pp |
| 32 | 72.6% | 75.9% | +3.3pp |
| 64 | 78.2% | **82.8%** | +4.6pp |
| 128 | 81.4% | **87.1%** | +5.7pp |
| 256 | 83.4% | **89.3%** | +5.9pp |
| 512 | 83.5% | **89.4%** | +5.9pp |

Reading for the decision session: the stage lifts every funnel-bound cell
above the 0.80 threshold (probe ≥ 64 in-process, saturating at ~89.4%,
i.e. recreation's exact-FDE ceiling at this rerank), but probes 16-32 are
routing-bound (category A: 16 of ~350 postings) — no rescore stage can
recover neighbors that were never scanned. The e2e spec demands 0.80 at
EVERY probe including 16, which mixes the routing-bound and funnel-bound
regimes in a single threshold; whether probe=16 should promise 0.80 is a
routing/spec question, not an intermediate-rescore one.
