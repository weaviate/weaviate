# Three-stage filtered prefix scan on real structures — wiki-dpr 10M

Branch `feat/filtered-scan` (on a local integration merge of
`feat/arena-byte-cache` + `feat/rq1-centered`; scan commits are new files +
minimal wiring and re-stack onto whatever base wins). Machine: Apple M1 Max,
128 B cache lines, **single-threaded by design this round**.

Setup: `hnsw.FilteredPrefixScan` over a 10M wiki-dpr index, centered rq1
codes (104 B record, 8 B header) in the **arena uint64 cache**
(`VECTOR_CACHE_IMPL=arena`), sroar allowlists, real stripe locks. Stage 1
reads one 64-byte line per member (header + 448 prefix bits), centered
sign-bit Hamming, bucketed best-B1 (default 4096). Stage 2 re-ranks
survivors full-width with the corrected multi-bit estimator to B2 (700).
Stage 3 rescores exactly from corpus floats mmapped directly out of the
frozen HDF5 (train is contiguous at offset 2048; no import, no copy).
Index build: 10M inserts, deferred centering activation at 10k, 17 min.
Data: `wikidpr-10m-per-filter.csv` (default budgets, all 327 filters),
`wikidpr-10m-anticorr-sweep.csv` (B1 sweep + similarity regret).

## Per-family results (default budgets, exact per-pair GT)

| family | filters×queries | recall@10 | worst filter | perfect queries | p50 (median filter) | worst p99 |
|---|---|---|---|---|---|---|
| topical | 14×1000 | 0.9941 | 0.9890 | 95.0% | 27 ms | 202 ms |
| random | 7×1000 | 0.9974 | 0.9948 | 97.8% | 68 ms | 273 ms |
| conjunction | 6×1000 | 0.9930 | 0.9867 | 94.3% | 17 ms | 76 ms |
| anticorr τ=0.03 | 100×1 | 0.9890 | 0.60 | 94.0% | 312 ms | 449 ms |
| anticorr τ=0.01 | 100×1 | 0.7440 | 0.30 | 10.0% | 330 ms | 463 ms |
| anticorr τ=0 | 100×1 | 0.0180 | 0.00 | 0.0% | 331 ms | 484 ms |

**Gates: topical and random pass ≥0.98** (0.9941 / 0.9974 aggregate; even
the worst individual filter is ≥0.989). Tail: at the p95-derived default
budgets, 2–6% of queries drop at least one neighbour (perfect-query
fractions above) and aggregate recall holds ≥0.99 anyway — the tail is
individual neighbours at estimator-noise margins, not query blowups.

## The anti-correlated family: one gate assumption falsified, one real
## finding, one number that fixes it

**τ=0 is a tie lottery, not a retention failure.** The hard similarity
cutoff packs the filtered candidates against the threshold: ranks 1→4096
span **0.0006** of similarity (0.71976 → 0.71915), thousands of candidates
are equal at float32 precision, and a fresh float32 brute force disagrees
with the frozen GT on 1 of 10 itself. Set-recall against that GT measures
tie-break order. The harness's similarity-regret column proves the scan's
answers are quality-identical: **mean regret 0.00005** (max 0.0001) while
set-recall reads 0.02. No budget changes this (B1=32k: recall 0.021,
regret unchanged) because nothing is broken. The τ=0 gate premise ("exact
by construction") does not survive contact with float32; recommend the
dataset's τ=0 rows be evaluated by regret, not set-recall.

**τ=0.01 is the real finding.** Margins are genuine (40 candidates within
0.005 of rank-10) but the filter excludes the query's near-field, so the
true answers are mid-field vectors whose prefix-Hamming ranks are far worse
than the corpus-wide rank curve that sized B1 — the "rank in allowlist ≤
rank in corpus" bound holds per vector, but the filter *changes which
vectors are the answers*. At B1=4096: recall 0.744, regret 0.00124.
**B1=32768 recovers 0.925 recall and 5× lower regret at unchanged latency**
(330→325 ms p50: stage 1 dominates and stage 2's 8× growth is invisible).
τ=0.03 goes 0.989 → 1.0000 with zero regret. Anti-correlated-shaped
workloads want a larger (or margin-adaptive) B1; it is nearly free.

## Cost anatomy

- **Stage 1 dominates and is DRAM-bound**: 40–116 ns/member depending on
  allowlist density and contiguity — random 1M-member lists pay ~1 DRAM
  miss per record (116 ns); 50%-dense lists drop to ~41 ns (hardware
  prefetch over near-sequential lines); topical (contiguous) 1M lists run
  ~44 ns. Bytes touched: members × 64 B (a 5M-member scan touches 320 MB
  per query at line granularity — the physics behind ~200 ms).
- **Allowlist iteration is its own real term**: ~6–13 ns/member of pure
  sroar walking; 62–86 ms of the ~330 ms at 9.9M members (~20%).
- **Stages 2+3 are noise at default budgets**: ~2 ms + 1–30 ms (stage 3's
  spread is SSD first-touch of mmapped rows; warm rows cost ~1–5 ms per
  700).
- **M1 caveat (128 B lines)**: the 104 B record is ONE M-series line, so
  stages 1 and 2 touch the same line per record here and differ in compute
  only; the stage-1-byte advantage (1 line vs 2) is an x86 claim, untested
  this round.
- Single-threaded: ~330 ms p50 at 9.9M members is one core walking 630 MB
  of lines+bitmap. The scan is embarrassingly parallel over members;
  concurrency is the next round.
