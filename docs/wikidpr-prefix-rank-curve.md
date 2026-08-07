# Stage-1 prefix budget on wiki-dpr (rq1 centered, 1M subset)

**Status:** third measurement of the truncated-codes arc, branch
`bench/hnsw-truncated-codes` (2026-08-08). Data:
`cmd/bitpack-bench/results/wikidpr-1M-rankcurve.csv`; harness
`bitpack-bench -mode rankcurve` (one streaming pass per query over the
column-major sign-bit store, histogram checkpoint at every 64-bit block).

Setup: first 1,000,000 rows (deterministic) of wiki-dpr-10m-e5b-filtered,
subset-exact ground truth (see `docs/hnsw-truncated-rq-wikidpr.md`),
**400 queries** (every 2nd/3rd of the 1000 NQ queries by stride), k=10.
Codes: 1-bit sign codes of the centered, randomly rotated vectors — the
same representation the HNSW rounds used, and the same store stage 2 would
refine from.

The statistic per query and depth is the **maximum rank over the 10 true
neighbours** under prefix-Hamming distance: the smallest stage-1 candidate
budget that keeps all of the true top-10 alive at that depth. Two tie
conventions: *worst* counts all equal-distance vectors ahead of the
neighbour (guaranteed retention under any tie-breaking); *expected* counts
half the tie bucket.

**Filtered-path framing:** this curve is computed over the whole 1M corpus.
Rank within any allowlist is ≤ rank within the corpus, so every budget
below is an upper bound for every filtered case — the filtered path
inherits these numbers conservatively.

## Budget to retain all 10 true neighbours

Worst-case ties / expected-case ties, 400 queries:

| depth | p50 | p75 | p90 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| 64 | 124,122 / 101,904 | 260,666 / 222,921 | 366,665 / 325,853 | 456,571 / 410,765 | 651,798 / 600,290 | 858,918 / 828,188 |
| 128 | 12,645 / 10,577 | 44,971 / 38,595 | 125,597 / 111,705 | 208,717 / 188,980 | 413,291 / 381,969 | 530,735 / 497,927 |
| 192 | 2,963 / 2,486 | 11,657 / 10,072 | 39,088 / 34,556 | 92,411 / 83,680 | 227,685 / 209,498 | 333,296 / 312,945 |
| 256 | 1,025 / 901 | 3,998 / 3,526 | 14,585 / 12,975 | 26,426 / 23,975 | 116,459 / 108,348 | 157,583 / 145,448 |
| 320 | 442 / 398 | 1,570 / 1,374 | 6,574 / 5,932 | 14,159 / 12,901 | 54,562 / 50,839 | 83,439 / 76,844 |
| **384** | **267 / 243** | **830 / 768** | **2,922 / 2,687** | **6,096 / 5,529** | **19,922 / 18,274** | 108,525 / 101,329 |
| 448 | 186 / 172 | 510 / 470 | 1,895 / 1,722 | 4,089 / 3,760 | 15,066 / 13,846 | 40,602 / 37,612 |
| **512** | **125 / 117** | **369 / 341** | **1,167 / 1,085** | **2,787 / 2,588** | **10,865 / 10,099** | 24,252 / 22,593 |
| 576 | 99 / 92 | 258 / 242 | 652 / 603 | 2,103 / 1,972 | 6,109 / 5,669 | 10,502 / 9,900 |
| 640 | 78 / 73 | 209 / 197 | 465 / 438 | 1,447 / 1,375 | 4,837 / 4,528 | 7,629 / 7,129 |
| 704 | 65 / 62 | 163 / 152 | 462 / 422 | 893 / 833 | 2,927 / 2,735 | 4,643 / 4,331 |
| 768 | 54 / 52 | 125 / 119 | 335 / 316 | 712 / 677 | 1,898 / 1,796 | 4,829 / 4,550 |

## The layout decision

**The p95 budget at depth 384 is 6,096 (worst) / 5,529 (expected) — low
thousands, not tens of thousands.** A stage-1 sweep reading one 64-byte
record per vector (16 B metadata + 384 bits) retains the true top-10 for
95% of queries within a ~6k candidate budget, i.e. 0.6% of the corpus
passes to stage 2. The padded single-line layout closes.

- Depth 512 (80 B, into a second line) buys ~2.2× smaller budgets
  (p95 2,787) — worth it only if stage-2 traffic, not stage-1 bytes,
  dominates.
- The p99 tail at 384 is ~20k and the worst query needs ~108k; a fixed
  budget sized at p95 will drop neighbours on the tail. Stage-2/3 sizing
  or an adaptive budget (e.g. distance-gap based) has to absorb that, or
  accept the tail as the recall cost — 6k covers 95% of queries exactly,
  not 95% of neighbours.
- Depth 64 is useless as a first cut on this corpus (p50 budget 124k =
  12% of the corpus); the elimination cascade should start at 128–192
  bits at the earliest.

## Stage-3 exact-rescore window (depth 768)

The full-width row is the rank of the worst true neighbour under complete
1-bit Hamming, which is exactly the exact-rescore window stage 3 needs
after a full-width stage-2 pass: **p50 54, p75 125, p90 335, p95 712,
p99 1,898** (worst-case ties; expected within 6%). A stage-3 window of
~700 covers 95% of queries; ~1,900 covers 99%. Consistent with the HNSW
rounds, where rq1@768 needed rescore windows of 80–160 at ef=128 for its
0.95-class operating points (HNSW's candidate set is ef-limited, so the
two numbers measure different stages of the same estimator).

## Comparison to DBPedia (round 1 arc)

e5's compressed similarity band costs roughly one extra 64-bit block at
the same absolute depth: at 384 bits, DBPedia's p95 budget was 3,557
(`dbpedia-1M-rankcurve2.csv`, center+rotate) vs 6,096 here (1.7×), and
wiki-dpr@384 ≈ DBPedia@320 (6,862). Note the fractional framing differs —
384 bits is 25% of DBPedia's rotated width but 50% of wiki-dpr's — so per
retained *fraction*, e5 is actually the friendlier corpus; per retained
*byte*, which is what the record layout pays for, it costs one extra
block. The curve shape (steep to ~5–6 blocks, diminishing after ~8) is
unchanged.

## Caveats

- 400 queries: percentile granularity at p99 is ±4 queries; treat p99 and
  max as tail indicators, not tight estimates.
- Budgets count vectors, not bytes; stage-1 cost per candidate differs by
  layout (this measurement is layout-independent).
- Single rotation seed (the production default). Tail queries are
  seed-dependent in principle; the p50–p95 band was seed-stable in the
  sphere round and is expected to be here too.
