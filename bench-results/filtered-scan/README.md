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

## Scan vs the current filtered graph path (ACORN), matched recall

Same index corpus, same filters/queries/GT. ACORN arm: default-quality
graph (M=32, efC=128, 23 min build), `SearchByVector` with the allowlist
(ACORN is the shipped default strategy), bits=1 default rescore (512),
ef ∈ {64,128,256,512}, untuned. Per filter, ACORN's cheapest ef matching
the scan's recall is compared on p50. Combined data:
`wikidpr-10m-combined.csv`.

| members | winner | margin (median filter) |
|---|---|---|
| 10k–60k | **scan** | 89–156× (ACORN needs 390–500 ms to match recall at ~0.4% selectivity) |
| 100k–600k | **scan** | 1.3–5.6× |
| 1M–2M | **acorn** | 2.2–11× |
| 5M–10M (incl. all anticorr) | **acorn** | 5–127× |

**The routing threshold is a member count, not a family: ~600k–1M members
(≈6–10% selectivity) on this corpus/machine.** Scan cost is linear in
members; graph cost is ef-shaped and nearly size-independent, but explodes
at low selectivity (the 10k–60k row) where traversal crawls through
blocked nodes.

**The predicted headline inverted.** Anti-correlated was expected to be
ACORN's weakness and the scan's exact win; measured, ACORN cruises there
(12–63 ms) and the scan pays its worst case (310–344 ms). Cause: these
anti-correlated filters are anti-correlated in *direction* but ~99% DENSE
(they exclude only the query's 100k near-field) — near-unfiltered graph
search. ACORN's real weakness is *low selectivity*, which on this dataset
is where the scan is 1.3–156× faster at equal recall. Caveats kept from
the τ analysis: at τ=0 both methods sit in the tie-lottery regime (12/100
ACORN-unmatched at ef512, regrets ~1e-4 on both sides — set-recall is not
a meaningful axis there), and the scan's τ=0.01 retention gap is a
budget artifact fixed by B1=32k at no latency cost.

Routing picture this leaves: below the threshold the scan replaces the
flat-search cutoff (today ~40k) and extends exact-quality filtered search
to ~600k members at single-digit-to-tens of ms; above it, the graph path
owns the regime — including dense-but-adversarial filters.

## Concurrent pressure: scan vs ACORN through the team benchmarker

Server-mode this time (not in-process): one import of wiki-dpr 10M into a
`feat/filtered-scan` server (centered rq1, arena cache, ASYNC_INDEXING),
then restarts on the same persisted index per toggle state — which also
exercised the new `AddBRQCentered` restore path end-to-end, twice. Load:
team benchmarker (gRPC) with a new `--filterSets` mode issuing
`ContainsAny` text filters through the real inverted-index → sroar →
allowlist path. Four cluster-union filter sets: 62,463 / 255,348 /
605,142 / 1,505,618 members, exact GT per set, client concurrency
p ∈ {1,2,4,8,16}, ef=128, k=10. Toggle: `FILTERED_SCAN_THRESHOLD=600000`
vs unset (ACORN). Data: `wikidpr-10m-pressure.csv`, plot:
`wikidpr-10m-pressure.png`. M1 Max, 8 performance cores.

An off-by-5k footnote that became a control: set600k (605,142 members)
sits *above* the 600,000 threshold, so both toggle states ran ACORN on it
— run-to-run agreement within ±1% at every concurrency (same for the
never-routed 1.5M set). The routed measurement at 605k was re-run
separately with threshold 610k.

| members | path | recall | QPS p1 | QPS p16 | p99 p1→p16 |
|---|---|---|---|---|---|
| 62k | scan | **0.9975** | 178 | **911** | 9→32 ms |
| 62k | ACORN | 0.8062 | 71 | 500 | 23→77 ms |
| 255k | scan | **0.9950** | 44 | 229 | 25→150 ms |
| 255k | ACORN | 0.8761 | 56 | 346 | 29→106 ms |
| 255k | ACORN ef512 | 0.9579 | 21 | 141 | 76→211 ms |
| 605k | scan (routed) | **0.9919** | 15 | 88 | 74→318 ms |
| 605k | ACORN | 0.9169 | 59 | 297 | 30→128 ms |
| 1.5M | ACORN (both toggles) | 0.9420 | 51–53 | 236–239 | 29→170 ms |

**Recall under load is bit-stable.** Every (path, size) cell reports the
same recall to 4 decimals at p=1 and p=16. No concurrency-dependent
quality anywhere — the gate this round existed to check.

**No leak.** 370k queries at p16 through the scan path over 8 sustained
minutes: RSS plateaus at 12.4 GB after ~90 s of warmup, then drifts
+0 MB over the final 4 minutes. (Earlier per-round RSS peaks of 15–18 GB
were first-touch warmup across the mixed working sets, not growth.)

**The lock prediction didn't materialize.** ACORN's scaling does not
collapse: 7.1× at p16 on 62k, easing to 4.7× at 1.5M. The scan scales
4.6–5.2×. Both knee at p=8 — the M1 Max's 8 performance cores — so the
ceiling is cores/bandwidth, not the seeding path's read lock, at least
up to p16 on this machine.

**The scan's aggregate capacity is a constant: ~56M members/s.** Across
all three routed sizes at p16: 62k×911 = 57M, 255k×229 = 58M,
605k×88.5 = 54M members/s. Saturated scan QPS on this machine is simply
`56M / members` — the linear-cost model survives concurrency perfectly,
which makes the routing threshold a one-line capacity formula rather
than a lookup table.

**The crossover is load-dependent, and it moves down.** Single-threaded
(previous section) the scan wins to ~600k–1M members. Under saturated
load, ACORN's near-size-independent ~300–350 QPS overtakes the scan's
`56M/members` between 255k and 605k on raw throughput (equal-throughput
point ≈ 190k). At *matched recall* the picture stays scan-friendly
longer: at 255k, ACORN at ef 512 still only reaches 0.958 (vs the scan's
0.995) at 141 QPS vs 229 — the scan strictly dominates. At 605k the scan
holds a +7.5-point recall edge (0.992 vs 0.917) but pays 3.4× the
throughput cost; ACORN at higher ef would land between, still short on
recall. Honest summary: **quality-first routing keeps the threshold at
~600k; throughput-first routing under saturation wants ~200–250k.** The
threshold should ultimately be derived from the capacity formula and the
deployment's load, not hardcoded — and re-derived on x86 (128 B-line
stage-1 advantage untested here, as before).

Latency shape worth noting: the routed scan at 605k/p16 shows p99 318 ms
against ~90 ms p50 — queueing on a per-query cost that is itself ~11 ms
of one core's time. Below ~250k members the scan's p99 stays under
ACORN's at every concurrency while delivering +9 to +19 recall points.
