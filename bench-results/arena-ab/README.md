# Arena vs sharded cache A/B — DBPedia 1M through the standard benchmarker

Branch `feat/arena-cache-toggle` (stacked on `feat/arena-byte-cache`, PR
#12546 — the toggle is deliberately NOT part of that PR). Machine: Apple M1
Max, **128-byte cache lines**; x86 (64-byte lines) is where this gets
decided, these numbers are indicative.

## Procedure

Build once from this branch; per bit depth, ONE import (build variance ~1 pt
would eat the signal), then the same persisted index served twice, toggled by
`VECTOR_CACHE_IMPL=sharded|arena`. Benchmarker: weaviate-benchmarking
`ann-benchmark`, dbpedia-openai-1000k-angular.hdf5, cosine, gRPC, parallel
queries, default ef sweep 16..512, k=10. Each query arm: warmup pass
(discarded) + full sweep, ×3 repetitions on a settled store, medians
reported. Sweeps run immediately after an import are depressed by up to 2×
(LSM compaction still running) — three such runs were quarantined
(`results-contaminated`, not in this directory or the plot); warm up and let
the store settle before measuring.

Configs: rq8 (production default) and rq1, both UNCENTERED as production
ships them. The rq1-centered shared-store comparison stays in-process in
`cmd/bitpack-bench` (bench branch).

## Parity gate: PASSED

Within each config, recall is identical between cache arms at every ef point
across all repetitions. Observed spreads are ~1e-13 and occur between
repetitions of the SAME arm too — float-summation order in the benchmarker's
recall aggregation, not a cache effect.

## QPS at matched recall (medians of 3)

| config | ef 16 | 24 | 32 | 48 | 64 | 96 | 128 | 256 | 512 | mean |
|---|---|---|---|---|---|---|---|---|---|---|
| rq8 arena vs sharded | +3.4% | +7.7% | +9.4% | +10.9% | +8.3% | +3.8% | +11.3% | +16.5% | +17.4% | **+9.9%** |
| rq1 arena vs sharded | +7.4% | +4.4% | −3.7% | −1.2% | +1.9% | +4.5% | +2.6% | +5.0% | +6.2% | **+3.0%** |

Plot: `qps-recall-arena-ab.png` (stock visualize.py over per-arm medians;
raw repetition JSONs alongside).

## The mechanism check came out INVERTED

Pre-registered expectation: the arena should help rq1 (200 B records, 2
lines, pointer chase proportionally expensive) more than rq8 (1552 B, ~13
lines, chase amortized). Measured: rq8 gains 3× more than rq1. The
microbenchmark intuition (1.43× on isolated 112 B gathers) does not
transfer linearly to end-to-end. Working hypothesis: at matched ef, rq8
traversal is bandwidth/TLB-bound — 1552 B gathers scattered across ~1.5 GiB
of individual allocations — so the arena's contiguity (predictable page
crossings, prefetch-friendly strides) pays throughout the sweep and grows
with ef; rq1 queries are ~2× faster overall, so fixed per-query costs
(graph edges, priority queues, gRPC) dilute any cache-layout effect, and
200 B sharded allocations already pack densely within small allocator size
classes. Treat the hypothesis as unverified; the x86 run should measure
dTLB misses alongside.
