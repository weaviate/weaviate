# HNSW on truncated rotated codes (RQ) — DBPedia 1M

**Status:** benchmark round on branch `bench/hnsw-truncated-codes` (2026-08-07).
Bench-only code paths; nothing here is reachable from user-facing config.
Data: `cmd/bitpack-bench/results/dbpedia-1M-truncated-hnsw.csv` and
`dbpedia-1M-isobyte-hnsw.csv`. Machine: Apple M1 Max (**128-byte cache
lines** — earlier estimates that assumed 64 bytes need doubling), DBPedia
990k × 1536d (ada002, L2-normalized), 10k queries, k=10, single-threaded
queries, parallel builds. All RQ runs use dataset-mean centering before the
random rotation, then quantization (see
`docs/uncentered-binary-quantization.md` for why centering precedes any
prefix scheme). The prefix of the rotated code is scaled by sqrt(n/D) at
encode time so truncated dot products stay unbiased estimates of the
full-width dot product; the centering correction `⟨μ,x⟩+⟨μ,q⟩−|μ|²` is exact
(stored per code in the otherwise-unused Norm2 slot).

## Headline answers

1. **Can HNSW run on truncated codes without losing recall?** Yes, with
   rescore. Native truncated builds at 1024 and 512 dims match the
   full-width QPS-recall frontier once a modest exact-rescore window is
   applied. Without rescore, truncated codes hit hard recall ceilings
   (0.834 at D=1024, 0.690 at D=512), so rescore is not optional below full
   width.
2. **Where does the truncation loss come from?** Entirely from navigating
   with noisier distances, not from the graph being built with them. The
   build-full/query-truncated diagnostic is point-for-point identical to
   the native truncated build (e.g. at D=512, ef=128, no rescore: 0.6864 in
   both; with rescore=100: 0.9756 vs 0.9749). Fixes should therefore target
   query-time estimation (rescore window, ef), not build-time distances.
3. **Iso-byte (208 B/vector): many dims at 1 bit beats few dims at high
   precision, monotonically.** rq1@1536 is the only 208-byte arm that
   reaches 0.98. HNSW and the filtered-scan path can share one 1-bit store.

## Main sweep — rq8, centered, maxConn=32 efC=128

Baseline (uncompressed float32, cache=all, from round 4): build 462.8 s,
6277 MiB heap = 5801 MiB floats + ~476 MiB rest; ef=128 → 0.9848 @ 495 QPS.

| config | build | codes MiB (B/code) | graph MiB | recall@10 / QPS, ef=128, rescore=100 | rescore window for ≥0.98 @ ef=128 |
|---|---|---|---|---|---|
| rq8 D=1536 | 145.6 s | 1465 (1552) | 199 (31.9M edges) | 0.9848 / 916 | **0 (none)** — raw ef=128 gives 0.9815 |
| rq8 D=1024 | 130.5 s | 982 (1040) | 199 | 0.9836 / 913 | 40 (→0.9827) |
| rq8 D=512 | 117.0 s | 499 (528) | 199 | 0.9749 / 998 | saturates at 0.9774; needs ef=256 + rescore 100 (→0.9831 @ 589 QPS) |

- **rq8 full width is effectively a free win over uncompressed**: same
  recall curve (0.883 @ ef=16 vs 0.885), 1.9–2.1× the QPS, 3.2× faster
  build, and resident vector memory drops 5801 → 1465 MiB. It needs no
  rescore up to ~0.98.
- Graph memory is ~199 MiB at every width (91 MiB packed connections +
  vertex overhead), measured exactly by walking the graph
  (`hnsw.BenchGraphStats`), not by heap deltas. The earlier "6.3 GiB HNSW"
  number was 92% resident floats.
- Recall ceilings without rescore: D=1536 → 0.991, D=1024 → 0.834,
  D=512 → 0.690 (at ef=512). Truncation noise, not graph quality, sets the
  ceiling: raising ef beyond 128 buys almost nothing at truncated widths.

### Build-truncated vs query-truncated (Task 3 diagnostic)

Same-width comparison, no rescore (build=1536+swap vs native build):

| ef | 1536-graph, 512-codes | native 512 build | 1536-graph, 1024-codes | native 1024 build |
|---|---|---|---|---|
| 16 | 0.6471 | 0.6481 | 0.7748 | 0.7740 |
| 128 | 0.6864 | 0.6864 | 0.8287 | 0.8288 |
| 512 | 0.6900 | 0.6900 | 0.8338 | 0.8338 |

Identical to three decimals. The graph built with truncated distances is as
good as the full-width graph; all loss is query-time estimation noise.

## Iso-byte comparison — 208 bytes/vector, centered, same graph params

Payload 192 B in all three; totals include metadata (16 B for rq8/rq4; the
centered rq1 layout is two 8-byte words — step/norm plus the ⟨μ,x⟩
correction — so exactly 208 B; uncentered rq1 would be 200 B).

| arm | raw recall@10 @ ef=128 | with rescore @ ef=128 | reaches 0.98? |
|---|---|---|---|
| rq8, D=192 | 0.4656 | saturates 0.8811 (window ≥160) | no |
| rq4, D=384 | 0.6233 | saturates 0.9660 (window ≥160) | no |
| rq1, D=1536 | 0.8010 | 0.9799 @ 40, **0.9825 @ 80** (1111 QPS) | **yes** |

Monotone in dimensions at every rescore window. For a fixed memory budget,
HNSW wants **many dimensions at low precision**. This is the
architecturally important result: the 1-bit full-width store that the
progressive-scan path uses is also the best HNSW store at this budget, so
the two can share one code store (rq1 data codes + multi-bit query codes),
with HNSW adding only its ~199 MiB graph. Total resident: ~0.4 GiB vs
6.3 GiB for float HNSW, at 0.98 recall with rescore window 80.

## Cache-line footprint per visited node (code fetch only)

| code | bytes | lines @128 B (M1) | lines @64 B (x86) |
|---|---|---|---|
| rq8 D=1536 | 1552 | 13 | 25 |
| rq8 D=1024 | 1040 | 9 | 17 |
| rq8 D=512 | 528 | 5 | 9 |
| rq8/rq4/rq1c 208 B | 208 | 2 | 4 |

**The narrower-codes-are-faster hypothesis did not pay off at this scale on
this machine.** At matched ef=128 (no rescore), QPS moves only ~10–20%
across a 7.5× code-size range (1041 → 1149 → 1174 → 1250 for 1552 → 528 →
208 → 208 B). With a ≤1.5 GiB working set against M1 Max's large SLC and
~400 GB/s DRAM bandwidth, traversal is not memory-bound; the win to expect
on x86 servers (smaller caches, 64 B lines, many concurrent queries) is
real but unproven here.

## Caveats

- **Rescore cost is optimistic**: exact rescore reads original floats from
  RAM in this harness; production reads them from the LSM object store. The
  measured ~10–15% QPS cost of window 80–100 is a lower bound; cold-cache
  disk rescore will cost more and shifts the trade-off toward wider codes.
- **QPS deltas between separate processes carry ±10–15% noise** (macOS
  scheduling/thermals). Example: native-512 measured 998 QPS where the
  in-process swap variant of the same configuration measured 1136. Recall
  numbers are unaffected; cross-process QPS comparisons closer than ~15%
  should not be trusted.
- **Centering's ranking benefit assumes queries share the corpus
  distribution.** A synthetic probe with queries drawn from a different
  distribution than the mean showed centering can slightly *hurt* ranking
  at truncated widths even though the estimator stays unbiased (verified
  against a pure-float reference). On DBPedia with same-model queries it
  behaves as designed; revisit for out-of-distribution query workloads.
- Restart/restore of truncated or centered indexes is unsupported: the
  commit-log RQ record does not carry the new knobs (bench uses a noop
  commit logger). The knobs are not reachable from REST/schema config.

## Code pointers

- Quantizers: `adapters/repos/db/vector/compressionhelpers/`
  `rotational_quantization.go` (8-bit), `rq4.go` (4-bit),
  `binary_rotational_quantization.go` (1-bit), all via `RQOptions`.
- Bench-only hooks: `compressionhelpers.SetBenchRQCenteringMean`,
  `hnsw.BenchGraphStats`, `hnsw.BenchSwapRQCompressor` (the swap
  diagnostic), `RQConfig.TruncatedDims` (`json:"-"`).
- Harness: `cmd/bitpack-bench -mode rqhnsw` (see `truncated_hnsw.go`).
- Drive-by fix in this round: compressed-origin 8-bit `RQDistancer`s
  returned garbage from `DistanceToFloat` (rotated query vs raw float);
  now encode-and-compare like the 4/1-bit quantizers, with a regression
  test (`TestRQCompressedDistancerDistanceToFloat`).
