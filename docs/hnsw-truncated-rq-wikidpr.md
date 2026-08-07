# Truncation cost per bit depth on wiki-dpr (768d e5, NQ queries)

**Status:** second round on branch `bench/hnsw-truncated-codes` (2026-08-08).
Data: `cmd/bitpack-bench/results/wikidpr-1M-rq1.csv`, `wikidpr-1M-rq8.csv`,
`wikidpr-1M-swaps.csv`. Same harness and machine as
`docs/hnsw-truncated-rq-codes.md` (M1 Max, 128 B cache lines,
single-threaded queries, maxConn=32 efC=128).

Dataset: **first 1,000,000 rows (deterministic)** of
`wiki-dpr-10m-e5b-filtered.hdf5` train; all 1000 NQ test queries. The
shipped ground truth is for the full 10M set and is invalid for any subset,
so exact top-100 by inner product was recomputed over the subset
(`bitpack-bench -mode gt`, 25 s). 1000 queries give recall@10 a sampling
noise of roughly ±0.5 pt; differences smaller than that are not called.

This corpus is deliberately harder than DBPedia/ada002: half the
dimensions, a compressed similarity band, and real out-of-corpus NQ
questions. Comparisons are **within each bit depth** (truncated vs full);
no cross-depth reference is implied.

## Headline

1. **Truncation costs roughly the same fraction of recall at both bit
   depths; what actually separates arms is centering.** Ceilings at ef=128
   (recall@10, any rescore window): rq1-centered 0.971 → 0.937 → 0.893 →
   0.792 for 768 → 512 → 384 → 256; rq8-uncentered 0.979 → 0.935 → 0.826
   for 768 → 512 → 384. Per retained fraction, the drop is comparable —
   6.6× more bytes do not buy rq8 out of the truncation penalty.
2. **On this dataset centering is decisive, OOD queries notwithstanding.**
   rq1 centered vs uncentered ceilings: +7 pts at full width (0.971 vs
   0.900), +20 at 512, +33 at 384, +49 at 256. The synthetic-probe worry
   from round 1 (centering could hurt with out-of-distribution queries) did
   not materialize: NQ questions embed close enough to the corpus mean.
3. **Uncentered truncation also degrades the graph itself.** Edge counts
   collapse with width in the uncentered arms (rq1u: 31.6M → 23.0M → 20.0M
   → 13.6M edges; rq8: 33.7M → 24.6M → 18.3M) while centered arms hold
   31–33M edges at every width. This is unlike DBPedia round 1, where the
   graph was width-invariant. Swap-diagnostic split below.
4. **The 64-byte code (rq1 centered @384 = 48 B payload + 16 B metadata,
   one x86 cache line) does not hold 0.95 here.** It ceilings at 0.893
   (window 160, ef=128). It is a ≥0.85 configuration on this corpus, not a
   0.95 one. At 0.95, the smallest rq1 configuration is full width 768
   (112 B centered).

## rq1 family (centered unless noted)

Codes: metadata is 8 B, +8 B when centered (the ⟨μ,x⟩ word).

| D | B/code (cent) | codes MiB | graph MiB (edges) | raw @ef128 | w=100 @ef128 | ceiling @ef128 | best seen |
|---|---|---|---|---|---|---|---|
| 768 | 112 | 107 | 204 (33.2M) | 0.695 | 0.969 | 0.971 | 0.984 @ef512 w100 |
| 512 | 80 | 76 | 202 (32.7M) | 0.562 | 0.927 | 0.937 | 0.945 @ef512 w100 |
| 384 | 64 | 61 | 201 (32.2M) | 0.475 | 0.875 | 0.893 | 0.893 |
| 256 | 48 | 46 | 198 (31.1M) | 0.365 | 0.765 | 0.792 | 0.792 |

Uncentered ceilings for the same widths: 0.900 / 0.735 / 0.562 / 0.304
(uncentered B/code: 104 / 72 / 56 / 40).

Rescore windows (smallest recorded point at ef=128 unless stated):

| D | ≥0.95 | ≥0.98 |
|---|---|---|
| 768 | window 80 (0.9657, 1393 QPS) | needs ef≥512: w100 → 0.9836 @ 446 QPS (ef=256 w100 = 0.9798, borderline) |
| 512 | not reachable (max 0.945) | not reachable |
| 384 | not reachable (max 0.893) | not reachable |
| 256 | not reachable (max 0.792) | not reachable |

Build times 100–146 s across all arms (uncompressed float baseline was not
rerun on this dataset).

## rq8 family (uncentered, matching production)

| D | B/code | codes MiB | graph MiB (edges) | raw @ef128 | w=100 @ef128 | ceiling @ef128 | best seen |
|---|---|---|---|---|---|---|---|
| 768 | 784 | 748 | 205 (33.7M) | 0.966 | 0.979 | 0.979 | 0.993 @ef512 w100 |
| 512 | 528 | 504 | 179 (24.6M) | 0.575 | 0.930 | 0.935 | 0.950 @ef512 w100 |
| 384 | 400 | 382 | 161 (18.3M) | 0.414 | 0.805 | 0.826 | 0.823 @ef512 w100 |

| D | ≥0.95 | ≥0.98 |
|---|---|---|
| 768 | window 10 @ef128 (0.9665, 1291 QPS) | ef=256 w100 → 0.9883 @ 675 QPS (ef=128 saturates at 0.9788, just under) |
| 512 | ef=512 w100 → 0.9503 @ 393 QPS (barely) | not reachable |
| 384 | not reachable (max 0.826) | not reachable |

Note the raw-code collapse under truncation: rq8@512's no-rescore recall
(0.575) is *worse* than centered rq1@768's (0.695) at 6.6× the bytes. The
truncated prefix of an uncentered rotation carries the corpus-mean bias in
every dimension regardless of bit depth — the per-vector affine range of
8-bit codes does not absorb it. If truncated rq8 is ever productized,
centering is a precondition, same as for 1-bit prefix schemes.

## Build-truncated vs query-truncated (swap diagnostic)

Round 1 (DBPedia) found truncation loss was 100% query-side. On this harder
corpus the uncentered truncated builds lose edges, so the split was
re-measured: build at 768, swap codes to the truncated width over the
unchanged graph, compare against the native truncated build at the same
width and settings (ef=128, w=100):

| arm | native build (w=100 / ceiling) | 768-built graph (w=100 / ceiling) | graph effect |
|---|---|---|---|
| rq1c query@512 | 0.9269 / 0.9373 | 0.9242 / 0.9346 | none (within variance) |
| rq1c query@384 | 0.8749 / 0.8928 | 0.8714 / 0.8901 | none |
| rq1c query@256 | 0.7646 / 0.7917 | 0.7682 / 0.7976 | none |
| rq8 query@512 | 0.9303 / 0.9352 | 0.9418 / 0.9489 | +1.2–1.4 pt (marginal vs variance) |
| rq8 query@384 | 0.8046 / 0.8263 | 0.8379 / 0.8640 | **+3.3–3.8 pt (real)** |

The split matches the edge-count story exactly: **centered truncated builds
produce graphs as good as full-width graphs** (round 1's "loss is all
query-side" replicates), while **uncentered truncated builds produce
measurably worse graphs** on this corpus — the corpus-mean bias in the
truncated distances degrades neighbor selection itself (edges drop 33.7M →
18.3M), and building at full width recovers up to ~4 pts of ceiling at
D=384. If truncated builds are ever productized without centering, build
with full-width distances and truncate only the stored codes.

## Memory reality check

Below ~112 B/code, HNSW's resident memory is graph-dominated: the graph
holds ~200 MiB regardless of width, so shrinking rq1 codes from 107 MiB
(768) to 61 MiB (384) moves total resident memory by only ~15%. The big
memory step was rq1-vs-rq8 (748 → 107 MiB), which is a bit-depth choice,
not a truncation choice. Truncation's memory argument on HNSW is marginal
for 1-bit codes; its real value, if any, is in scan-path sweep cost, which
this round does not measure.

## Caveats

- 1000 queries → ±0.5 pt recall sampling noise, and two identical builds
  differ by up to ~1 pt (parallel insertion order changes the graph:
  rq1c@768 measured 0.9710 vs 0.9618 ceiling across two builds). Treat
  differences under ~1 pt between separate builds as noise; borderline
  calls (e.g. rq8@512 reaching 0.9503) as "at the threshold".
- Rescore reads floats from RAM here; production pays LSM reads. Windows of
  80–160 will cost more than the ~5–10% QPS measured here.
- e5 vectors are L2-normalized; all similarities are inner products
  (cosine-equivalent). Queries use the `query:` prefix embedding of real NQ
  questions (out-of-corpus by construction).
