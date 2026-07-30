# Binary quantization thresholds against zero on uncentered data

**Status:** finding from the bitpack scan experiment (`cmd/bitpack-bench`,
branch `bench/bitpack-progressive-scan`). No production code changed. This
note exists because the issue affects production BQ independently of that
experiment.

## The finding

`compressionhelpers.BinaryQuantizer.Encode` quantizes each dimension by sign:

```go
if vec[i] < 0 { code |= bit }   // binary_quantization.go
```

The threshold is literal zero — there is no learned or computed pivot per
dimension. Neither production call site centers the data first:

- HNSW-BQ: `compressionhelpers/compression.go` (`NewBQCompressor`)
- flat-BQ: `flat/quantizer.go`

Vectors arrive at most L2-normalized (for cosine). Many real embedding models
— OpenAI ada002 among them — produce vectors whose dataset mean has
significant norm, so a zero threshold quantizes against a biased origin: some
bits are nearly constant across the whole dataset and carry almost no
information.

## Measured effect (DBPedia 1M, ada002 1536d, exhaustive 1536-bit Hamming scan, exact rescore of top 350, recall@10 over 10k queries)

| configuration | recall@10 |
|---|---|
| sign bits of raw normalized vectors (production BQ analog) | 0.9979 |
| random rotation, then sign bits (uncentered) | 0.9889 |
| subtract dataset mean, then sign bits | 0.9996 |
| subtract dataset mean, rotate, then sign bits | 0.9996 |

Two observations:

1. **Centering recovers roughly a third of BQ's residual error** in the most
   favourable setting (full-width codes plus generous exact rescore). Under
   any scheme that reads fewer bits or rescores a smaller window, the gap
   widens sharply: with a 64-bit first-cut candidate filter, uncentered runs
   lost 15–40 points of recall where centered runs lost almost none.
2. **Rotating without centering is worse than not rotating at all**
   (0.9889 vs 0.9979). In raw coordinates the dataset-mean bias is
   concentrated in a few dimensions — a few useless bits, the rest clean. A
   random rotation spreads that bias evenly across *every* bit. Any pipeline
   that rotates and then sign-quantizes without centering (the 1-bit
   rotational quantizer follows this shape for its data codes, though its
   multi-bit query codes soften the effect) inherits this.

## Implication

For BQ on uncentered embedding models, computing the dataset mean (or a
per-dimension median pivot) at encoder initialization and thresholding
against it is a small change with measurable recall headroom — and it is a
precondition for any scheme that prunes on code prefixes. The mean must be
persisted with the encoder state, and the same subtraction must be applied
to queries.

Numbers and configurations are reproducible via `cmd/bitpack-bench`
(`results/dbpedia-1M-round2.csv`).
