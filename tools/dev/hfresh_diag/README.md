# HFresh+MUVERA Diagnostic Tools

This directory contains tools for running recall attribution diagnostics on HFresh+MUVERA indexes.

## Overview

The diagnostic workflow is:

1. **Convert HDF5 → JSONL** (if your data is in HDF5 format)
2. **Run diagnostic test** to build index, execute traced queries, and produce recall attribution report

## Prerequisites

### For HDF5 conversion:
```bash
pip install h5py numpy
```

### For running diagnostics:
Go 1.21+ (already required for Weaviate)

## Quick Start

### Step 1: Inspect your HDF5 file

```bash
python tools/dev/hfresh_diag/hdf5_to_jsonl.py --hdf5 /path/to/dataset.hdf5 --inspect
```

This prints the HDF5 structure so you can identify the correct keys.

Example output for object arrays with flattened multi-vectors:
```
=== HDF5 Structure: /path/to/dataset.hdf5 ===

  train: shape=(119461,), dtype=object
    -> sample[0]: type=ndarray, shape=(22272,), dtype=float32
  test: shape=(661,), dtype=object
    -> sample[0]: type=ndarray, shape=(4096,), dtype=float32
  neighbors: shape=(661, 119461), dtype=int64

Top-level keys: ['train', 'test', 'neighbors']
```

### Step 2: Convert HDF5 to JSONL

For object arrays with flattened multi-vectors, use `--dim`:

```bash
python tools/dev/hfresh_diag/hdf5_to_jsonl.py \
    --hdf5 ~/Documents/datasets/custom-Multivector-lotte-lotte-lifestyle.hdf5 \
    --out /tmp/hfresh_diag_lotte \
    --docs-key train \
    --queries-key test \
    --gt-key neighbors \
    --dim 128 \
    --gt-k 100 \
    --limit-queries 100 \
    -v
```

For standard fixed-shape arrays:

```bash
python tools/dev/hfresh_diag/hdf5_to_jsonl.py \
    --hdf5 /path/to/dataset.hdf5 \
    --out /tmp/hfresh_diag_data \
    --docs-key train \
    --queries-key test \
    --gt-key neighbors \
    --gt-k 100 \
    --limit-docs 10000 \
    --limit-queries 100
```

### Step 3: Run diagnostic

```bash
go test -v -run TestHFreshMuveraDiagnostic \
    ./adapters/repos/db/vector/hfresh/... -args \
    -docs=/tmp/hfresh_diag_lotte/docs.jsonl \
    -queries=/tmp/hfresh_diag_lotte/queries.jsonl \
    -groundtruth=/tmp/hfresh_diag_lotte/gt.jsonl \
    -k=10 \
    -searchprobe=64 \
    -rescorelimit=100 \
    -out=/tmp/hfresh_diag_lotte/report.json
```

## HDF5 Structure Support

### Single-vector datasets (standard ANN benchmarks)

```
/train          (N, D) float32 - document vectors
/test           (Q, D) float32 - query vectors
/neighbors      (Q, K) int32   - ground-truth neighbor IDs
```

Each document will be converted to a single-vector multi-vector (1 vector per doc).

### Multi-vector datasets (fixed vectors per doc)

```
/train          (N, M, D) float32 - document multi-vectors
/test           (Q, M, D) float32 - query multi-vectors
/neighbors      (Q, K) int32      - ground-truth neighbor IDs
```

### Object arrays with flattened multi-vectors (requires --dim)

```
/train          (N,) object   - each element is a flat float32 array
/test           (Q,) object   - e.g., train[0].shape = (22272,) for 174 vectors × 128 dims
/neighbors      (Q, K) int64  - ground-truth neighbor IDs
```

Example: if `train[0].shape = (22272,)` and `--dim=128`, it becomes `(174, 128)`.

Use `--dim` flag to specify vector dimensions for reshaping.

### Multi-vector datasets (variable vectors per doc)

```
/train_vectors  (total_vecs, D) float32    - all document vectors flattened
/train_offsets  (N+1,) int64               - offsets for each document
/test_vectors   (total_query_vecs, D) float32
/test_offsets   (Q+1,) int64
/neighbors      (Q, K) int32
```

Use `--docs-offsets-key` and `--queries-offsets-key` flags for this format.

## JSONL Format

### docs.jsonl
```json
{"id": 0, "vectors": [[0.1, 0.2, ...], [0.3, 0.4, ...]]}
{"id": 1, "vectors": [[0.5, 0.6, ...], [0.7, 0.8, ...]]}
```

### queries.jsonl
```json
{"id": "q0", "vectors": [[0.1, 0.2, ...], [0.3, 0.4, ...]]}
{"id": "q1", "vectors": [[0.5, 0.6, ...], [0.7, 0.8, ...]]}
```

### gt.jsonl
```json
{"query_id": "q0", "doc_ids": [5, 12, 3, 8, ...]}
{"query_id": "q1", "doc_ids": [1, 7, 9, 2, ...]}
```

## Diagnostic Report

The diagnostic produces a JSON report with:

### Failure Attribution
- **Routing failures**: Ground-truth doc not in any selected posting
- **Approx failures**: Doc in posting but not in approximate top-k
- **Exact failures**: Doc in approximate top but not in final results
- **Successes**: Doc correctly returned

### Recall Metrics
- Overall recall
- Average per-query recall
- Median, min, max recall
- Recall@1, Recall@5, Recall@10

### Per-Query Details
- Selected centroids
- Approximate top IDs
- Returned IDs
- Scan statistics
- Attribution breakdown

## Converter Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--hdf5` | required | Path to HDF5 file |
| `--out` | required | Output directory |
| `--inspect` | false | Only print HDF5 structure |
| `--docs-key` | train | HDF5 key for document vectors |
| `--docs-offsets-key` | none | HDF5 key for doc offsets (variable-length) |
| `--queries-key` | test | HDF5 key for query vectors |
| `--queries-offsets-key` | none | HDF5 key for query offsets |
| `--gt-key` | neighbors | HDF5 key for ground-truth |
| `--dim` | none | Vector dimension for reshaping flat arrays (required for object arrays) |
| `--limit-docs` | none | Limit number of documents |
| `--limit-queries` | none | Limit number of queries |
| `--gt-k` | none | Limit ground-truth to top-k neighbors per query |
| `-v` | false | Verbose output with sample logging |

## Diagnostic Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-docs` | required | Path to docs.jsonl |
| `-queries` | required | Path to queries.jsonl |
| `-groundtruth` | required | Path to gt.jsonl |
| `-k` | 10 | Number of results per query |
| `-searchprobe` | 64 | Number of centroids to probe |
| `-rescorelimit` | 100 | Maximum candidates for rescoring |
| `-out` | none | Path to output report.json |
| `-verbose` | false | Enable verbose logging |

## Customizing for Different Formats

If your HDF5 structure doesn't match the expected formats, edit the `load_vectors()` function in `hdf5_to_jsonl.py`. The converter is designed to be easily adaptable.

## Notes

- The diagnostic runner bypasses the Weaviate server and calls HFresh directly
- No changes are made to production code or search algorithms
- This is diagnostic-only code for recall attribution analysis
