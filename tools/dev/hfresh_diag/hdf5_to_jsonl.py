#!/usr/bin/env python3
"""
HDF5 to JSONL Converter for HFresh+MUVERA Diagnostic Runner

Converts HDF5 benchmark files into the JSONL format expected by
TestHFreshMuveraDiagnostic.

USAGE:
    python hdf5_to_jsonl.py \
        --hdf5 /path/to/dataset.hdf5 \
        --out /tmp/hfresh_diag_data \
        --docs-key train \
        --queries-key test \
        --gt-key neighbors \
        --dim 128 \
        --gt-k 100 \
        --limit-queries 100 \
        -v

EXPECTED HDF5 STRUCTURE:

For single-vector datasets (standard ANN benchmarks):
    /train          (N, D) float32 - document vectors
    /test           (Q, D) float32 - query vectors
    /neighbors      (Q, K) int32   - ground-truth neighbor IDs

For multi-vector datasets (MUVERA/ColBERT-style):
    /train          (N, M, D) float32  - document multi-vectors (fixed M vectors per doc)
    /test           (Q, M, D) float32  - query multi-vectors
    /neighbors      (Q, K) int32       - ground-truth neighbor IDs

For object arrays with flattened multi-vectors (requires --dim):
    /train          (N,) object  - each element is a flat float32 array
    /test           (Q,) object  - e.g., train[0].shape = (22272,) for 174 vectors × 128 dims
    /neighbors      (Q, K) int32

OR with variable-length multi-vectors:
    /train_vectors  (total_vecs, D) float32    - all vectors flattened
    /train_offsets  (N+1,) int64               - offsets into train_vectors for each doc
    /test_vectors   (total_query_vecs, D) float32
    /test_offsets   (Q+1,) int64
    /neighbors      (Q, K) int32

OUTPUT FORMAT:

docs.jsonl:
    {"id": 0, "vectors": [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]}

queries.jsonl:
    {"id": "q0", "vectors": [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]}

gt.jsonl:
    {"query_id": "q0", "doc_ids": [5, 12, 3, 8, ...]}

DEPENDENCIES:
    pip install h5py numpy

"""

import argparse
import json
import os
import sys
from pathlib import Path

# Defer h5py import until actually needed
h5py = None
np = None


def ensure_dependencies():
    """Import h5py and numpy, exit with helpful message if not available."""
    global h5py, np
    if h5py is not None:
        return

    try:
        import h5py as _h5py
        import numpy as _np
        h5py = _h5py
        np = _np
    except ImportError:
        print("ERROR: Required packages not found. Install with:")
        print("  pip install h5py numpy")
        sys.exit(1)


def inspect_hdf5(path: str) -> None:
    """Print the structure of an HDF5 file."""
    ensure_dependencies()
    print(f"\n=== HDF5 Structure: {path} ===\n")

    def print_item(name, obj):
        if isinstance(obj, h5py.Dataset):
            dtype_str = str(obj.dtype)
            print(f"  {name}: shape={obj.shape}, dtype={dtype_str}")
            # For object arrays, show sample element info
            if obj.dtype == object and len(obj) > 0:
                sample = obj[0]
                if hasattr(sample, 'shape'):
                    print(f"    -> sample[0]: type={type(sample).__name__}, shape={sample.shape}, dtype={sample.dtype}")
                else:
                    print(f"    -> sample[0]: type={type(sample).__name__}, len={len(sample) if hasattr(sample, '__len__') else 'N/A'}")
        elif isinstance(obj, h5py.Group):
            print(f"  {name}/  (group)")

    with h5py.File(path, 'r') as f:
        f.visititems(print_item)
        print()

        # Show top-level keys
        print("Top-level keys:", list(f.keys()))
        print()


def load_vectors(f, key: str, dim: int = None, offsets_key: str = None,
                 limit: int = None, verbose: bool = False):
    """
    Load vectors from HDF5, handling multiple formats:
    - Fixed 2D arrays: (N, D) -> single vector per doc
    - Fixed 3D arrays: (N, M, D) -> M vectors per doc
    - Object arrays with flat 1D elements: requires --dim to reshape
    - Variable-length with offsets

    Returns:
        List of lists of vectors: [[[v1], [v2], ...], [[v1], [v2], ...], ...]
        where each inner list contains the vectors for one document/query.
    """
    if key not in f:
        raise KeyError(f"Key '{key}' not found in HDF5 file. Available: {list(f.keys())}")

    data = f[key]
    shape = data.shape
    dtype = data.dtype

    # Check if using offsets (variable-length multi-vectors)
    if offsets_key and offsets_key in f:
        offsets = f[offsets_key][:]
        all_vecs = data[:]

        n = len(offsets) - 1
        if limit:
            n = min(n, limit)

        result = []
        for i in range(n):
            start, end = int(offsets[i]), int(offsets[i + 1])
            vecs = all_vecs[start:end].tolist()
            result.append(vecs)

            if verbose and i < 3:
                print(f"    [{i}] offsets [{start}:{end}] -> {end-start} vectors")

        return result

    # Object arrays (each element is a separate array)
    if dtype == object:
        n = shape[0] if limit is None else min(shape[0], limit)
        result = []

        for i in range(n):
            item = data[i]
            # Convert to numpy array if needed
            arr = np.asarray(item, dtype=np.float32)

            if arr.ndim == 1:
                # Flat 1D array - needs reshaping with --dim
                if dim is None:
                    raise ValueError(
                        f"Object array element {i} is 1D with shape {arr.shape}. "
                        f"Use --dim to specify vector dimensions for reshaping. "
                        f"Example: if shape is (22272,) and dim=128, it becomes (174, 128)."
                    )
                if len(arr) % dim != 0:
                    raise ValueError(
                        f"Element {i} has length {len(arr)} which is not divisible by dim={dim}. "
                        f"Cannot reshape to (-1, {dim})."
                    )
                reshaped = arr.reshape(-1, dim)

                if verbose and i < 3:
                    print(f"    [{i}] raw shape={arr.shape}, dtype={arr.dtype} -> reshaped to {reshaped.shape}")

                result.append(reshaped.tolist())

            elif arr.ndim == 2:
                # Already 2D: (M, D)
                if verbose and i < 3:
                    print(f"    [{i}] shape={arr.shape}, dtype={arr.dtype}")
                result.append(arr.tolist())

            else:
                raise ValueError(f"Unexpected array dimensionality {arr.ndim} for element {i}")

        return result

    # Fixed-shape arrays
    if len(shape) == 1:
        # 1D array - might be flat vectors, need dim
        if dim is None:
            raise ValueError(
                f"Dataset '{key}' is 1D with shape {shape}. "
                f"Use --dim to specify vector dimensions, or use --docs-offsets-key for variable-length."
            )
        # This case shouldn't normally happen - 1D at dataset level means single flat array
        # Let's treat it as a single document with multiple vectors
        arr = data[:]
        if len(arr) % dim != 0:
            raise ValueError(f"Array length {len(arr)} not divisible by dim={dim}")
        reshaped = arr.reshape(-1, dim)
        return [reshaped.tolist()]

    if len(shape) == 2:
        # Could be (N, D) single-vector or (N, flat) needing reshape
        n = shape[0] if limit is None else min(shape[0], limit)

        # Check if second dimension matches dim (if provided)
        if dim is not None and shape[1] != dim and shape[1] % dim == 0:
            # Flat multi-vectors: (N, M*D) -> reshape each to (M, D)
            result = []
            for i in range(n):
                row = data[i]
                reshaped = np.asarray(row).reshape(-1, dim)

                if verbose and i < 3:
                    print(f"    [{i}] raw shape=({shape[1]},) -> reshaped to {reshaped.shape}")

                result.append(reshaped.tolist())
            return result

        # Single vector per item: (N, D) -> wrap each as [[vec]]
        if verbose:
            print(f"    Format: single-vector (N={n}, D={shape[1]})")
        return [[data[i].tolist()] for i in range(n)]

    elif len(shape) == 3:
        # Multi-vector per item: (N, M, D)
        n = shape[0] if limit is None else min(shape[0], limit)
        if verbose:
            print(f"    Format: fixed multi-vector (N={n}, M={shape[1]}, D={shape[2]})")
        return [data[i].tolist() for i in range(n)]

    else:
        raise ValueError(f"Unexpected shape {shape} for key '{key}'. Expected 1D, 2D, or 3D array.")


def load_ground_truth(f, key: str, limit: int = None, verbose: bool = False):
    """
    Load ground-truth neighbor IDs.

    Returns:
        List of lists of doc IDs: [[id1, id2, ...], [id1, id2, ...], ...]
    """
    if key not in f:
        raise KeyError(f"Key '{key}' not found in HDF5 file. Available: {list(f.keys())}")

    data = f[key]
    shape = data.shape
    n = shape[0] if limit is None else min(shape[0], limit)

    if verbose:
        print(f"    Shape: {shape}, loading {n} queries")
        if len(shape) > 1:
            print(f"    Neighbors per query: {shape[1]}")

    return [data[i].tolist() for i in range(n)]


def write_docs_jsonl(vectors: list, output_path: str) -> int:
    """Write documents to JSONL file."""
    with open(output_path, 'w') as f:
        for doc_id, vecs in enumerate(vectors):
            doc = {"id": doc_id, "vectors": vecs}
            f.write(json.dumps(doc) + '\n')
    return len(vectors)


def write_queries_jsonl(vectors: list, output_path: str) -> int:
    """Write queries to JSONL file."""
    with open(output_path, 'w') as f:
        for i, vecs in enumerate(vectors):
            query = {"id": f"q{i}", "vectors": vecs}
            f.write(json.dumps(query) + '\n')
    return len(vectors)


def write_gt_jsonl(neighbors: list, output_path: str, k: int = None) -> int:
    """Write ground-truth to JSONL file."""
    with open(output_path, 'w') as f:
        for i, nbs in enumerate(neighbors):
            if k is not None:
                nbs = nbs[:k]
            gt = {"query_id": f"q{i}", "doc_ids": [int(x) for x in nbs]}
            f.write(json.dumps(gt) + '\n')
    return len(neighbors)


def main():
    parser = argparse.ArgumentParser(
        description="Convert HDF5 benchmark files to JSONL for HFresh diagnostic runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument("--hdf5", required=True, help="Path to HDF5 file")
    parser.add_argument("--out", required=True, help="Output directory for JSONL files")
    parser.add_argument("--inspect", action="store_true", help="Only inspect HDF5 structure, don't convert")

    # Dataset keys (with sensible defaults)
    parser.add_argument("--docs-key", default="train", help="HDF5 key for document vectors (default: train)")
    parser.add_argument("--docs-offsets-key", default=None, help="HDF5 key for document offsets (for variable-length)")
    parser.add_argument("--queries-key", default="test", help="HDF5 key for query vectors (default: test)")
    parser.add_argument("--queries-offsets-key", default=None, help="HDF5 key for query offsets")
    parser.add_argument("--gt-key", default="neighbors", help="HDF5 key for ground-truth (default: neighbors)")

    # Vector dimensions (required for flat object arrays)
    parser.add_argument("--dim", type=int, default=None,
                        help="Vector dimension for reshaping flat arrays (required for object arrays with 1D elements)")

    # Limits
    parser.add_argument("--limit-docs", type=int, default=None, help="Limit number of documents")
    parser.add_argument("--limit-queries", type=int, default=None, help="Limit number of queries")
    parser.add_argument("--gt-k", type=int, default=None, help="Limit ground-truth to top-k neighbors per query")

    # Verbose
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output with sample logging")

    args = parser.parse_args()

    # Check input file exists
    if not os.path.exists(args.hdf5):
        print(f"ERROR: File not found: {args.hdf5}")
        sys.exit(1)

    # Inspect mode
    if args.inspect:
        inspect_hdf5(args.hdf5)
        return

    # Ensure dependencies are available
    ensure_dependencies()

    # Create output directory
    os.makedirs(args.out, exist_ok=True)

    print(f"Converting: {args.hdf5}")
    print(f"Output dir: {args.out}")
    if args.dim:
        print(f"Vector dim: {args.dim}")
    print()

    with h5py.File(args.hdf5, 'r') as f:
        # Show structure if verbose
        if args.verbose:
            inspect_hdf5(args.hdf5)

        # Load and write documents
        print(f"Loading documents from '{args.docs_key}'...")
        docs = load_vectors(f, args.docs_key, dim=args.dim,
                           offsets_key=args.docs_offsets_key,
                           limit=args.limit_docs, verbose=args.verbose)
        docs_path = os.path.join(args.out, "docs.jsonl")
        n_docs = write_docs_jsonl(docs, docs_path)
        print(f"  Written {n_docs} documents to {docs_path}")

        if n_docs > 0:
            first_doc = docs[0]
            n_vecs = len(first_doc)
            vec_dim = len(first_doc[0]) if first_doc else 0
            print(f"  First doc: {n_vecs} vectors × {vec_dim} dims")

            # Show stats for multi-vector
            if n_docs > 1:
                vec_counts = [len(d) for d in docs[:min(100, n_docs)]]
                print(f"  Vectors per doc (first {len(vec_counts)}): min={min(vec_counts)}, max={max(vec_counts)}, avg={sum(vec_counts)/len(vec_counts):.1f}")

        # Load and write queries
        print(f"\nLoading queries from '{args.queries_key}'...")
        queries = load_vectors(f, args.queries_key, dim=args.dim,
                              offsets_key=args.queries_offsets_key,
                              limit=args.limit_queries, verbose=args.verbose)
        queries_path = os.path.join(args.out, "queries.jsonl")
        n_queries = write_queries_jsonl(queries, queries_path)
        print(f"  Written {n_queries} queries to {queries_path}")

        if n_queries > 0:
            first_query = queries[0]
            n_vecs = len(first_query)
            vec_dim = len(first_query[0]) if first_query else 0
            print(f"  First query: {n_vecs} vectors × {vec_dim} dims")

        # Load and write ground-truth
        print(f"\nLoading ground-truth from '{args.gt_key}'...")
        gt = load_ground_truth(f, args.gt_key, limit=args.limit_queries, verbose=args.verbose)
        gt_path = os.path.join(args.out, "gt.jsonl")
        n_gt = write_gt_jsonl(gt, gt_path, args.gt_k)
        print(f"  Written {n_gt} ground-truth entries to {gt_path}")

        if n_gt > 0:
            original_k = len(gt[0])
            written_k = args.gt_k if args.gt_k else original_k
            print(f"  Original neighbors per query: {original_k}")
            if args.gt_k and args.gt_k < original_k:
                print(f"  Truncated to top-{args.gt_k} neighbors (--gt-k)")

    print()
    print("=" * 70)
    print("Conversion complete! Run diagnostic with:")
    print()
    print(f"  go test -v -run TestHFreshMuveraDiagnostic \\")
    print(f"    ./adapters/repos/db/vector/hfresh/... -args \\")
    print(f"    -docs={docs_path} \\")
    print(f"    -queries={queries_path} \\")
    print(f"    -groundtruth={gt_path} \\")
    print(f"    -k={args.gt_k or 10} \\")
    print(f"    -searchprobe=64 \\")
    print(f"    -rescorelimit=100 \\")
    print(f"    -out={args.out}/report.json")
    print("=" * 70)


if __name__ == "__main__":
    main()
