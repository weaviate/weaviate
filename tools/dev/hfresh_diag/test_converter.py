#!/usr/bin/env python3
"""
Test script for the HDF5 to JSONL converter.
Creates a sample HDF5 file with multi-vector data and converts it.
"""

import os
import sys
import tempfile
import subprocess

try:
    import h5py
    import numpy as np
except ImportError:
    print("ERROR: Required packages not found. Install with:")
    print("  pip install h5py numpy")
    sys.exit(1)


def create_sample_hdf5(path: str, n_docs: int = 50, n_queries: int = 5,
                       vecs_per_item: int = 4, dims: int = 128, k: int = 10):
    """Create a sample HDF5 file with multi-vector data."""
    print(f"Creating sample HDF5: {path}")
    print(f"  {n_docs} docs, {n_queries} queries")
    print(f"  {vecs_per_item} vectors per item, {dims} dimensions")

    with h5py.File(path, 'w') as f:
        # Create document vectors (N, M, D)
        train_data = np.random.randn(n_docs, vecs_per_item, dims).astype(np.float32)
        f.create_dataset('train', data=train_data)

        # Create query vectors (Q, M, D)
        test_data = np.random.randn(n_queries, vecs_per_item, dims).astype(np.float32)
        f.create_dataset('test', data=test_data)

        # Create ground-truth neighbors (Q, K)
        # For this test, just use random doc IDs
        neighbors = np.random.randint(0, n_docs, size=(n_queries, k)).astype(np.int32)
        f.create_dataset('neighbors', data=neighbors)

    print(f"  Created with keys: train, test, neighbors")


def main():
    # Create temp directory for test
    with tempfile.TemporaryDirectory() as tmpdir:
        hdf5_path = os.path.join(tmpdir, "sample.hdf5")
        out_dir = os.path.join(tmpdir, "converted")

        # Create sample HDF5
        create_sample_hdf5(hdf5_path)

        # Get converter path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        converter_path = os.path.join(script_dir, "hdf5_to_jsonl.py")

        # Test inspect mode
        print("\n=== Testing --inspect mode ===")
        result = subprocess.run(
            [sys.executable, converter_path, "--hdf5", hdf5_path, "--inspect"],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            print("STDERR:", result.stderr)
            sys.exit(1)

        # Test conversion
        print("\n=== Testing conversion ===")
        result = subprocess.run(
            [sys.executable, converter_path,
             "--hdf5", hdf5_path,
             "--out", out_dir,
             "--docs-key", "train",
             "--queries-key", "test",
             "--gt-key", "neighbors",
             "-v"],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            print("STDERR:", result.stderr)
            sys.exit(1)

        # Verify output files
        print("\n=== Verifying output files ===")
        for fname in ["docs.jsonl", "queries.jsonl", "gt.jsonl"]:
            fpath = os.path.join(out_dir, fname)
            if not os.path.exists(fpath):
                print(f"ERROR: {fname} not created!")
                sys.exit(1)

            with open(fpath) as f:
                lines = f.readlines()
                print(f"  {fname}: {len(lines)} lines")

                # Show first line
                if lines:
                    import json
                    first = json.loads(lines[0])
                    if "vectors" in first:
                        print(f"    First entry: id={first.get('id', first.get('query_id'))}, "
                              f"num_vectors={len(first['vectors'])}, "
                              f"dims={len(first['vectors'][0]) if first['vectors'] else 0}")
                    else:
                        print(f"    First entry: {first}")

        print("\n=== All tests passed! ===")


if __name__ == "__main__":
    main()
