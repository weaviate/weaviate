#!/usr/bin/env python3
"""H3 sub-ef filters for the wiki-dpr 10M sidecar: 32 and 300 random members
(seed 42), exact GT over members for all 1000 queries, padded to 100 with a
sentinel. Appends meta entries to a separate meta-subef.json (merged by hand
or via --metaout) to avoid touching the frozen meta.json."""
import h5py
import json
import numpy as np
import os

HDF5 = "/Users/abdel/Documents/Projects/hnsw+pq/datasets/wiki-dpr-10m-e5-filtered/wiki-dpr-10m-e5b-filtered.hdf5"
SIDECAR = os.path.expanduser("~/Documents/datasets/wikidpr-10m-scan")
N = 10_000_000
SEED = 42
SIZES = [32, 300]
PAD = np.int64(2**62)

rng = np.random.default_rng(SEED)
f = h5py.File(HDF5)
q = np.fromfile(os.path.join(SIDECAR, "test.f32"), dtype="<f4").reshape(-1, 768)
metas = []
for size in SIZES:
    ids = np.sort(rng.choice(N, size, replace=False))
    vecs = np.stack([f["train"][int(i)] for i in ids]).astype(np.float32)
    # corpus is L2-normalized, metric cosine == dot
    S = q @ vecs.T  # (1000, size)
    order = np.argsort(-S, axis=1)
    gt = np.full((len(q), 100), PAD, dtype=np.int64)
    top = min(100, size)
    gt[:, :top] = ids[order[:, :top]]
    name = f"subef_{size}"
    mask = np.zeros(N, dtype=bool)
    mask[ids] = True
    np.packbits(mask).tofile(os.path.join(SIDECAR, "filters", name + ".bits"))
    gt.astype("<i8").tofile(os.path.join(SIDECAR, "filters", name + ".gt.i64"))
    metas.append({"name": name, "family": "subef", "size": size, "scope": "global", "query_row": 0})
    print(name, "members:", size, flush=True)

json.dump(metas, open(os.path.join(SIDECAR, "meta-subef.json"), "w"), indent=1)
print("done")
