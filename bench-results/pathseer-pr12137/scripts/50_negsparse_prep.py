#!/usr/bin/env python3
"""neg-sparse family construction (PREREGISTERED-ACORN.md).

For each of the first 50 anticorr pivot queries and each cardinality m in
{50k, 150k, 300k, 1M, 2M, 4M}: members = uniform sample of size m (seed 42,
per-(query,m) stream) from vectors whose similarity rank to the query is
> 100,000. Exact GT = top-100 members by dot. Bitmaps np.packbits, GT .gt.i64,
meta entries scope per_query appended to the sidecar meta.json as family
"negsparse".
"""
import json
import numpy as np
import os

HDF5 = "/Users/abdel/Documents/Projects/hnsw+pq/datasets/wiki-dpr-10m-e5-filtered/wiki-dpr-10m-e5b-filtered.hdf5"
SIDECAR = os.path.expanduser("~/Documents/datasets/wikidpr-10m-scan")
N, DIMS, OFFSET = 10_000_000, 768, 2048
K_EXCLUDE = 100_000
POINTS = [(50_000, "50k"), (150_000, "150k"), (300_000, "300k"),
          (1_000_000, "1m"), (2_000_000, "2m"), (4_000_000, "4m")]
SEED = 42
PAD = np.int64(2**62)

meta = json.load(open(os.path.join(SIDECAR, "meta.json")))
pivots = sorted({x["query_row"] for x in meta if x["family"] == "anticorr"})[:50]
queries = np.fromfile(os.path.join(SIDECAR, "test.f32"), dtype="<f4").reshape(-1, DIMS)
Q = queries[pivots]  # (50, 768)

corpus = np.memmap(HDF5, dtype="<f4", mode="r", offset=OFFSET, shape=(N, DIMS))

# similarity of all corpus rows to the 50 pivots, chunked
S = np.empty((N, len(pivots)), dtype=np.float32)
CH = 500_000
for lo in range(0, N, CH):
    hi = min(lo + CH, N)
    S[lo:hi] = np.asarray(corpus[lo:hi]) @ Q.T
    if lo % 2_000_000 == 0:
        print(f"  sims {hi}/{N}", flush=True)

new_meta = []
for qi, qrow in enumerate(pivots):
    sims = S[:, qi]
    order = np.argsort(-sims)          # rank 0 = most similar
    eligible = order[K_EXCLUDE:]       # ranks > K
    for m, tag in POINTS:
        rng = np.random.default_rng([SEED, qrow, m])
        members = rng.choice(eligible, m, replace=False)
        name = f"negsparse_q{qrow:04d}_m{tag}"
        mask = np.zeros(N, dtype=bool)
        mask[members] = True
        np.packbits(mask).tofile(os.path.join(SIDECAR, "filters", name + ".bits"))
        msims = sims[members]
        top = np.argpartition(-msims, 99)[:100]
        top = top[np.argsort(-msims[top])]
        gt = np.full(100, PAD, dtype=np.int64)
        gt[:100] = members[top]
        gt.astype("<i8").tofile(os.path.join(SIDECAR, "filters", name + ".gt.i64"))
        new_meta.append({"name": name, "family": "negsparse", "size": int(m),
                         "scope": "per_query", "query_row": int(qrow)})
    if qi % 10 == 0:
        print(f"  pivot {qi+1}/50 done", flush=True)

names = {x["name"] for x in meta}
added = [x for x in new_meta if x["name"] not in names]
json.dump(meta + added, open(os.path.join(SIDECAR, "meta.json"), "w"), indent=1)
print(f"done: {len(added)} filters appended; meta entries now {len(meta)+len(added)}")
