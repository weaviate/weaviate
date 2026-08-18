#!/usr/bin/env python3
"""Stage 1 data prep for the PathSeer (PR 12137) evaluation.

BEIR-Cohere corpus (beir-cohere-dot-filtered.hdf5, 772,162 x 1024, dot)
subsampled to 500,000 rows (rng seed 42). Filters are category unions at
selectivity 0.10 / 0.40, POS (includes the query's own category) and NEG
(excludes it), one filter per (config, query-category). Exact top-100 GT per
query over its filter, by dot product. Sidecar layout matches pathseerbench:
train.f32, test.f32, filters/<name>.bits (np.packbits big-endian),
<name>.gt.i64, meta.json (scope "group" with query_rows).
"""
import h5py
import json
import numpy as np
import os
import sys

SRC = os.path.expanduser("~/Documents/datasets/beir-cohere-dot-filtered.hdf5")
OUT = os.path.expanduser("~/Documents/datasets/beir-500k-pathseer")
N_SUB = 500_000
N_Q = 1000
SEED = 42
CONFIGS = [("pos", 0.10), ("pos", 0.40), ("neg", 0.10), ("neg", 0.40)]

os.makedirs(os.path.join(OUT, "filters"), exist_ok=True)
rng = np.random.default_rng(SEED)

f = h5py.File(SRC)
n_full = f["train"].shape[0]
sub = np.sort(rng.choice(n_full, N_SUB, replace=False))
tc = f["train_categories"][:][sub]
qc = f["test_categories"][:N_Q]
Q = f["test"][:N_Q].astype(np.float32)

print(f"subsampled {N_SUB}/{n_full}; {len(np.unique(tc))} categories present", flush=True)

# corpus export + score matrix, chunked over the (sorted) subsample
T_path = os.path.join(OUT, "train.f32")
S = np.empty((N_Q, N_SUB), dtype=np.float32)
CH = 50_000
with open(T_path, "wb") as out:
    for lo in range(0, N_SUB, CH):
        hi = min(lo + CH, N_SUB)
        block = f["train"][sub[lo]:sub[hi - 1] + 1]  # contiguous read then gather
        local = sub[lo:hi] - sub[lo]
        chunk = block[local].astype(np.float32)
        out.write(chunk.astype("<f4").tobytes())
        S[:, lo:hi] = Q @ chunk.T
        print(f"  corpus+scores {hi}/{N_SUB}", flush=True)
Q.astype("<f4").tofile(os.path.join(OUT, "test.f32"))

cat_sizes = {int(c): int((tc == c).sum()) for c in np.unique(tc)}
all_cats = sorted(cat_sizes)
metas = []
PAD = np.int64(2**62)  # never a valid id

for corr, s in CONFIGS:
    target = int(s * N_SUB)
    for c in sorted(set(int(x) for x in qc)):
        crng = np.random.default_rng(abs(hash((corr, s, c, SEED))) % 2**32)
        others = [x for x in all_cats if x != c]
        crng.shuffle(others)
        chosen = [c] if corr == "pos" else []
        total = cat_sizes.get(c, 0) if corr == "pos" else 0
        for o in others:
            if total >= target:
                break
            chosen.append(o)
            total += cat_sizes[o]
        mask = np.isin(tc, chosen)
        size = int(mask.sum())
        name = f"{corr}{int(s*100)}_cat{c:02d}"
        qrows = [int(i) for i in range(N_Q) if int(qc[i]) == c]
        if not qrows:
            continue
        np.packbits(mask).tofile(os.path.join(OUT, "filters", name + ".bits"))
        gt = np.full((len(qrows), 100), PAD, dtype=np.int64)
        masked_idx = np.nonzero(mask)[0]
        for gi, qi in enumerate(qrows):
            sc = S[qi, masked_idx]
            top = min(100, len(masked_idx))
            part = np.argpartition(-sc, top - 1)[:top]
            order = part[np.argsort(-sc[part])]
            gt[gi, :top] = masked_idx[order]
        gt.astype("<i8").tofile(os.path.join(OUT, "filters", name + ".gt.i64"))
        metas.append({"name": name, "family": f"{corr}{int(s*100)}", "size": size,
                      "scope": "group", "query_rows": qrows})
    print(f"config {corr}{int(s*100)}: {len([m for m in metas if m['family']==f'{corr}{int(s*100)}'])} filters", flush=True)

json.dump(metas, open(os.path.join(OUT, "meta.json"), "w"), indent=1)
print("done:", len(metas), "filters ->", OUT)
