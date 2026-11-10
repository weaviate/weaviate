#!/usr/bin/env python
"""Write the token ids of a tuskanny/<dataset>_<encoder> folder as the side
file the TachiomFresh benchmarks read (TACHIOMFRESH_TOKENIDS): the uint32
ids of token_ids.npy, little-endian, no header, one per document token in
the order of documents.npy, which is the order of the MVC1 cache's docTokens
section (convert_beir_npy.py keeps file order and does not subsample). The
MVC1 cache itself is not touched.

    python3 bench/export_token_ids.py \
        $BENCH_HOME/datasets/raw/nfcorpus_colbertv2 \
        $BENCH_HOME/datasets/nfcorpus-colbertv2.tokenids.bin
"""

import os
import sys

import numpy as np

src, out = sys.argv[1], sys.argv[2]
ids = np.load(os.path.join(src, "token_ids.npy"), allow_pickle=False)
doclens = np.load(os.path.join(src, "doclens.npy"), allow_pickle=False)
assert ids.ndim == 1 and ids.shape[0] == doclens.sum(), f"{ids.shape} token ids, doclens sum {doclens.sum()}"
assert ids.min() >= 0 and ids.max() < 2**32
ids.astype("<u4").tofile(out)
print(f"wrote {out}: {ids.shape[0]} token ids, {len(np.unique(ids))} types, {os.path.getsize(out)} bytes")
