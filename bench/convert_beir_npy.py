#!/usr/bin/env python
"""Convert a tuskanny/<dataset>_<encoder> Hugging Face layout into an MVC1
cache, the corpus file the Go benchmark tests read (TACHIOMFRESH_MVC1).

Runs seven loader checks in order and refuses to write the cache unless
check 5 (exact MaxSim vs gt_top100.tsv) matches under exactly one query
padding convention, or under both when they cannot differ.

    python3 bench/convert_beir_npy.py \
        -input $BENCH_HOME/datasets/raw/nfcorpus_colbertv2 -o $BENCH_HOME/datasets/nfcorpus-colbertv2.bin
    python3 bench/convert_beir_npy.py \
        -input $BENCH_HOME/datasets/raw/scidocs_colbertv2 -o $BENCH_HOME/datasets/scidocs-colbertv2.bin -expect-docs 25657

Cache format, little-endian throughout:

    header, 40 bytes
       0  4  magic "MVC1"          20  4  total doc tokens
       4  4  dims                  24  4  total query tokens
       8  4  documents             28  8  subsample seed (0: no subsample)
      12  4  queries               36  4  corpus documents
      16  4  ground-truth depth
    then, in order:
      docIDs        documents         uint32   0..n-1 (file order, no subsample)
      docCounts     documents         uint32   tokens per document
      queryCounts   queries           uint32   tokens per query
      mean          dims              float32  token mean over the docs
      groundTruth   queries*depth     uint32   doc ids, exact rank order
      docTokens     docTokens*dims    float32
      queryTokens   queryTokens*dims  float32
"""

import argparse
import os
import sys
import time

import numpy as np

MAGIC = b"MVC1"
D = 128
GT_FILE = "gt_top100.tsv"
# Depth used only when gt_top100.tsv turns out not to be exact MaxSim
# (check 6).
GT_DEPTH_RECOMPUTE = 1000
N_CHECK_QUERIES = 5


def load(path, name, mmap=True):
    p = os.path.join(path, name)
    if mmap:
        return np.load(p, mmap_mode="r", allow_pickle=False)
    return np.load(p, allow_pickle=False)


def maxsim_all_docs(q, docs, offsets):
    """Exact MaxSim (sum over query tokens of max_d q·d, dot on float32) of one
    query against every document. Blocked: one [T, n_tokens] similarity
    matrix per query, then a per-document segmented max."""
    sims = q @ docs.T  # [T, n_tokens]
    per_doc_max = np.maximum.reduceat(sims, offsets[:-1], axis=1)  # [T, n_docs]
    return per_doc_max.sum(axis=0, dtype=np.float32)


def topk(scores, k):
    """Top-k doc ids by score descending, ties by doc id ascending, so that the
    comparison with the tsv is deterministic."""
    idx = np.lexsort((np.arange(len(scores)), -scores))
    return idx[:k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-input", required=True, help="directory with the .npy/.tsv files")
    ap.add_argument("-o", required=True, help="output MVC1 cache path")
    ap.add_argument("-seed", type=int, default=12345, help="seed for the two random check queries")
    # the dataset's document count, asserted by check 2
    ap.add_argument("-expect-docs", type=int, default=3633, help="expected number of documents")
    args = ap.parse_args()
    path = args.input

    # ---- check 1: dtype and shape of every .npy from the header -------------
    print("== check 1: dtype/shape from headers")
    names = ["documents", "doclens", "doc_ids", "token_ids", "queries", "query_lens", "queries_ids"]
    for n in names:
        a = np.load(os.path.join(path, n + ".npy"), mmap_mode="r", allow_pickle=True)
        print(f"  {n:12s} {str(a.dtype):10s} {a.shape}")

    docs_raw = load(path, "documents.npy")
    doclens = np.asarray(load(path, "doclens.npy")).astype(np.int64)
    queries_raw = load(path, "queries.npy")
    query_lens = np.asarray(load(path, "query_lens.npy")).astype(np.int64)

    # ---- check 2: bitcast, float32, sum/len asserts, norms ------------------
    print("== check 2: documents bitcast + doclens asserts + token norms")
    if docs_raw.dtype == np.uint16:
        docs_f16 = docs_raw.view(np.float16)  # bitcast, never astype
        print("  documents.npy dtype is uint16: reinterpreted as float16 via .view")
    elif docs_raw.dtype == np.float16:
        docs_f16 = docs_raw
        print("  documents.npy dtype is already float16 in the header: no bitcast needed")
    else:
        sys.exit(f"unexpected documents dtype {docs_raw.dtype}")
    n_tokens, dims = docs_f16.shape
    assert dims == D, f"dims {dims} != {D}"
    n_docs = len(doclens)
    print(f"  n_docs={n_docs} n_tokens={n_tokens} dims={dims}")
    assert n_docs == args.expect_docs, f"expected {args.expect_docs} docs, got {n_docs}"
    assert doclens.sum() == n_tokens, f"doclens sum {doclens.sum()} != n_tokens {n_tokens}"
    assert doclens.min() > 0, "a document has zero tokens"
    print(f"  assert len(doclens)=={args.expect_docs}: ok; assert doclens.sum()==n_tokens: ok")
    print(f"  doclens min/mean/max = {doclens.min()}/{doclens.mean():.2f}/{doclens.max()}")
    t0 = time.time()
    docs = np.ascontiguousarray(docs_f16.astype(np.float32))
    print(f"  converted to float32 in {time.time() - t0:.1f}s ({docs.nbytes / 1e6:.0f} MB)")
    norms = np.linalg.norm(docs, axis=1)
    print(f"  doc token L2 norm min/mean/max = {norms.min():.6f}/{norms.mean():.6f}/{norms.max():.6f}")
    offsets = np.zeros(n_docs + 1, dtype=np.int64)
    offsets[1:] = np.cumsum(doclens)

    # ---- check 3: gt_top100.tsv ---------------------------------------------
    print(f"== check 3: {GT_FILE}")
    gt_raw = np.loadtxt(os.path.join(path, GT_FILE), delimiter="\t", dtype=np.float64)
    print(f"  rows={gt_raw.shape[0]} columns={gt_raw.shape[1]}")
    gt_q = gt_raw[:, 0].astype(np.int64)
    gt_doc = gt_raw[:, 1].astype(np.int64)
    gt_rank = gt_raw[:, 2].astype(np.int64)
    gt_score = gt_raw[:, 3].astype(np.float32)
    n_queries = queries_raw.shape[0]
    rows_per_query = np.bincount(gt_q, minlength=n_queries)
    print(f"  queries in file={len(np.unique(gt_q))} rows/query min/max={rows_per_query.min()}/{rows_per_query.max()}")
    print(f"  query id range [{gt_q.min()}, {gt_q.max()}] doc id range [{gt_doc.min()}, {gt_doc.max()}] (n_docs={n_docs})")
    print(f"  columns look like: query_idx, doc_idx, rank, score; integer 0-based ids (not doc_ids.npy strings)")
    # ranked? rank column is 1..100 per query and score is non-increasing
    ranked = True
    for qi in range(n_queries):
        m = gt_q == qi
        r = gt_rank[m]
        s = gt_score[m]
        if not (np.array_equal(r, np.arange(1, len(r) + 1)) and np.all(np.diff(s) <= 1e-6)):
            ranked = False
            break
    print(f"  rank column is 1..N per query and scores non-increasing: {ranked}")
    print(f"  score min/mean/max = {gt_score.min():.4f}/{gt_score.mean():.4f}/{gt_score.max():.4f}")
    depth = int(rows_per_query.max())
    gt_tsv = np.full((n_queries, depth), -1, dtype=np.int64)
    gt_tsv_score = np.zeros((n_queries, depth), dtype=np.float32)
    for qi in range(n_queries):
        m = gt_q == qi
        order = np.argsort(gt_rank[m], kind="stable")
        gt_tsv[qi, : m.sum()] = gt_doc[m][order]
        gt_tsv_score[qi, : m.sum()] = gt_score[m][order]

    # ---- check 4: query padding ---------------------------------------------
    print("== check 4: query padding")
    queries = np.ascontiguousarray(np.asarray(queries_raw, dtype=np.float32))
    nq, T, qd = queries.shape
    assert qd == D
    print(f"  queries shape [{nq}, {T}, {qd}]; query_lens min/mean/max = {query_lens.min()}/{query_lens.mean():.2f}/{query_lens.max()}")
    nonzero_padded = 0
    padded_rows = 0
    for qi in range(nq):
        pad = queries[qi, query_lens[qi] :]
        padded_rows += pad.shape[0]
        nonzero_padded += int(np.count_nonzero(np.abs(pad).sum(axis=1) > 0))
    print(f"  padded rows total={padded_rows}, of which nonzero={nonzero_padded}")
    qnorms = np.linalg.norm(queries.reshape(-1, D), axis=1)
    print(f"  query token L2 norm (all {nq * T} rows) min/mean/max = {qnorms.min():.6f}/{qnorms.mean():.6f}/{qnorms.max():.6f}")

    # ---- check 5: loader validation, 5 queries, both conventions ------------
    print("== check 5: exact MaxSim vs gt_top100 for 5 queries, both padding conventions")
    rng = np.random.default_rng(args.seed)
    extra = sorted(rng.choice(np.arange(3, nq), size=N_CHECK_QUERIES - 3, replace=False).tolist())
    check_qs = [0, 1, 2] + extra
    print(f"  queries checked: {check_qs} (seed={args.seed})")

    def run_convention(name, take_all):
        set_hits = []
        ranked_hits = []
        valid = []
        max_score_diff = 0.0
        for qi in check_qs:
            q = queries[qi] if take_all else queries[qi, : query_lens[qi]]
            scores = maxsim_all_docs(q, docs, offsets)
            mine = topk(scores, depth)
            theirs = gt_tsv[qi]
            set_hits.append(len(set(mine.tolist()) & set(theirs.tolist())))
            ranked_hits.append(int((mine == theirs).sum()))
            diff = float(np.abs(scores[theirs] - gt_tsv_score[qi]).max())
            max_score_diff = max(max_score_diff, diff)
            # The tsv is an exact MaxSim top-`depth` under my loader iff my
            # score of every listed doc equals the tsv score and no unlisted
            # doc scores strictly higher than the listed minimum. Set/ranked
            # equality is too strict: equal-score ties at the boundary or
            # inside the list are ordered arbitrarily by whoever wrote the tsv.
            outside = np.ones(len(scores), dtype=bool)
            outside[theirs] = False
            valid.append(diff < 1e-3 and scores[outside].max() <= scores[theirs].min() + 1e-5)
        print(f"  ({name}) set overlap /{depth}: {set_hits}; ranked position matches /{depth}: {ranked_hits}; "
              f"max |my score - tsv score| over their top-{depth}: {max_score_diff:.5f}; "
              f"tsv is a valid exact top-{depth} up to ties: {valid}")
        return all(valid)

    match_a = run_convention("a: padded rows dropped, query_lens tokens", False)
    match_b = run_convention("b: padded rows scored, all 32 tokens", True)
    print(f"  convention a matches: {match_a}; convention b matches: {match_b}")
    if padded_rows == 0 or nonzero_padded == 0:
        # query_lens equals the padded width for every query, so a and b score
        # the same tokens and cannot disagree; or the padding is all zeros,
        # which adds 0 to every MaxSim, so the two conventions agree as well. Convention a then: a zero query token
        # would still cost the index one centroid probe per query token.
        print(f"  conventions a and b are identical ({'no padded rows' if padded_rows == 0 else 'the padding is all zeros'})")
        if not match_a:
            sys.exit("check 5: gt_top100 does not match exact MaxSim; not writing the cache")
        take_all = False
    else:
        if match_a == match_b:
            sys.exit("check 5: gt_top100 matched under neither or both conventions; not writing the cache")
        take_all = match_b
    print(f"  -> using convention {'b (all 32 tokens scored)' if take_all else 'a (query_lens tokens only)'}")

    # ---- check 6: ground truth to write --------------------------------------
    print("== check 6: ground truth to write")
    # The full exact ranking is cheap, so the tsv is verified for all
    # queries, not only the five of check 5, before it is trusted as ground
    # truth.
    t0 = time.time()
    all_valid = 0
    all_diff = 0.0
    for qi in range(nq):
        q = queries[qi] if take_all else queries[qi, : query_lens[qi]]
        scores = maxsim_all_docs(q, docs, offsets)
        theirs = gt_tsv[qi]
        diff = float(np.abs(scores[theirs] - gt_tsv_score[qi]).max())
        all_diff = max(all_diff, diff)
        outside = np.ones(len(scores), dtype=bool)
        outside[theirs] = False
        if diff < 1e-3 and scores[outside].max() <= scores[theirs].min() + 1e-5:
            all_valid += 1
    print(f"  all {nq} queries: tsv top-{depth} is a valid exact top-{depth} up to ties for {all_valid}/{nq}; "
          f"max score diff {all_diff:.5f} ({time.time() - t0:.1f}s)")
    if ranked and all_valid == nq:
        gt = gt_tsv.astype(np.uint32)
        gt_depth = depth
        print(f"  gt_top100.tsv is an exact MaxSim ranking: writing it as ground truth, depth {gt_depth}")
    else:
        gt_depth = min(GT_DEPTH_RECOMPUTE, n_docs)
        gt = np.empty((nq, gt_depth), dtype=np.uint32)
        for qi in range(nq):
            q = queries[qi] if take_all else queries[qi, : query_lens[qi]]
            gt[qi] = topk(maxsim_all_docs(q, docs, offsets), gt_depth)
        print(f"  gt_top100.tsv is NOT an exact MaxSim ranking: recomputed exact MaxSim, depth {gt_depth}")

    # ---- check 7: write queries under the matched convention ------------------
    print("== check 7: write cache")
    if take_all:
        qcounts = np.full(nq, T, dtype=np.uint32)
        qflat = queries.reshape(-1, D)
    else:
        qcounts = query_lens.astype(np.uint32)
        qflat = np.concatenate([queries[qi, : query_lens[qi]] for qi in range(nq)], axis=0)
    qflat = np.ascontiguousarray(qflat, dtype=np.float32)
    mean = docs.mean(axis=0, dtype=np.float64).astype(np.float32)
    with open(args.o, "wb") as fh:
        fh.write(MAGIC)
        for v in (D, n_docs, nq, gt_depth, n_tokens, int(qcounts.sum())):
            fh.write(np.uint32(v).tobytes())
        fh.write(np.uint64(0).tobytes())  # seed: no subsample
        fh.write(np.uint32(n_docs).tobytes())  # corpus docs == kept docs
        fh.write(np.arange(n_docs, dtype=np.uint32).tobytes())
        fh.write(doclens.astype(np.uint32).tobytes())
        fh.write(qcounts.tobytes())
        fh.write(mean.tobytes())
        fh.write(gt.tobytes())
        fh.write(docs.tobytes())
        fh.write(qflat.tobytes())
        size = fh.tell()
    print(f"  wrote {args.o}: {n_docs} docs, {n_tokens} doc tokens, {nq} queries, {int(qcounts.sum())} query tokens, "
          f"gt depth {gt_depth}, |mean|_2={np.linalg.norm(mean):.5f}, {size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
