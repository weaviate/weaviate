#!/usr/bin/env python3
"""D2-PrePR P1 mixed-version BM25 driver.

Drives ingest + cross-node BM25 queries against the d2prep1 cluster over REST,
so the same probe runs regardless of which binary a node is on. Emits BM25
scores per node so a mis-decode (wrong scores) on a base-main node reading a V2
segment is directly observable.

Usage:
  driver.py create  --node URL                 create the DocV2 BM25 class
  driver.py ingest  --node URL [--n N]         ingest corpus (incl >65535 doc)
  driver.py wait    --node URL [URL ...]       wait until overflow doc queryable
  driver.py query   --node URL --term T        BM25 query, print scores JSON
  driver.py probe   --node URL [--label L]     full per-node BM25 probe (JSON)
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request

OVERFLOW_TOKEN = "overflowtoken"
OVERFLOW_MARK = "zephyrmark"
OVERFLOW_REPEAT = 70000  # > math.MaxUint16 (65535): the uint16-clamp trap


def corpus():
    overflow = (OVERFLOW_TOKEN + " ") * OVERFLOW_REPEAT + OVERFLOW_MARK
    return [
        "the quick brown fox jumps over the lazy dog",
        "a quick movement of the enemy will jeopardize six gunboats",
        "random unrelated content about databases and search engines",
        "bm25 ranking scores documents by term frequency and length",
        overflow,
    ]


def _req(method, url, body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}
    except Exception as e:
        return 0, {"error": str(e)}


def create_class(node):
    cls = {
        "class": "DocV2",
        "vectorizer": "none",
        "invertedIndexConfig": {
            "bm25": {"k1": 1.2, "b": 0.75},
            "usingBlockMaxWAND": True,
            "cleanupIntervalSeconds": 60,
        },
        "replicationConfig": {"factor": 3},
        "properties": [
            {"name": "content", "dataType": ["text"], "indexSearchable": True}
        ],
    }
    st, resp = _req("POST", f"{node}/v1/schema", cls)
    print(json.dumps({"create": st, "resp": resp}))
    return 0 if st in (200, 422) else 1


def ingest(node, n):
    docs = corpus()
    if n:
        docs = docs[:n]
    okc = 0
    for c in docs:
        st, resp = _req("POST", f"{node}/v1/objects",
                        {"class": "DocV2", "properties": {"content": c}},
                        timeout=60)
        tag = c[:40] if len(c) > 40 else c
        if st == 200:
            okc += 1
        else:
            print(json.dumps({"ingest_fail": st, "doc": tag, "resp": resp}))
    print(json.dumps({"ingested": okc, "total": len(docs)}))
    return 0 if okc == len(docs) else 1


def _bm25(node, term, props=("content",), extra=""):
    sel = "content _additional{score}"
    q = '{Get{DocV2(bm25:{query:%s}%s){%s}}}' % (json.dumps(term), extra, sel)
    st, resp = _req("POST", f"{node}/v1/graphql", {"query": q})
    rows = []
    if st == 200 and resp.get("data"):
        rows = (resp["data"].get("Get") or {}).get("DocV2") or []
    out = []
    for r in rows:
        score = None
        add = r.get("_additional") or {}
        s = add.get("score")
        if s is not None:
            try:
                score = float(s)
            except Exception:
                score = s
        out.append({"content": (r.get("content") or "")[:30], "score": score,
                    "len": len(r.get("content") or "")})
    return st, resp.get("errors"), out


def query(node, term):
    st, errs, rows = _bm25(node, term)
    print(json.dumps({"status": st, "errors": errs, "rows": rows}, indent=2))
    return 0 if st == 200 and not errs else 1


def wait_ready(nodes, deadline_s=90):
    q = '{Get{DocV2(bm25:{query:%s}){content}}}' % json.dumps(OVERFLOW_MARK)
    for node in nodes:
        ok = False
        end = time.time() + deadline_s
        last = None
        while time.time() < end:
            st, resp = _req("POST", f"{node}/v1/graphql", {"query": q})
            last = (st, resp.get("errors") if isinstance(resp, dict) else None)
            if st == 200 and resp.get("data"):
                docs = (resp["data"].get("Get") or {}).get("DocV2") or []
                if len(docs) == 1:
                    ok = True
                    break
            time.sleep(1)
        print(json.dumps({"node": node, "ready": ok, "last": last}))
        if not ok:
            return 1
    return 0


def probe(node, label):
    """Full per-node BM25 probe. The scores here are the migration-safety
    evidence: a base-main node mis-decoding a V2 length section produces
    different scores from a node that reads V2 correctly."""
    result = {"label": label, "node": node}
    # marker query: only the overflow doc carries it
    st_m, err_m, rows_m = _bm25(node, OVERFLOW_MARK)
    result["marker"] = {"status": st_m, "errors": err_m, "rows": rows_m}
    # overflow token (70000x): scores the long doc with its TRUE length
    st_o, err_o, rows_o = _bm25(node, OVERFLOW_TOKEN)
    result["overflow_token"] = {"status": st_o, "errors": err_o, "rows": rows_o}
    # ordinary BM25: both "quick" docs
    st_q, err_q, rows_q = _bm25(node, "quick")
    result["quick"] = {"status": st_q, "errors": err_q,
                       "n": len(rows_q), "rows": rows_q}
    print(json.dumps(result, indent=2))
    return 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("create", "ingest", "query", "probe"):
        p = sub.add_parser(name)
        p.add_argument("--node", required=True)
        if name == "ingest":
            p.add_argument("--n", type=int, default=0)
        if name == "query":
            p.add_argument("--term", required=True)
        if name == "probe":
            p.add_argument("--label", default="node")
    pw = sub.add_parser("wait")
    pw.add_argument("--node", required=True, nargs="+")
    args = ap.parse_args()

    if args.cmd == "create":
        return create_class(args.node)
    if args.cmd == "ingest":
        return ingest(args.node, args.n)
    if args.cmd == "wait":
        return wait_ready(args.node)
    if args.cmd == "query":
        return query(args.node, args.term)
    if args.cmd == "probe":
        return probe(args.node, args.label)
    return 2


if __name__ == "__main__":
    sys.exit(main())
