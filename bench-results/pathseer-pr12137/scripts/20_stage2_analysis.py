#!/usr/bin/env python3
"""Stage 2 analysis: per-filter recall/latency curves, H6 matched-recall
comparison (pathseer vs acorn vs sweeping), H2 table (dist comps vs
cardinality), anticorr summary (tau=0 by regret)."""
import csv
import sys
from collections import defaultdict
import statistics as st

path = "bench-results/pathseer-pr12137/data/stage2-wikidpr10m.csv"
rows = list(csv.DictReader(open(path)))

# median across passes
cell = defaultdict(list)
for r in rows:
    cell[(r["filter"], r["family"], int(r["size"]), r["strategy"], int(r["ef"]))].append(r)
med = {}
for k, rs in cell.items():
    med[k] = {
        "recall": st.median(float(r["recall_at_10"]) for r in rs),
        "p50": st.median(float(r["p50_ms"]) for r in rs),
        "mean": st.median(float(r["mean_ms"]) for r in rs),
        "dist": st.median(float(r["mean_dist_comps"]) for r in rs),
        "regret": st.median(float(r["sim_regret"]) for r in rs),
    }

# curves per (filter, strategy): [(recall, mean_ms, ef)]
curves = defaultdict(list)
meta = {}
for (f, fam, size, s, ef), v in med.items():
    if fam in ("topical", "random", "conjunction"):
        curves[(f, s)].append((v["recall"], v["mean"], ef))
        meta[f] = (fam, size)

def ms_at(curve, target):
    pts = sorted(curve)
    if not pts or pts[-1][0] < target:
        return None
    if pts[0][0] >= target:
        return pts[0][1]
    for (r0, m0, _), (r1, m1, _) in zip(pts, pts[1:]):
        if r0 < target <= r1:
            w = (target - r0) / (r1 - r0)
            return m0 + w * (m1 - m0)
    return None

TGT = 0.90
print(f"=== H6: mean ms to reach recall@10 = {TGT} (None = never reaches) ===")
print(f"{'filter':<26}{'size':>9} {'sweeping':>9} {'acorn':>9} {'pathseer':>9}  best")
for f in sorted(meta, key=lambda f: meta[f][1]):
    fam, size = meta[f]
    vals = {}
    for s in ("sweeping", "acorn", "pathseer"):
        vals[s] = ms_at(curves[(f, s)], TGT)
    def fmt(v):
        return f"{v:9.2f}" if v is not None else "     --  "
    ok = {s: v for s, v in vals.items() if v is not None}
    best = min(ok, key=ok.get) if ok else "-"
    print(f"{f:<26}{size:>9} {fmt(vals['sweeping'])} {fmt(vals['acorn'])} {fmt(vals['pathseer'])}  {best}")

print("\n=== H2: dist comps at ef128, by size ===")
print(f"{'filter':<26}{'size':>9} {'sweep':>10} {'acorn':>10} {'pathseer':>10} {'ps/sweep':>9} {'ps/acorn':>9}")
for f in sorted(meta, key=lambda f: meta[f][1]):
    fam, size = meta[f]
    d = {}
    for s in ("sweeping", "acorn", "pathseer"):
        k = (f, fam, size, s, 128)
        d[s] = med[k]["dist"] if k in med else None
    if all(v is not None for v in d.values()):
        print(f"{f:<26}{size:>9} {d['sweeping']:>10.0f} {d['acorn']:>10.0f} {d['pathseer']:>10.0f} "
              f"{d['pathseer']/d['sweeping']:>9.3f} {d['pathseer']/d['acorn']:>9.1f}")

print("\n=== anticorr (9.9M members): pooled by tau, ef128 ===")
pool = defaultdict(lambda: defaultdict(list))
for (f, fam, size, s, ef), v in med.items():
    if fam == "anticorr" and ef == 128:
        tau = f.split("tau")[-1]
        pool[(tau, s)]["recall"].append(v["recall"])
        pool[(tau, s)]["mean"].append(v["mean"])
        pool[(tau, s)]["regret"].append(v["regret"])
        pool[(tau, s)]["dist"].append(v["dist"])
for k in sorted(pool):
    p = pool[k]
    print(f"tau={k[0]:<5} {k[1]:<9} recall={st.mean(p['recall']):.4f} regret={st.mean(p['regret']):.5f} "
          f"mean_ms={st.mean(p['mean']):.2f} dist={st.mean(p['dist']):.0f} (n={len(p['recall'])})")
