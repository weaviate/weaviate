#!/usr/bin/env python3
"""Sparse-NEG round analysis (PREREGISTERED-ACORN.md).

Pools the 50 per-query filters per cardinality point. Decision band =
{300k, 1M, 2M, 4M} (members > ~306k scan threshold, ratio <= 0.4 gate).
Materiality: >5% QPS at matched recall 0.90, or reaching a recall the other
cannot within the ef grid.
"""
import csv
import sys
from collections import defaultdict
import statistics as st

BASE = "bench-results/pathseer-pr12137/data"
rows = list(csv.DictReader(open(f"{BASE}/neg-sparse-graph.csv")))

def point(name):
    return name.split("_m")[-1]

cell = defaultdict(list)
for r in rows:
    cell[(point(r["filter"]), r["strategy"], int(r["ef"]), r["filter"])].append(r)
pool = defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0])  # q, rec, ms, dist, popped
for (pt, arm, ef, flt), rs in cell.items():
    rec = st.median(float(r["recall_at_10"]) for r in rs)
    ms = st.median(float(r["mean_ms"]) for r in rs)
    dist = st.median(float(r["mean_dist_comps"]) for r in rs)
    pop = st.median(float(r["mean_nodes_popped"]) for r in rs)
    p = pool[(pt, arm, ef)]
    p[0] += 1
    p[1] += rec
    p[2] += ms
    p[3] += dist
    p[4] += pop

curves = defaultdict(lambda: defaultdict(list))
for (pt, arm, ef), p in pool.items():
    curves[pt][arm].append((p[1]/p[0], p[2]/p[0], ef, p[3]/p[0], p[4]/p[0]))

def qps_at(pts, target):
    pts = sorted(pts)
    if not pts or pts[-1][0] < target:
        return None
    if pts[0][0] >= target:
        return 1000.0 / pts[0][1]
    for a, b in zip(pts, pts[1:]):
        if a[0] < target <= b[0]:
            w = (target - a[0]) / (b[0] - a[0])
            return 1000.0 / (a[1] + w * (b[1] - a[1]))
    return None

ORDER = ["50k", "150k", "300k", "1m", "2m", "4m"]
BAND = {"300k", "1m", "2m", "4m"}
print("=== per-point curves (pooled over 50 filters) ===")
for pt in ORDER:
    if pt not in curves:
        continue
    for arm in ("sweeping", "acorn", "pathseer"):
        for rec, ms, ef, dist, pop in sorted(curves[pt].get(arm, []), key=lambda x: x[2]):
            print(f"{pt:>5} {arm:<9} ef{ef:<4} recall={rec:.4f} mean_ms={ms:8.2f} dist={dist:9.0f} popped={pop:8.0f} ns/pop={ms*1e6/pop if pop else 0:6.0f}")
    print()

print("=== decision: acorn vs pathseer, QPS @ 0.90 ===")
for pt in ORDER:
    if pt not in curves:
        continue
    a, p = qps_at(curves[pt].get("acorn", []), 0.90), qps_at(curves[pt].get("pathseer", []), 0.90)
    amax = max((r for r, *_ in curves[pt].get("acorn", [])), default=0)
    pmax = max((r for r, *_ in curves[pt].get("pathseer", [])), default=0)
    band = "IN-BAND" if pt in BAND else "outside"
    if a and p:
        d = (a/p - 1) * 100
        verdict = "ACORN_MATERIAL" if d > 5 else ("PATHSEER_MATERIAL" if d < -5 else "tie")
        print(f"{pt:>5} [{band}] acorn={a:.1f} pathseer={p:.1f} qps  acorn-vs-ps={d:+.1f}%  -> {verdict}")
    else:
        who = []
        if p and not a:
            who.append(f"acorn never reaches 0.90 (max {amax:.3f}) -> PATHSEER_MATERIAL")
        if a and not p:
            who.append(f"pathseer never reaches 0.90 (max {pmax:.3f}) -> ACORN_MATERIAL")
        if not a and not p:
            who.append(f"neither reaches 0.90 (acorn max {amax:.3f}, pathseer max {pmax:.3f})")
        print(f"{pt:>5} [{band}] " + "; ".join(who))

# scan rows (PN4)
try:
    srows = list(csv.DictReader(open(f"{BASE}/neg-sparse-scan.csv")))
    sp = defaultdict(list)
    for r in srows:
        sp[(point(r["filter"]), r["b1"])].append((float(r["recall_at_10"]), float(r["p50_ms"])))
    print("\n=== PN4: scan under neg-sparse ===")
    for k in sorted(sp, key=lambda k: (ORDER.index(k[0]) if k[0] in ORDER else 99, k[1])):
        v = sp[k]
        print(f"{k[0]:>5} B1={k[1]:>6} recall={st.mean(x[0] for x in v):.4f} (min {min(x[0] for x in v):.3f}) p50={st.median(x[1] for x in v):.2f}ms (n={len(v)})")
except FileNotFoundError:
    print("\n[scan CSV not present yet]")
