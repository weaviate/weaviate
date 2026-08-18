#!/usr/bin/env python3
"""Stage 1 decision analysis (pre-registered): per (sel, corr) cell, compute
QPS at interpolated recall 0.90 / 0.95 for each strategy and the PathSeer
gain over best(sweeping, acorn).

Aggregation: per family (pos10/pos40/neg10/neg40) and (strategy, ef), pool
all filters weighted by query count (recall = weighted mean; latency = mean
of per-filter mean_ms weighted by queries; QPS = 1000/mean_ms single-thread).
Passes are medianed first.
"""
import csv
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "bench-results/pathseer-pr12137/data/stage1-beir500k.csv"
rows = list(csv.DictReader(open(path)))

# median across passes per (filter, strategy, ef)
by_cell = defaultdict(list)
for r in rows:
    by_cell[(r["filter"], r["family"], r["strategy"], int(r["ef"]))].append(r)

def med(vals):
    s = sorted(vals)
    return s[len(s) // 2]

# pool filters per (family, strategy, ef)
pooled = defaultdict(lambda: {"q": 0, "hits": 0.0, "t": 0.0, "dist": 0.0, "strat_used": defaultdict(int)})
for (flt, fam, strat, ef), rs in by_cell.items():
    q = int(rs[0]["queries"])
    recall = med([float(r["recall_at_10"]) for r in rs])
    mean_ms = med([float(r["mean_ms"]) for r in rs])
    dist = med([float(r["mean_dist_comps"]) for r in rs])
    p = pooled[(fam, strat, ef)]
    p["q"] += q
    p["hits"] += recall * q
    p["t"] += mean_ms * q
    p["dist"] += dist * q
    for part in rs[0]["strategy_used"].split("|"):
        if ":" in part:
            k, v = part.split(":")
            p["strat_used"][k] += int(v)

print(f"{'family':<7} {'strategy':<10} {'ef':>4} {'recall':>7} {'mean_ms':>8} {'qps':>8} {'dist':>7}  strategy_used")
curves = defaultdict(list)  # (family, strategy) -> [(recall, qps, ef)]
for (fam, strat, ef) in sorted(pooled, key=lambda x: (x[0], x[1], x[2])):
    p = pooled[(fam, strat, ef)]
    recall = p["hits"] / p["q"]
    mean_ms = p["t"] / p["q"]
    qps = 1000.0 / mean_ms
    used = ",".join(f"{k}:{v}" for k, v in sorted(p["strat_used"].items()))
    print(f"{fam:<7} {strat:<10} {ef:>4} {recall:>7.4f} {mean_ms:>8.3f} {qps:>8.1f} {p['dist']/p['q']:>7.0f}  {used}")
    curves[(fam, strat)].append((recall, qps, ef))

def qps_at(curve, target):
    """Interpolate QPS at target recall along the ef curve (recall
    monotone-ish in ef). Returns None if never reaches target."""
    pts = sorted(curve)
    if pts[-1][0] < target:
        return None
    if pts[0][0] >= target:
        return pts[0][1]
    for (r0, q0, _), (r1, q1, _) in zip(pts, pts[1:]):
        if r0 < target <= r1:
            w = (target - r0) / (r1 - r0)
            return q0 + w * (q1 - q0)
    return None

print("\n=== pre-registered decision: QPS at matched recall ===")
for fam in ["pos10", "pos40", "neg10", "neg40"]:
    line = [fam]
    for target in (0.90, 0.95):
        qs = {s: qps_at(curves[(fam, s)], target) for s in ("sweeping", "acorn", "pathseer")}
        base = max([v for k, v in qs.items() if k != "pathseer" and v is not None], default=None)
        ps = qs.get("pathseer")
        if base and ps:
            gain = (ps / base - 1) * 100
            line.append(f"@{target}: ps={ps:.0f} best-other={base:.0f} gain={gain:+.1f}%")
        else:
            line.append(f"@{target}: ps={ps} others={ {k: v for k, v in qs.items() if k != 'pathseer'} }")
    print("  ".join(str(x) for x in line))
