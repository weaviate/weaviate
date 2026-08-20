#!/usr/bin/env python3
"""Prefilter-ablation analysis (pre-registered tie rule).

Cells:
- BEIR (f32 / rq1): families neg10, neg40, pos40 pooled over their 22
  filters weighted by query count.
- wiki 10M: per global filter; anticorr pooled per tau.

Tie rule per cell: guard TIES the PR if QPS@recall0.90 within ±3% AND
recall@10 at matched ef within −0.005. PR wins materially if >5% QPS at
matched recall or >0.01 recall at matched ef.
"""
import csv
import glob
import sys
from collections import defaultdict
import statistics as st

BASE = "bench-results/pathseer-pr12137/data"
rows = []
for f in ("ablation-beir-f32.csv", "ablation-beir-rq1.csv", "ablation-wiki.csv"):
    try:
        rows += list(csv.DictReader(open(f"{BASE}/{f}")))
    except FileNotFoundError:
        print(f"[missing {f}]", file=sys.stderr)

# median across passes per (dataset, filter, arm, ef)
cell = defaultdict(list)
for r in rows:
    cell[(r["dataset"], r["filter"], r["family"], int(r["size"]), r["strategy"], int(r["ef"]))].append(r)
med = {}
for k, rs in cell.items():
    med[k] = {
        "recall": st.median(float(r["recall_at_10"]) for r in rs),
        "mean": st.median(float(r["mean_ms"]) for r in rs),
        "dist": st.median(float(r["mean_dist_comps"]) for r in rs),
        "regret": st.median(float(r["sim_regret"]) for r in rs),
        "q": int(rs[0]["queries"]),
    }

# build pooled cells: key -> arm -> ef -> {recall, mean, dist}
def cellkey(ds, flt, fam):
    if ds.startswith("beir"):
        return f"{ds}:{fam}"
    if fam == "anticorr":
        return f"{ds}:anticorr_tau{flt.split('tau')[-1]}"
    return f"{ds}:{flt}"

pool = defaultdict(lambda: defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0]))  # q, rec, ms, dist, regret
for (ds, flt, fam, size, arm, ef), v in med.items():
    p = pool[cellkey(ds, flt, fam)][(arm, ef)]
    p[0] += v["q"]
    p[1] += v["recall"] * v["q"]
    p[2] += v["mean"] * v["q"]
    p[3] += v["dist"] * v["q"]
    p[4] += v["regret"] * v["q"]

curves = defaultdict(lambda: defaultdict(list))  # cell -> arm -> [(recall, ms, ef, dist)]
for ck, arms in pool.items():
    for (arm, ef), p in arms.items():
        curves[ck][arm].append((p[1] / p[0], p[2] / p[0], ef, p[3] / p[0]))

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

TGT = 0.90
print(f"=== per-cell arm summary (QPS @ recall {TGT}, ef-interpolated) ===")
verdict_cells = []
for ck in sorted(curves):
    arms = curves[ck]
    line = [ck]
    qs = {}
    for arm in ("sweeping", "acorn", "pathseer", "ours-no-twohop", "ours-guard", "ours-puredos"):
        if arm in arms:
            qs[arm] = qps_at(arms[arm], TGT)
            line.append(f"{arm}={qs[arm]:.0f}" if qs[arm] else f"{arm}=--")
    print("  ".join(line))

    # tie rule: guard vs PR
    if "pathseer" in arms and "ours-guard" in arms and "subef" not in ck:
        pr, gd = qs.get("pathseer"), qs.get("ours-guard")
        # matched-ef recall deltas
        prc = {ef: r for r, m, ef, d in arms["pathseer"]}
        gdc = {ef: r for r, m, ef, d in arms["ours-guard"]}
        drec = [gdc[ef] - prc[ef] for ef in prc if ef in gdc]
        worst_drec = min(drec) if drec else None
        if pr and gd:
            dq = (gd / pr - 1) * 100
            tie = abs(dq) <= 3 and worst_drec is not None and worst_drec >= -0.005
            pr_wins = dq < -5 or (worst_drec is not None and worst_drec < -0.01)
            gd_wins = dq > 5 or (worst_drec is not None and min(-x for x in drec) < -0.01 if drec else False)
            status = "TIE" if tie else ("PR_WINS" if pr_wins else ("GUARD_AHEAD" if dq > 3 else "within-margins"))
        elif pr and not gd:
            dq, worst_drec, status = None, worst_drec, "PR_WINS (guard never reaches 0.90)"
        elif gd and not pr:
            dq, worst_drec, status = None, worst_drec, "GUARD_AHEAD (PR never reaches 0.90)"
        else:
            dq, status = None, "neither reaches 0.90 — judge by matched-ef recall"
        verdict_cells.append((ck, status, dq, worst_drec))
        print(f"    guard-vs-PR: dQPS={dq if dq is None else round(dq,1)}%  worst matched-ef drecall={worst_drec}  -> {status}")

print("\n=== PA checks ===")
# PA1: no-twohop vs PR
for ck in sorted(curves):
    arms = curves[ck]
    if "pathseer" in arms and "ours-no-twohop" in arms:
        pr, nt = qps_at(arms["pathseer"], TGT), qps_at(arms["ours-no-twohop"], TGT)
        if pr and nt:
            print(f"PA1 {ck}: no-twohop vs PR dQPS@0.90 = {(nt/pr-1)*100:+.1f}%")
        else:
            prc = {ef: r for r, m, ef, d in arms["pathseer"]}
            ntc = {ef: r for r, m, ef, d in arms["ours-no-twohop"]}
            dd = {ef: round(ntc[ef]-prc[ef], 4) for ef in prc if ef in ntc}
            print(f"PA1 {ck}: neither/one misses 0.90; matched-ef drecall {dd}")
# PA2: guard dist at random_10k vs PR
ck = "wikidpr10m-rq1:random_10k"
if ck in curves and "ours-guard" in curves[ck]:
    for r, m, ef, d in sorted(curves[ck]["pathseer"], key=lambda x: x[2]):
        for r2, m2, ef2, d2 in curves[ck]["ours-guard"]:
            if ef2 == ef:
                print(f"PA2 random_10k ef{ef}: PR dist={d:.0f} guard dist={d2:.0f} ratio={d2/d:.3f}")
# PA4: puredos
for ck in sorted(curves):
    if "ours-puredos" in curves[ck]:
        pts = sorted(curves[ck]["ours-puredos"], key=lambda x: x[2])
        print(f"PA4 {ck}: puredos " + "; ".join(f"ef{ef}: recall={r:.3f} ms={m:.2f}" for r, m, ef, d in pts))
# PA5: per-node constant in dispersed band (PR + guard at random_10k)
ck = "wikidpr10m-rq1:random_10k"
if ck in curves:
    for arm in ("pathseer", "ours-guard"):
        if arm in curves[ck]:
            for r, m, ef, d in sorted(curves[ck][arm], key=lambda x: x[2]):
                print(f"PA5 {ck} {arm} ef{ef}: dist={d:.0f} (ef/s={ef/0.001:.0f}) mean_ms={m:.2f} -> {m*1e6/d:.0f} ns/node")
