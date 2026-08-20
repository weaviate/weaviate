#!/usr/bin/env python3
"""neg-sparse deliverable plot: per cardinality point, two quality views —
top row: recall@10 frontier (pre-registered metric, degenerate by
construction on this family); bottom row: similarity-regret frontier (the
quality metric that actually discriminates, per the tau=0 precedent).
Scan points overlaid where measured."""
import csv
import os
from collections import defaultdict
import statistics as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = "bench-results/pathseer-pr12137"
COL = {"sweeping": "#2a78d6", "acorn": "#008300", "pathseer": "#e87ba4", "scan": "#eda100"}
MRK = {"sweeping": "o", "acorn": "s", "pathseer": "^", "scan": "D"}
GRID = dict(color="#e6e6e3", linewidth=0.8)
plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white",
    "axes.edgecolor": "#c3c2b7", "axes.labelcolor": "#333330",
    "text.color": "#333330", "xtick.color": "#666660", "ytick.color": "#666660",
    "font.size": 8.5, "axes.titlesize": 9.5,
})

rows = list(csv.DictReader(open(f"{BASE}/data/neg-sparse-graph.csv")))
pool = defaultdict(lambda: [0, 0.0, 0.0, 0.0])
for r in rows:
    pt = r["filter"].split("_m")[-1]
    p = pool[(pt, r["strategy"], int(r["ef"]))]
    p[0] += 1
    p[1] += float(r["recall_at_10"])
    p[2] += float(r["mean_ms"])
    p[3] += float(r["sim_regret"])

scan = defaultdict(lambda: [0, 0.0, 0.0, 0.0])
try:
    for r in csv.DictReader(open(f"{BASE}/data/neg-sparse-scan.csv")):
        pt = r["filter"].split("_m")[-1]
        s = scan[(pt, int(r["b1"]))]
        s[0] += 1
        s[1] += float(r["recall_at_10"])
        s[2] += float(r["p50_ms"])
        s[3] += float(r["sim_regret"])
except FileNotFoundError:
    pass

ORDER = ["50k", "150k", "300k", "1m", "2m", "4m"]
LAB = {"50k": "50k (0.5%)", "150k": "150k (1.5%)", "300k": "300k (3%) — band",
       "1m": "1M (10%) — band", "2m": "2M (20%) — band", "4m": "4M (40%) — band"}
fig, axes = plt.subplots(2, 6, figsize=(3.0 * 6, 6.0))
for col, pt in enumerate(ORDER):
    for arm in ("sweeping", "acorn", "pathseer"):
        pts = sorted((p[1]/p[0], p[2]/p[0], p[3]/p[0]) for (q, a, ef), p in pool.items() if q == pt and a == arm)
        if not pts:
            continue
        axes[0, col].plot([x[0] for x in pts], [x[1] for x in pts], "-", marker=MRK[arm],
                          color=COL[arm], linewidth=1.8, markersize=4.5)
        rp = sorted((x[2], x[1]) for x in pts)
        axes[1, col].plot([x[0] for x in rp], [x[1] for x in rp], "-", marker=MRK[arm],
                          color=COL[arm], linewidth=1.8, markersize=4.5)
    for (q, b1), s in scan.items():
        if q != pt:
            continue
        axes[0, col].plot(s[1]/s[0], s[2]/s[0], MRK["scan"], color=COL["scan"], markersize=6)
        axes[1, col].plot(s[3]/s[0], s[2]/s[0], MRK["scan"], color=COL["scan"], markersize=6)
    axes[0, col].set_title(LAB[pt])
    for row in (0, 1):
        axes[row, col].set_yscale("log")
        axes[row, col].grid(True, **GRID)
        axes[row, col].set_axisbelow(True)
        for sp in ("top", "right"):
            axes[row, col].spines[sp].set_visible(False)
    axes[1, col].invert_xaxis()  # lower regret (better) to the right, like recall
axes[0, 0].set_ylabel("mean ms (log) — vs recall@10")
axes[1, 0].set_ylabel("mean ms (log) — vs sim regret\n(axis inverted: better →)")
handles = [plt.Line2D([], [], color=COL[a], marker=MRK[a], linestyle="-" if a != "scan" else "",
                      label=a) for a in ("sweeping", "acorn", "pathseer", "scan")]
fig.legend(handles=handles, ncol=4, loc="lower center", frameon=False, fontsize=9)
fig.suptitle("neg-sparse 10M (rq1): top = pre-registered recall frontier (degenerate tie band); bottom = regret frontier", fontsize=11)
fig.tight_layout(rect=[0, 0.05, 1, 0.95])
fig.savefig(f"{BASE}/plots/fig5-negsparse-frontiers.png", dpi=150)
print("fig5 saved")
