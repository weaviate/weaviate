#!/usr/bin/env python3
"""Ablation deliverable plot: recall-vs-latency frontier per cell, arms overlaid.
Series identity = fixed palette slot + distinct marker (secondary encoding)."""
import csv
import os
from collections import defaultdict
import statistics as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = "bench-results/pathseer-pr12137"
ARMS = ["sweeping", "acorn", "pathseer", "ours-no-twohop", "ours-guard", "ours-puredos"]
COL = {"sweeping": "#2a78d6", "acorn": "#008300", "pathseer": "#e87ba4",
       "ours-no-twohop": "#eda100", "ours-guard": "#1baf7a", "ours-puredos": "#eb6834"}
MRK = {"sweeping": "o", "acorn": "s", "pathseer": "^", "ours-no-twohop": "D",
       "ours-guard": "v", "ours-puredos": "X"}
GRID = dict(color="#e6e6e3", linewidth=0.8)
plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white",
    "axes.edgecolor": "#c3c2b7", "axes.labelcolor": "#333330",
    "text.color": "#333330", "xtick.color": "#666660", "ytick.color": "#666660",
    "font.size": 8.5, "axes.titlesize": 9.5,
})

rows = []
for f in ("ablation-beir-f32.csv", "ablation-beir-rq1.csv", "ablation-wiki.csv"):
    rows += list(csv.DictReader(open(f"{BASE}/data/{f}")))

def cellkey(r):
    if r["dataset"].startswith("beir"):
        return f"{r['dataset']}: {r['family']}"
    if r["family"] == "anticorr":
        return f"wiki10M: anticorr τ={r['filter'].split('tau')[-1]}"
    return f"wiki10M: {r['filter']}"

pool = defaultdict(lambda: defaultdict(lambda: [0, 0.0, 0.0]))
cellmed = defaultdict(list)
for r in rows:
    if r["filter"] == "subef_32":
        continue
    cellmed[(cellkey(r), r["strategy"], int(r["ef"]), r["filter"])].append(r)
for (ck, arm, ef, flt), rs in cellmed.items():
    rec = st.median(float(r["recall_at_10"]) for r in rs)
    ms = st.median(float(r["mean_ms"]) for r in rs)
    q = int(rs[0]["queries"])
    p = pool[ck][(arm, ef)]
    p[0] += q
    p[1] += rec * q
    p[2] += ms * q

cells = sorted(pool)
ncols = 4
nrows = (len(cells) + ncols - 1) // ncols
fig, axes = plt.subplots(nrows, ncols, figsize=(3.1 * ncols, 2.9 * nrows))
for ax in axes.flat[len(cells):]:
    ax.axis("off")
for ax, ck in zip(axes.flat, cells):
    for arm in ARMS:
        pts = sorted((p[1] / p[0], p[2] / p[0]) for (a, ef), p in pool[ck].items() if a == arm)
        if not pts:
            continue
        ax.plot([x for x, y in pts], [y for x, y in pts], "-", marker=MRK[arm],
                color=COL[arm], linewidth=1.8, markersize=4.5)
    ax.set_yscale("log")
    ax.set_title(ck)
    ax.grid(True, **GRID)
    ax.set_axisbelow(True)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
handles = [plt.Line2D([], [], color=COL[a], marker=MRK[a], linestyle="-", label=a) for a in ARMS]
fig.legend(handles=handles, ncol=6, loc="lower center", frameon=False, fontsize=9)
fig.suptitle("Prefilter ablation: recall@10 (x) vs mean ms (y, log) per cell — ef ∈ {64,128,256,512}", fontsize=11)
fig.tight_layout(rect=[0, 0.05, 1, 0.965])
fig.savefig(f"{BASE}/plots/fig4-ablation-frontiers.png", dpi=150)
print("fig4 saved:", len(cells), "cells")
