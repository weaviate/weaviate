#!/usr/bin/env python3
"""Pre-registered plots for the PathSeer PR 12137 evaluation.
1. recall-vs-latency frontier per cardinality band (stage 2)
2. distance computations vs filter cardinality, three strategies (H2)
3. two-hop window length (pop index at candidate-heap-full) ECDF per ef (H1)
"""
import csv
import os
from collections import defaultdict
import statistics as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = "bench-results/pathseer-pr12137"
OUT = os.path.join(BASE, "plots")
os.makedirs(OUT, exist_ok=True)

COL = {"sweeping": "#2a78d6", "acorn": "#008300", "pathseer": "#e87ba4"}
GRID = dict(color="#e6e6e3", linewidth=0.8)
plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white",
    "axes.edgecolor": "#c3c2b7", "axes.labelcolor": "#333330",
    "text.color": "#333330", "xtick.color": "#666660", "ytick.color": "#666660",
    "font.size": 9, "axes.titlesize": 10,
})

rows = list(csv.DictReader(open(f"{BASE}/data/stage2-wikidpr10m.csv")))
cell = defaultdict(list)
for r in rows:
    cell[(r["filter"], r["family"], int(r["size"]), r["strategy"], int(r["ef"]))].append(r)
med = {k: {
    "recall": st.median(float(r["recall_at_10"]) for r in rs),
    "mean": st.median(float(r["mean_ms"]) for r in rs),
    "dist": st.median(float(r["mean_dist_comps"]) for r in rs),
} for k, rs in cell.items()}

# ---- fig 1: frontier small multiples -------------------------------------
BANDS = [("random_10k", "10k (0.1%)"), ("topical_40k_a", "40k (0.4%)"),
         ("conj_topical100k_a_idlo", "55k (0.5%)"), ("conj_topical1m_a_idmid", "238k (2.4%)"),
         ("conj_topical1m_a_idlo", "531k (5.3%)"), ("topical_2m_b", "2M (20%)")]
fig, axes = plt.subplots(2, 3, figsize=(10.5, 6.2), sharey=False)
for ax, (flt, label) in zip(axes.flat, BANDS):
    for s in ("sweeping", "acorn", "pathseer"):
        pts = sorted((med[k]["recall"], med[k]["mean"]) for k in med if k[0] == flt and k[3] == s)
        if not pts:
            continue
        ax.plot([p[0] for p in pts], [p[1] for p in pts], "-o", color=COL[s],
                linewidth=2, markersize=5, label=s)
    ax.set_yscale("log")
    ax.set_title(f"{label} — {flt}", fontsize=9)
    ax.grid(True, **GRID)
    ax.set_axisbelow(True)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
axes[0, 0].legend(frameon=False, fontsize=8, loc="upper left")
for ax in axes[-1]:
    ax.set_xlabel("recall@10")
for ax in axes[:, 0]:
    ax.set_ylabel("mean latency ms (log)")
fig.suptitle("wiki-dpr 10M (rq1): recall vs latency, ef ∈ {64,128,256,512} — up-and-left is better",
             fontsize=11)
fig.tight_layout(rect=[0, 0, 1, 0.96])
fig.savefig(f"{OUT}/fig1-frontier-by-cardinality.png", dpi=150)
print("fig1 saved")

# ---- fig 2: H2 dist comps vs cardinality ---------------------------------
fig, ax = plt.subplots(figsize=(7.5, 5))
for s in ("sweeping", "acorn", "pathseer"):
    by_size = defaultdict(list)
    for k in med:
        if k[3] == s and k[4] == 128 and k[1] in ("topical", "random", "conjunction"):
            by_size[k[2]].append(med[k]["dist"])
    xs = sorted(by_size)
    ys = [st.median(by_size[x]) for x in xs]
    ax.plot(xs, ys, "-o", color=COL[s], linewidth=2, markersize=5)
    # label at the left end, where the three series are far apart
    ax.annotate(s, (xs[0], ys[0]), xytext=(-6, 0), textcoords="offset points",
                color=COL[s], fontsize=9, va="center", ha="right")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("filter cardinality (members, log)")
ax.set_ylabel("distance computations per query (log)")
ax.set_title("wiki-dpr 10M, ef=128: distance computations vs filter cardinality (H2)")
ax.grid(True, **GRID)
ax.set_axisbelow(True)
for sp in ("top", "right"):
    ax.spines[sp].set_visible(False)
ax.set_xlim(right=3e7)
fig.tight_layout()
fig.savefig(f"{OUT}/fig2-distcomps-vs-cardinality.png", dpi=150)
print("fig2 saved")

# ---- fig 3: H1 window-length ECDF per ef ---------------------------------
q = [r for r in csv.DictReader(open(f"{BASE}/data/stage1-beir500k-perquery.csv"))
     if r["strategy"] == "pathseer" and r["pass"] == "1"]
RAMP = {64: "#86b6ef", 128: "#5598e7", 256: "#2a78d6", 512: "#184f95"}
fig, ax = plt.subplots(figsize=(7.5, 5))
for ef in (64, 128, 256, 512):
    vals = sorted(int(r["pop_idx_cand_full"]) for r in q if int(r["ef"]) == ef and int(r["pop_idx_cand_full"]) >= 0)
    ys = [i / len(vals) for i in range(1, len(vals) + 1)]
    ax.plot(vals, ys, color=RAMP[ef], linewidth=2)
    idx = int(0.98 * len(vals))
    ax.annotate(f"ef={ef}", (vals[idx], ys[idx]), xytext=(6, -2), textcoords="offset points",
                color=RAMP[ef], fontsize=9)
ax.set_xscale("log")
ax.set_xlabel("pop index at which the candidate heap first reached ef (log)")
ax.set_ylabel("fraction of queries (ECDF)")
ax.set_title("BEIR 500k: the PathSeer two-hop window closes after very few pops (H1)")
ax.grid(True, **GRID)
ax.set_axisbelow(True)
for sp in ("top", "right"):
    ax.spines[sp].set_visible(False)
fig.tight_layout()
fig.savefig(f"{OUT}/fig3-twohop-window-ecdf.png", dpi=150)
print("fig3 saved")
