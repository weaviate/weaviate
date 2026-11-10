#!/usr/bin/env python
"""Head-to-head TachiomFresh vs HFresh + MUVERA on one dataset.

Reads the outputs of TestNFCorpusFrontier (tachiomfresh) and TestNFCorpusMuvera
(hfresh), computes each index's Pareto frontier of Accuracy@10 vs single-thread
QPS, and writes three figures (PNG) plus markdown tables for the report.
A fourth figure, TachiomFresh's RAM breakdown, is drawn from the "memory"
lines of TestNFCorpusMemoryBreakdown or TestNFCorpusFrontier.

    python3 bench/plot_compare.py [nfcorpus|scidocs|lotte] [--results results/x86]

Style: the project's shared plot style. Its STYLE dict, rcParams and the
line/marker and bar conventions are copied below. It renders text with LaTeX
(text.usetex); when no `latex` binary is on PATH this script falls back to
matplotlib's bundled Computer Modern fonts, the closest look without TeX.
"""

import argparse
import os
import re
import shutil
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib as mpl  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import seaborn as sns  # noqa: E402
from matplotlib.transforms import Bbox  # noqa: E402
from matplotlib.ticker import MultipleLocator  # noqa: E402

# the shared plot style
STYLE = {
    # the colours from the fourth on are not in the shared style: one per
    # series (see SLOT), so that no index changes colour between figures
    'palette': ["#EC8F8D", "#44A194", "#537D96", "#C9892B", "#8A6FB3", "#7E9B3A", "#2B3A67",
                # eighth to tenth: v3.4, v3.5, v3.6
                "#A63A50", "#59A14F", "#F1C232",
                # eleventh: step B
                "#1F77B4",
                # twelfth and thirteenth: v4.0, v4.3
                "#D62728", "#17BECF",
                # fourteenth: HFresh + MUVERA v1.1
                "#2CA02C",
                # fifteenth and sixteenth: v4.4, v4.5
                "#E377C2", "#7F7F7F",
                # seventeenth: HFresh + MUVERA v1.2
                "#98DF8A",
                # eighteenth and nineteenth: 4.4+centering, v4.6
                "#BCBD22", "#9467BD",
                # twentieth: v5.0
                "#8C564B",
                # twenty-first: v5.1
                "#E6550D",
                # twenty-second: v6.0
                "#3182BD",
                # twenty-third: v7.0
                "#000000"],
    'markers': ['o', 's', '^', 'D', 'P', 'X', 'v', '<', '>', '*', 'h', 'p', 'H', 'd', '8', 'X', 'v', 'D', 'P', 'd', '*', 'h', 'P'],
    'hatches': ['', '//', 'xx', '..', 'oo', '\\', '--', '++', '||', '**', 'OO', '\\\\', '//', 'xx', '++', '..', '\\', '//', 'xx', '..', 'oo', '--', '++'],
    'figure_size': (7.5, 4.5),
    'figure_size_wide': (11.5, 4.5),
    'dpi': 150,
    'font_family': 'serif',
    'title_size': 18,
    'label_size': 18,
    'tick_size': 16,
    'legend_size': 16,
    'line_width': 2.0,
    'marker_size': 8,
    'grid_alpha': 0.18,
    'save_format': 'pdf',
    'save_dpi': 300,
    'output_dir': Path('figures'),
    'bg_color': '#F4F0E4',
    'edge_color': '#333333',
}
USETEX = shutil.which("latex") is not None

# the shared plot style
sns.set_theme(style='whitegrid', context='notebook', palette=STYLE['palette'])
mpl.rcParams.update({
    'figure.figsize': STYLE['figure_size'],
    'figure.dpi': STYLE['dpi'],
    'savefig.dpi': STYLE['save_dpi'],
    'savefig.format': STYLE['save_format'],
    'savefig.bbox': 'tight',
    'savefig.transparent': False,
    'font.family': STYLE['font_family'],
    'font.size': STYLE['label_size'],
    'text.usetex': USETEX,
    'axes.titlesize': STYLE['title_size'],
    'axes.labelsize': STYLE['label_size'],
    'axes.linewidth': 0.8,
    'axes.edgecolor': STYLE['edge_color'],
    'xtick.labelsize': STYLE['tick_size'],
    'xtick.major.width': 0.8,
    'ytick.labelsize': STYLE['tick_size'],
    'ytick.major.width': 0.8,
    'legend.fontsize': STYLE['legend_size'],
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.grid': True,
    'axes.facecolor': STYLE['bg_color'],
    'axes.axisbelow': True,
    'grid.alpha': STYLE['grid_alpha'],
    'grid.linestyle': '--',
    'grid.linewidth': 0.8,
})
if not USETEX:
    # the fallback: Computer Modern text and math without a TeX install
    mpl.rcParams.update({
        'font.serif': ['cmr10'],
        'mathtext.fontset': 'cm',
        'axes.formatter.use_mathtext': True,
        'axes.unicode_minus': False,
    })


def tex(text):
    """Escape the characters LaTeX treats specially, when it renders. In the
    fallback, cmr10 has no underscore glyph (it draws a dot accent), so an
    underscore goes through mathtext instead."""
    if not USETEX:
        return text.replace("_", r"$\_$")
    return text.replace("%", r"\%").replace("_", r"\_")


# dataset -> name on the figures. Raw test outputs are read from
# runs/<dataset>/, tables written to tables/<dataset>-<name>.md; figures:
# see PLOTS.
DATASETS = {"nfcorpus": "NFCorpus/ColBERTv2", "scidocs": "SCIDOCS/ColBERTv2", "fiqa": "FiQA/ColBERTv2",
            "lotte": "LoTTE lifestyle/ColBERTv2",
            # the same corpora encoded with LateOn, runs/<dataset>-lateon/
            "nfcorpus-lateon": "NFCorpus/LateOn", "scidocs-lateon": "SCIDOCS/LateOn", "fiqa-lateon": "FiQA/LateOn"}
# The results root is an argument (one per machine or harness), default
# results/x86. A relative --results resolves against the project folder
# ($BENCH_HOME, default the parent of the fork that holds bench/), not the
# cwd.
_ap = argparse.ArgumentParser()
_ap.add_argument("dataset", nargs="?", default="nfcorpus", choices=sorted(DATASETS))
_ap.add_argument("--results", default="results/x86", help="results root, default results/x86")
# --final: the figures of the result alone, under plots/<encoder>/final/:
# HFresh + MUVERA v1.0 and v1.2 where they were run, and TachiomFresh v6.0;
# the version folders keep every version.
_ap.add_argument("--final", action="store_true", help="HFresh v1.0, v1.2 and TachiomFresh v6.0 only, into plots/<encoder>/final/")
# --version picks the newest TachiomFresh version on the figures: it is
# drawn with the earlier versions it is compared with (see SERIES), its
# figures go to plots/<encoder>/<version>/ and its tables get the
# <version>- prefix (v2.0's tables are unprefixed). v4.6 brings
# 4.4+centering with it. Default: the newest version whose run exists.
_ap.add_argument("--version", choices=["v2.0", "v3.0", "v3.1", "v3.2", "v3.3", "v3.4", "v3.5", "v3.6", "stepB-rescoring", "v4.0", "v4.3", "v4.4", "v4.5", "v4.6", "v5.0", "v5.1", "v6.0", "v7.0"], default=None)
_args = _ap.parse_args()
RESULTS = str(Path(os.environ.get("BENCH_HOME") or Path(__file__).resolve().parent.parent.parent).resolve() / _args.results)
DATASET = _args.dataset
# The figures of the object-store harness (results/x86-lsm) say in their
# legend that the documents are read from the object store, as a shard
# reads them, so that they are never mistaken for memory-mapped figures of
# results/x86.
HARNESS_NOTE = "documents read from the object store" if os.path.basename(RESULTS.rstrip("/")) == "x86-lsm" else ""
RUNS = os.path.join(RESULTS, "runs", DATASET)
# Figures go to plots/<encoder>/<version>/<corpus>-<name>.png, the corpus
# name without the encoder suffix (e.g. plots/lateon/v4.4/<corpus>-pareto-r10-qps.png
# next to plots/colbertv2/v4.4/<corpus>-pareto-r10-qps.png). The run
# folders and the tables keep the dataset key (<corpus>-lateon).
ENCODER = "lateon" if DATASET.endswith("-lateon") else "colbertv2"
CORPUS = DATASET.removesuffix("-lateon")
PLOTS = os.path.join(RESULTS, "plots", ENCODER)
TABLES = os.path.join(RESULTS, "tables")
# An -extended run (a second run that widens the grid when an index stops
# at its edge) adds its cells to the same figures and tables. It rebuilds the same maxPostingSize, so parse()
# keeps one build line per build: the first file's.
TF_FILES = [os.path.join(RUNS, "frontier.txt"), os.path.join(RUNS, "frontier-extended.txt")]
HF_FILES = [os.path.join(RUNS, "muvera.txt"), os.path.join(RUNS, "muvera-extended.txt")]
# HFresh + MUVERA v1.1: HFresh's exact scoring reads each candidate into a
# pooled container, as TachiomFresh v4.3 does, so both indexes read the
# objects bucket the same way; muvera.txt holds v1.0. Drawn when its run
# exists, right after v1.0.
HF2_FILES = [os.path.join(RUNS, "muvera-v1.1.txt"), os.path.join(RUNS, "muvera-v1.1-extended.txt")]
# HFresh + MUVERA v1.2: v1.1 with the tokens centered on the mean of the
# first 10,000 document tokens before the muvera encoding, the rescoring on
# the original tokens.
HF3_FILES = [os.path.join(RUNS, "muvera-v1.2.txt"), os.path.join(RUNS, "muvera-v1.2-extended.txt")]
# a dedicated breakdown run if there is one, else the frontier run's lines
MEM_FILES = [os.path.join(RUNS, "memory-breakdown.txt")] + TF_FILES
# TachiomFresh v2.0 is a series on the figures when its run exists. Builds
# come from the run that built the indexes.
TF2_BUILD_FILES = [os.path.join(RUNS, "frontier-v2.0.txt")]
_settled = os.path.join(RUNS, "frontier-v2.0-settled.txt")
# One line per version: the pass comparable to HFresh + MUVERA, whose grid
# runs in the process that built the index, right after the build, with the
# merges its searches trigger. That is the first pass, the build run's own
# cells. A settled pass (frontier-*-settled.txt) is not drawn or tabled.
TF2_CELL_FILES = TF2_BUILD_FILES
# True draws v2.0's first pass as a second, dashed series (see main());
# off, since the cells drawn are already the first pass (TF2_CELL_FILES).
TF2_FIRST_PASS = False

TF, HF = "TachiomFresh v1.0", "HFresh + MUVERA"
HF2 = "HFresh + MUVERA v1.1"
HF3 = "HFresh + MUVERA v1.2"
TF2 = "TachiomFresh v2.0"
TF2A = "TachiomFresh v2.0, right after build"
# v3.0 (TAC-lite): its files are in runs/<dataset>/v3.0/
TF3 = "TachiomFresh v3.0"
TF3A = "TachiomFresh v3.0, right after build"
TF3_BUILD_FILES = [os.path.join(RUNS, "v3.0", "frontier.txt")]
_settled3 = os.path.join(RUNS, "v3.0", "frontier-settled.txt")
TF3_CELL_FILES = TF3_BUILD_FILES
# v3.1 (posting size by token type): runs/<dataset>/v3.1/
TF4 = "TachiomFresh v3.1"
# frontier-extended.txt: builds or cells added after the first run, as
# TF_FILES does for v1.0
TF4_BUILD_FILES = [os.path.join(RUNS, "v3.1", "frontier.txt"), os.path.join(RUNS, "v3.1", "frontier-extended.txt")]
TF4_CELL_FILES = TF4_BUILD_FILES
# v3.2 (variance in the posting size): runs/<dataset>/v3.2/
TF5 = "TachiomFresh v3.2"
TF5_BUILD_FILES = [os.path.join(RUNS, "v3.2", "frontier.txt"), os.path.join(RUNS, "v3.2", "frontier-extended.txt")]
TF5_CELL_FILES = TF5_BUILD_FILES
# v3.3: v3.2's index with a cheaper stage 1 in search
TF6 = "TachiomFresh v3.3"
TF6_BUILD_FILES = [os.path.join(RUNS, "v3.3", "frontier.txt"), os.path.join(RUNS, "v3.3", "frontier-extended.txt")]
TF6_CELL_FILES = TF6_BUILD_FILES
# v3.4: v3.3 with the index's own posting store
TF7 = "TachiomFresh v3.4"
TF7_BUILD_FILES = [os.path.join(RUNS, "v3.4", "frontier.txt"), os.path.join(RUNS, "v3.4", "frontier-extended.txt")]
TF7_CELL_FILES = TF7_BUILD_FILES
# v3.5: v3.4's posting table packed in exactly sized byte arrays, fields at
# 4 + 2 bytes; v3.6: the widths fitted per posting. Same index, search results and dynamics as v3.4.
TF8 = "TachiomFresh v3.5"
TF8_BUILD_FILES = [os.path.join(RUNS, "v3.5", "frontier.txt"), os.path.join(RUNS, "v3.5", "frontier-extended.txt")]
TF8_CELL_FILES = TF8_BUILD_FILES
TF9 = "TachiomFresh v3.6"
TF9_BUILD_FILES = [os.path.join(RUNS, "v3.6", "frontier.txt"), os.path.join(RUNS, "v3.6", "frontier-extended.txt")]
TF9_CELL_FILES = TF9_BUILD_FILES
# step B: v3.6 plus a centroid-only rescoring of the stage 1 candidates in
# memory before the exact MaxSim on disk; a cell has a fourth parameter,
# rescoreLimit, the documents sent to the disk (equal to kDocsToScore: no
# rescoring, v3.6's search). Not adopted; its runs are in stepB-rescoring/.
# Drawn only with --version stepB-rescoring.
TF10 = "TachiomFresh step B"
TF10_BUILD_FILES = [os.path.join(RUNS, "stepB-rescoring", "frontier.txt"), os.path.join(RUNS, "stepB-rescoring", "frontier-extended.txt")]
TF10_CELL_FILES = TF10_BUILD_FILES
# v4.0: v3.6 whose splits, merges, reassigns and
# neighbour check read one token from the stored object through a ranged
# read, instead of the whole document. Same index and search results as
# v3.6; measured on the object-store harness.
TF11 = "TachiomFresh v4.0"
TF11_BUILD_FILES = [os.path.join(RUNS, "v4.0", "frontier.txt"), os.path.join(RUNS, "v4.0", "frontier-extended.txt")]
TF11_CELL_FILES = TF11_BUILD_FILES
# v4.3: v4.0 with stage 3 reading each candidate
# into the query's reused memory. Same build, index and results as v4.0.
TF12 = "TachiomFresh v4.3"
TF12_BUILD_FILES = [os.path.join(RUNS, "v4.3", "frontier.txt"), os.path.join(RUNS, "v4.3", "frontier-extended.txt")]
TF12_CELL_FILES = TF12_BUILD_FILES
# v4.4: v4.3 plus a cleanup of the entries of deleted documents, enqueued
# by the scan that finds a posting over a third dead. Same build and search
# as v4.3; its update-benchmark runs are drawn by plot_dynamics.py.
TF13 = "TachiomFresh v4.4"
TF13_BUILD_FILES = [os.path.join(RUNS, "v4.4", "frontier.txt"), os.path.join(RUNS, "v4.4", "frontier-extended.txt")]
TF13_CELL_FILES = TF13_BUILD_FILES
# v4.5: v4.4 with a token assigned to the centroid of its type nearest in
# L2 (computed on the RQ-8 codes of the centroid HNSW)
# instead of the one of largest dot product; the split, merge and reassign
# checks compare L2 too; the queries probe by the dot product as before.
TF14 = "TachiomFresh v4.5"
TF14_BUILD_FILES = [os.path.join(RUNS, "v4.5", "frontier.txt"), os.path.join(RUNS, "v4.5", "frontier-extended.txt")]
TF14_CELL_FILES = TF14_BUILD_FILES
# The centering: the mean of the first 10,000 document tokens is subtracted
# from every document token before it meets a centroid (insert, split,
# reassign, the checks); queries are raw. v4.6 is the centering on v4.5 (L2
# assignment); 4.4+centering is the same code on v4.4 (dot-product
# assignment).
TF15 = "TachiomFresh 4.4+centering"
TF15_BUILD_FILES = [os.path.join(RUNS, "4.4+centering", "frontier.txt"), os.path.join(RUNS, "4.4+centering", "frontier-extended.txt")]
TF15_CELL_FILES = TF15_BUILD_FILES
TF16 = "TachiomFresh v4.6"
TF16_BUILD_FILES = [os.path.join(RUNS, "v4.6", "frontier.txt"), os.path.join(RUNS, "v4.6", "frontier-extended.txt")]
TF16_CELL_FILES = TF16_BUILD_FILES
# v5.0: v4.6 where the spread of a type is TACHIOM's variance and the
# posting limit of a large type is sqrt(n) over the type's variance
# relative to the mean variance over the large types.
TF17 = "TachiomFresh v5.0"
TF17_BUILD_FILES = [os.path.join(RUNS, "v5.0", "frontier.txt"), os.path.join(RUNS, "v5.0", "frontier-extended.txt")]
TF17_CELL_FILES = TF17_BUILD_FILES
# v5.1: v5.0 with the ratio capped
# at 1, so a type more spread out than the mean gets sqrt(n) and no more.
TF18 = "TachiomFresh v5.1"
TF18_BUILD_FILES = [os.path.join(RUNS, "v5.1", "frontier.txt"), os.path.join(RUNS, "v5.1", "frontier-extended.txt")]
TF18_CELL_FILES = TF18_BUILD_FILES
# v6.0: v5.1 with the index
# computing its own centering mean: the documents are staged and searched
# exhaustively under 10,000 tokens, then the index is built on the mean of
# every staged token.
TF19 = "TachiomFresh v6.0"
TF19_BUILD_FILES = [os.path.join(RUNS, "v6.0", "frontier.txt"), os.path.join(RUNS, "v6.0", "frontier-extended.txt")]
TF19_CELL_FILES = TF19_BUILD_FILES
# v7.0: v6.0 made safe against crashes and repeated inserts, with the token
# ids taken through the API and the dot product only.
TF20 = "TachiomFresh v7.0"
TF20_BUILD_FILES = [os.path.join(RUNS, "v7.0", "frontier.txt"), os.path.join(RUNS, "v7.0", "frontier-extended.txt")]
TF20_CELL_FILES = TF20_BUILD_FILES
# What a legend shows for each index: the version and, in brackets, what it
# does, so that a figure can be read on its own. The series names above stay
# the keys and the names in the tables.
LEGEND = {
    HF: "HFresh + MUVERA v1.0",
    HF2: "HFresh + MUVERA v1.1 (exact scoring reads into reused memory)",
    HF3: "HFresh + MUVERA v1.2 (v1.1 + centered muvera input)",
    TF: "TACHIOM-Fresh v1.0 (static)",
    TF2: "TACHIOM-Fresh v2.0 (dynamic)",
    TF3: "TACHIOM-Fresh v3.0 (TAC token separation only)",
    TF4: "TACHIOM-Fresh v3.1 (TAC sqrt allocation)",
    TF5: "TACHIOM-Fresh v3.2 (TAC sqrt + spread)",
    TF6: "TACHIOM-Fresh v3.3 (v3.2 + faster candidate scan)",
    TF7: "TACHIOM-Fresh v3.4 (v3.3 + own posting store)",
    TF8: "TACHIOM-Fresh v3.5 (v3.4 + packed postings, 4 + 2 bytes per entry)",
    TF9: "TACHIOM-Fresh v3.6 (v3.5 + posting field widths fitted per posting)",
    TF10: "TACHIOM-Fresh step B (v3.6 + centroid-only rescoring before the disk; not adopted)",
    TF11: "TACHIOM-Fresh v4.0 (v3.6 + token-level reads from the object store)",
    TF12: "TACHIOM-Fresh v4.3 (v4.0 + exact scoring reads into reused memory)",
    TF13: "TACHIOM-Fresh v4.4 (v4.3 + cleanup of dead entries)",
    TF14: "TACHIOM-Fresh v4.5 (v4.4 + L2 placement)",
    TF15: "TACHIOM-Fresh 4.4+centering (v4.4 + tokens centered on the mean)",
    TF16: "TACHIOM-Fresh v4.6 (v4.5 + tokens centered on the mean)",
    TF17: "TACHIOM-Fresh v5.0 (v4.6 + spread independent from mean)",
    TF18: "TACHIOM-Fresh v5.1 (v5.0 + spread capped at 1)",
    TF19: "TACHIOM-Fresh v6.0 (v5.1 + the index computes its own mean)",
    TF20: "TACHIOM-Fresh v7.0 (v6.0 + robustness and token ids from the API)",
}

VERSION = _args.version or ("v7.0" if os.path.exists(TF20_BUILD_FILES[0]) else
                            "v6.0" if os.path.exists(TF19_BUILD_FILES[0]) else
                            "v5.1" if os.path.exists(TF18_BUILD_FILES[0]) else
                            "v5.0" if os.path.exists(TF17_BUILD_FILES[0]) else
                            "v4.6" if os.path.exists(TF16_BUILD_FILES[0]) or os.path.exists(TF15_BUILD_FILES[0]) else
                            "v4.5" if os.path.exists(TF14_BUILD_FILES[0]) else
                            "v4.4" if os.path.exists(TF13_BUILD_FILES[0]) else
                            "v4.3" if os.path.exists(TF12_BUILD_FILES[0]) else
                            "v4.0" if os.path.exists(TF11_BUILD_FILES[0]) else
                            "v3.6" if os.path.exists(TF9_BUILD_FILES[0]) else
                            "v3.5" if os.path.exists(TF8_BUILD_FILES[0]) else
                            "v3.4" if os.path.exists(TF7_BUILD_FILES[0]) else
                            "v3.3" if os.path.exists(TF6_BUILD_FILES[0]) else
                            "v3.2" if os.path.exists(TF5_BUILD_FILES[0]) else
                            "v3.1" if os.path.exists(TF4_BUILD_FILES[0]) else
                            "v3.0" if os.path.exists(TF3_BUILD_FILES[0]) else "v2.0")
# V3: v3.0 is on the figure; V4: v3.1 is too; V5: v3.2 is too
V70 = VERSION == "v7.0"
V60 = V70 or VERSION == "v6.0"
V51 = V60 or VERSION == "v5.1"
V50 = V51 or VERSION == "v5.0"
V15 = V50 or VERSION == "v4.6"
V14 = V15 or VERSION == "v4.5"
V13 = V14 or VERSION == "v4.4"
V12 = V13 or VERSION == "v4.3"
V11 = V12 or VERSION == "v4.0"
V10 = VERSION == "stepB-rescoring"
V9 = V11 or V10 or VERSION == "v3.6"
V8 = V9 or VERSION == "v3.5"
V7 = V8 or VERSION == "v3.4"
V6 = V7 or VERSION == "v3.3"
V5 = V6 or VERSION == "v3.2"
V4 = V5 or VERSION == "v3.1"
V3 = V4 or VERSION == "v3.0"
PREFIX = ("final-" if _args.final else f"{VERSION}-") if V3 else ""
if V3:
    MEM_FILES = TF20_BUILD_FILES if V70 else TF19_BUILD_FILES if V60 else TF18_BUILD_FILES if V51 else TF17_BUILD_FILES if V50 else TF16_BUILD_FILES if V15 else TF14_BUILD_FILES if V14 else TF13_BUILD_FILES if V13 else TF12_BUILD_FILES if V12 else TF11_BUILD_FILES if V11 else TF10_BUILD_FILES if V10 else TF9_BUILD_FILES if V9 else TF8_BUILD_FILES if V8 else TF7_BUILD_FILES if V7 else TF6_BUILD_FILES if V6 else TF5_BUILD_FILES if V5 else TF4_BUILD_FILES if V4 else TF3_BUILD_FILES
# the plot shows only cells at or above this recall@10; the frontier is
# unaffected, since a cell with recall >= this can only be dominated by
# cells that are also above it. An index whose best cell is under it keeps
# its legend entry and is named in a note on the figure with its best recall
MIN_PLOT_RECALL = 0.80
# build and memory bars show every build that has a point on the drawn
# frontier, see main()

def build_key(name, params):
    """The build a cell or build line of series name belongs to."""
    return f"{name} mps={params['maxPostingSize']}"


# Order in legends and bar groups: HFresh first, then the TachiomFresh
# versions together, oldest to newest. A version is drawn when its run
# exists (a dataset can have runs of some versions only). From v5.0 on,
# v3.6, v4.0 and v4.3 are left out of the figures.
SERIES = (([HF] if any(os.path.exists(f) for f in HF_FILES) else []) + ([HF2] if any(os.path.exists(f) for f in HF2_FILES) else [])
          + ([HF3] if any(os.path.exists(f) for f in HF3_FILES) else [])
          + ([TF] if any(os.path.exists(f) for f in TF_FILES) else [])
          + ([TF2] if os.path.exists(TF2_BUILD_FILES[0]) else [])
          + ([TF3] if V3 and os.path.exists(TF3_BUILD_FILES[0]) else []) + ([TF4] if V4 and os.path.exists(TF4_BUILD_FILES[0]) else []) + ([TF5] if V5 and os.path.exists(TF5_BUILD_FILES[0]) else []) + ([TF6] if V6 and os.path.exists(TF6_BUILD_FILES[0]) else []) + ([TF7] if V7 and os.path.exists(TF7_BUILD_FILES[0]) else [])
          + ([TF8] if V8 and os.path.exists(TF8_BUILD_FILES[0]) else []) + ([TF9] if V9 and not V50 and os.path.exists(TF9_BUILD_FILES[0]) else [])
          + ([TF10] if V10 else []) + ([TF11] if V11 and not V50 and os.path.exists(TF11_BUILD_FILES[0]) else [])
          + ([TF12] if V12 and not V50 and os.path.exists(TF12_BUILD_FILES[0]) else []) + ([TF13] if V13 and os.path.exists(TF13_BUILD_FILES[0]) else [])
          + ([TF14] if V14 and os.path.exists(TF14_BUILD_FILES[0]) else [])
          + ([TF15] if V15 and os.path.exists(TF15_BUILD_FILES[0]) else []) + ([TF16] if V15 and os.path.exists(TF16_BUILD_FILES[0]) else [])
          + ([TF17] if V50 and os.path.exists(TF17_BUILD_FILES[0]) else [])
          + ([TF18] if V51 and os.path.exists(TF18_BUILD_FILES[0]) else [])
          + ([TF19] if V60 and os.path.exists(TF19_BUILD_FILES[0]) else [])
          + ([TF20] if V70 and os.path.exists(TF20_BUILD_FILES[0]) else [])
)
FINAL = _args.final
if FINAL:
    SERIES = [name for name in (HF, HF3, TF19) if name in SERIES]
# an index keeps its palette slot, marker and hatch on every figure
SLOT = {TF: 0, HF: 1, TF2: 2, TF3: 3, TF4: 4, TF5: 5, TF6: 6, TF7: 7, TF8: 8, TF9: 9, TF10: 10, TF11: 11, TF12: 12, HF2: 13, TF13: 14, TF14: 15, HF3: 16, TF15: 17, TF16: 18, TF17: 19, TF18: 20, TF19: 21, TF20: 22}
COLOR = {name: STYLE['palette'][SLOT[name]] for name in SERIES}
MARKER = {name: STYLE['markers'][SLOT[name]] for name in SERIES}
HATCH = {name: STYLE['hatches'][SLOT[name]] for name in SERIES}
# the newest TachiomFresh version on the figure: its points are labelled and
# its first pass is drawn dashed; the older one's settings are in the table
NEWEST = TF20 if V70 and TF20 in SERIES else TF19 if V60 and TF19 in SERIES else TF18 if V51 and TF18 in SERIES else TF17 if V50 and TF17 in SERIES else TF16 if V15 and TF16 in SERIES else TF15 if V15 and TF15 in SERIES else TF14 if V14 and TF14 in SERIES else TF13 if V13 and TF13 in SERIES else TF12 if V12 else TF11 if V11 else TF10 if V10 else TF9 if V9 else TF8 if V8 else TF7 if V7 else TF6 if V6 else TF5 if V5 else TF4 if V4 else TF3 if V3 else TF2

# go test -v prints a t.Log line as "    file.go:123: text" (the tests log
# one line per call); runs printed with fmt.Printf have the text at the line
# start. Both parse the same. Indentation alone is kept: some lines are
# indented and must not match.
LOG_PREFIX = re.compile(r"^\s*\S+\.go:\d+: ")


def unprefix(line):
    return LOG_PREFIX.sub("", line, count=1)


CELL = re.compile(r"cell index=(\S+) (.*) recall@10=([\d.]+) qps=([\d.]+) mean_ms=([\d.]+)")
BUILD = re.compile(r"build index=(\S+) (.*)")


def parse(paths, name):
    """Cells and builds of series name from the run files in paths that exist (at least
    one must, e.g. only the -extended run); a build seen in an
    earlier file wins over the same build in a later one, see TF_FILES."""
    cells, builds, seen = [], [], set()
    if not any(os.path.exists(path) for path in paths):
        raise FileNotFoundError(paths[0])
    for path in paths:
        if not os.path.exists(path):
            continue
        for line in open(path):
            line = unprefix(line)
            m = CELL.match(line)
            if m:
                params = dict(kv.split("=") for kv in m.group(2).split())
                cells.append({"index": m.group(1), "series": name, "params": params,
                              "r10": float(m.group(3)), "qps": float(m.group(4)), "mean_ms": float(m.group(5))})
                continue
            m = BUILD.match(line)
            if m:
                fields = dict(kv.split("=") for kv in m.group(2).split())
                fields["index"] = m.group(1)
                fields["series"] = name
                key = build_key(name, fields)
                if key not in seen:
                    seen.add(key)
                    builds.append(fields)
    return cells, builds


def frontier(cells):
    """Cells not dominated in (qps, r10): walking from fastest to slowest, keep
    a cell only if it beats every faster cell's recall."""
    best = -1.0
    out = []
    for c in sorted(cells, key=lambda c: (-c["qps"], -c["r10"])):
        if c["r10"] > best:
            out.append(c)
            best = c["r10"]
    return out


def one_per_bin(front):
    """At most one frontier point per integer accuracy bin, e.g. [90, 91):
    the fastest one, the best QPS at that accuracy level (to declutter the
    plot)."""
    best = {}
    for c in front:
        b = int(100 * c["r10"])
        if b not in best or c["qps"] > best[b]["qps"]:
            best[b] = c
    return [best[b] for b in sorted(best)]


def declutter(fig, ax, anns, points, step=8, rounds=40, marker_px=12):
    """Nudge point labels until none overlaps another label or a marker: a
    label moves further from its point, in the direction it already sits (up
    if above, down if below). Frontier points bunch up where accuracy moves
    little between settings. points are (x, y) in data
    coordinates; each marker is treated as a marker_px square."""
    fig.canvas.draw()
    half = marker_px / 2
    markers = []
    for x, y in points:
        px, py = ax.transData.transform((x, y))
        markers.append(Bbox.from_extents(px - half, py - half, px + half, py + half))

    def nudge(a):
        x, y = a.xyann
        a.xyann = (x, y + step if y >= 0 else y - step)

    for _ in range(rounds):
        fig.canvas.draw()
        boxes = [a.get_window_extent() for a in anns]
        moved = False
        for i in range(len(anns)):
            if any(boxes[i].overlaps(m) for m in markers):
                nudge(anns[i])
                boxes[i] = anns[i].get_window_extent()
                moved = True
            for j in range(i + 1, len(anns)):
                if boxes[i].overlaps(boxes[j]):
                    nudge(anns[j])
                    boxes[j] = anns[j].get_window_extent()
                    moved = True
        if not moved:
            return


def save(fig, name):
    """PNG only (the style's default is PDF)."""
    # one folder per version: plots/<encoder>/<version>/<corpus>-<name>.png,
    # plots/<encoder>/final/ with --final
    folder = "final" if FINAL else VERSION
    os.makedirs(os.path.join(PLOTS, folder), exist_ok=True)
    fig.savefig(os.path.join(PLOTS, folder, f"{CORPUS}-{name}.png"), dpi=200, format="png")
    plt.close(fig)


def legend(ax, **kw):
    # legend frame as in the shared plot style
    return ax.legend(frameon=True, fancybox=False, edgecolor='0.8', **kw)


def label_tf(p):
    label = f"mps={p['maxPostingSize']} kC={p['kCentroids']} kDocs={p['kDocsToScore']}"
    # step B: the disk budget of the centroid-only rescoring
    if "rescoreLimit" in p:
        label += f" R={p['rescoreLimit']}"
    return label


def label_hf(p):
    return f"mps={p['maxPostingSize']} probe={p['searchProbe']} rescore={p['rescoreLimit']}"


MEMORY = re.compile(r"memory index=tachiomfresh (.*)")

# the breakdown's parts, in stacking order: (legend label, measured
# fields summed). Coarse on purpose: the centroid HNSW is the same code in
# both indexes, the question is centroids vs postings
PARTS = [
    ("centroids (HNSW graph, RQ-8 codes, split tombstones)",
     ("graph_bytes", "vector_cache_bytes", "tombstones_bytes")),
    ("postings", ("postings_bytes",)),
    # absent (0) in runs of versions without token types
    ("token types", ("types_bytes",)),
    # step B (rescoring) only; absent (0) elsewhere
    ("forward map (centroid per token)", ("forward_bytes",)),
]
# Left out of the bars, in the table: memtable_bytes (the LSM store's
# pending writes after the search grid, the merges and reassigns the
# searches triggered; flushed at a threshold in production, never in the
# harness) and other_bytes (version map, task queues, locks, id sequence,
# commit logger).


def memory_breakdown(drawn=None):
    """TachiomFresh RAM per build as one horizontal bar split into its parts.
    drawn: the maxPostingSize values with a point on the drawn Pareto
    frontier; only those get a bar, as in the build and memory bars. The
    table keeps every build."""
    rows = {}
    for path in MEM_FILES:
        if not os.path.exists(path):
            continue
        for line in open(path):
            line = unprefix(line)
            m = MEMORY.match(line)
            if m:
                f = {k: int(v) for k, v in (kv.split("=") for kv in m.group(1).split())}
                rows.setdefault(f["maxPostingSize"], f)
        if rows:
            print(f"memory breakdown from {path}")
            # a dedicated breakdown run stands alone; a version's frontier
            # files are all read, since a build added later is in
            # frontier-extended.txt
            if os.path.basename(path) == "memory-breakdown.txt":
                break
    if not rows:
        print("no memory lines; breakdown figure skipped")
        return
    for f in rows.values():
        for label, fields in PARTS:
            # tombstones_bytes exists only in older runs; newer runs count
            # split tombstones inside graph_bytes
            f[label] = sum(f.get(x, 0) for x in fields)
            if f[label] < 0:
                print(f"WARNING: mps={f['maxPostingSize']} {label}={f[label]} is negative; drawn as 0")

    # a part no build of this version reports (the forward map outside step
    # B; the token types before v3.0) gets no segment, no legend entry and
    # no table row
    parts = [(label, fields) for label, fields in PARTS if any(f[label] > 0 for f in rows.values())]

    all_mps = sorted(rows)
    mps = [m for m in all_mps if drawn is None or m in drawn]
    if not mps:
        print("no build of the breakdown is on the drawn frontier; all builds drawn")
        mps = all_mps
    print("breakdown bars: mps " + ", ".join(str(m) for m in mps))
    fig, ax = plt.subplots(figsize=(11.5, 0.9 * len(mps) + 1.5))
    ys = np.arange(len(mps))
    left = np.zeros(len(mps))
    for i, (label, _) in enumerate(parts):
        widths = np.array([max(rows[m][label], 0) / 1e6 for m in mps])
        # black-edged hatched bars, as in the shared plot style
        ax.barh(ys, widths, left=left, height=0.62, color=STYLE['palette'][i],
                edgecolor='black', linewidth=0.6, hatch=STYLE['hatches'][i], label=label)
        left += widths
    # the label is the sum of the drawn parts, not total_bytes, which also
    # holds other_bytes (heap not attributed to a part of the index), so the
    # label matches its bar
    for y, m in zip(ys, mps):
        ax.annotate(f"{left[y]:.1f} MB", (left[y], y), xytext=(6, 0),
                    textcoords="offset points", va="center", fontsize=13)
    ax.set_yticks(ys)
    ax.set_yticklabels([f"mps={m}" for m in mps])
    ax.invert_yaxis()
    ax.set_xlabel("RAM (MB, live heap)")
    ax.set_xlim(0, left.max() * 1.15)
    ax.grid(axis="y", visible=False)
    # above the axes: with few builds, any spot inside covers a bar, and
    # below it the offset depends on the figure height
    legend(ax, loc="lower center", bbox_to_anchor=(0.5, 1.02), ncol=2, fontsize=13, title=HARNESS_NOTE or None, title_fontsize=12)
    fig.tight_layout()
    save(fig, "memory-breakdown")

    mps = all_mps
    out = ["| part | " + " | ".join(f"mps={m}" for m in mps) + " |",
           "|---|" + "---|" * len(mps)]
    for label, _ in parts + [("memtable_bytes", None), ("other_bytes", None), ("total_bytes", None)]:
        # older runs have no memtable_bytes and hold it inside other_bytes
        out.append(f"| {label} | " + " | ".join(f"{rows[m].get(label, 0) / 1e6:.1f} MB" for m in mps) + " |")
    for f in rows.values():
        f.setdefault("types_bytes", 0)
    for field in ("entries", "live_postings", "cleared_postings"):
        out.append(f"| {field} | " + " | ".join(f"{rows[m][field]:,}" for m in mps) + " |")
    open(os.path.join(TABLES, f"{DATASET}-{PREFIX}memory-breakdown.md"), "w").write("\n".join(out) + "\n")
    print("\n".join(out))


def main():
    series, builds = {}, {}
    if TF in SERIES:
        series[TF], builds[TF] = parse(TF_FILES, TF)
    if HF in SERIES:
        series[HF], builds[HF] = parse(HF_FILES, HF)
    if HF2 in SERIES:
        series[HF2], builds[HF2] = parse(HF2_FILES, HF2)
    if HF3 in SERIES:
        series[HF3], builds[HF3] = parse(HF3_FILES, HF3)
    if TF2 in SERIES:
        series[TF2], _ = parse(TF2_CELL_FILES, TF2)
        _, builds[TF2] = parse(TF2_BUILD_FILES, TF2)
    if TF3 in SERIES:
        series[TF3], _ = parse(TF3_CELL_FILES, TF3)
        _, builds[TF3] = parse(TF3_BUILD_FILES, TF3)
    if TF4 in SERIES:
        series[TF4], _ = parse(TF4_CELL_FILES, TF4)
        _, builds[TF4] = parse(TF4_BUILD_FILES, TF4)
    if TF5 in SERIES:
        series[TF5], _ = parse(TF5_CELL_FILES, TF5)
        _, builds[TF5] = parse(TF5_BUILD_FILES, TF5)
    if TF6 in SERIES:
        series[TF6], _ = parse(TF6_CELL_FILES, TF6)
        _, builds[TF6] = parse(TF6_BUILD_FILES, TF6)
    if TF7 in SERIES:
        series[TF7], _ = parse(TF7_CELL_FILES, TF7)
        _, builds[TF7] = parse(TF7_BUILD_FILES, TF7)
        # v3.4's RAM comes from a second run of its builds when there is one
        # (frontier-ram.txt): a fix to the reassign queue's dedup set changed
        # its RAM only, not its search results
        ram_file = os.path.join(RUNS, "v3.4", "frontier-ram.txt")
        if os.path.exists(ram_file):
            _, again = parse([ram_file], TF7)
            ram = {b["maxPostingSize"]: b["ram_bytes"] for b in again}
            for b in builds[TF7]:
                if b["maxPostingSize"] in ram:
                    b["ram_bytes"] = ram[b["maxPostingSize"]]
            print(f"v3.4 RAM from {ram_file}")
    if TF8 in SERIES:
        series[TF8], _ = parse(TF8_CELL_FILES, TF8)
        _, builds[TF8] = parse(TF8_BUILD_FILES, TF8)
    if TF9 in SERIES:
        series[TF9], _ = parse(TF9_CELL_FILES, TF9)
        _, builds[TF9] = parse(TF9_BUILD_FILES, TF9)
    if TF10 in SERIES:
        series[TF10], _ = parse(TF10_CELL_FILES, TF10)
        _, builds[TF10] = parse(TF10_BUILD_FILES, TF10)
    if TF11 in SERIES:
        series[TF11], _ = parse(TF11_CELL_FILES, TF11)
        _, builds[TF11] = parse(TF11_BUILD_FILES, TF11)
    if TF12 in SERIES:
        series[TF12], _ = parse(TF12_CELL_FILES, TF12)
        _, builds[TF12] = parse(TF12_BUILD_FILES, TF12)
    if TF13 in SERIES:
        series[TF13], _ = parse(TF13_CELL_FILES, TF13)
        _, builds[TF13] = parse(TF13_BUILD_FILES, TF13)
    if TF14 in SERIES:
        series[TF14], _ = parse(TF14_CELL_FILES, TF14)
        _, builds[TF14] = parse(TF14_BUILD_FILES, TF14)
    if TF15 in SERIES:
        series[TF15], _ = parse(TF15_CELL_FILES, TF15)
        _, builds[TF15] = parse(TF15_BUILD_FILES, TF15)
    if TF16 in SERIES:
        series[TF16], _ = parse(TF16_CELL_FILES, TF16)
        _, builds[TF16] = parse(TF16_BUILD_FILES, TF16)
    if TF17 in SERIES:
        series[TF17], _ = parse(TF17_CELL_FILES, TF17)
        _, builds[TF17] = parse(TF17_BUILD_FILES, TF17)
    if TF18 in SERIES:
        series[TF18], _ = parse(TF18_CELL_FILES, TF18)
        _, builds[TF18] = parse(TF18_BUILD_FILES, TF18)
    if TF19 in SERIES:
        series[TF19], _ = parse(TF19_CELL_FILES, TF19)
        _, builds[TF19] = parse(TF19_BUILD_FILES, TF19)
    if TF20 in SERIES:
        series[TF20], _ = parse(TF20_CELL_FILES, TF20)
        _, builds[TF20] = parse(TF20_BUILD_FILES, TF20)
    fronts = {name: frontier(series[name]) for name in SERIES}
    # an index with no cell at the cutoff is named on the figure with its
    # best recall, so that its legend entry is explained
    under_cutoff = [(name, max(fronts[name], key=lambda c: c["r10"])) for name in SERIES
                    if max(c["r10"] for c in fronts[name]) < MIN_PLOT_RECALL]
    # first passes: (label, frontier); the newest version's is drawn
    first_passes = {}
    if TF2 in SERIES and TF2_FIRST_PASS:
        first_passes[TF2] = (TF2A, frontier(parse(TF2_BUILD_FILES, TF2A)[0]))
    if False:  # see TF2_CELL_FILES: one line per version
        first_passes[TF3] = (TF3A, frontier(parse(TF3_BUILD_FILES, TF3A)[0]))
    first_pass = first_passes.get(NEWEST, (None, []))[1]

    # --- figure 1: Pareto frontier, x Accuracy@10 (%), y QPS, both linear --
    # (no title, as in the shared style's examples)
    fig, ax = plt.subplots(figsize=(11.5, 6.5))
    point_labels, points = [], []
    # step B's points carry a fourth value, the documents sent to the disk
    LABELS = ("labels: (maxPostingSize, n_probe, k_candidates, rescoreLimit)" if V10
              else "labels: (maxPostingSize, n_probe, rescoreLimit)")
    # tallest plotted point: a label hung below a point near the x axis would
    # fall under the axis
    top_qps = max((c["qps"] for name in SERIES for c in fronts[name] if c["r10"] >= MIN_PLOT_RECALL), default=1.0)
    for name in SERIES:
        f = one_per_bin([c for c in fronts[name] if c["r10"] >= MIN_PLOT_RECALL])
        xs, ys = [100 * c["r10"] for c in f], [c["qps"] for c in f]
        points += list(zip(xs, ys))
        # line, white halo, black-edged marker: the shared style's look
        ax.plot(xs, ys, color=COLOR[name], linewidth=STYLE['line_width'], alpha=0.85)
        ax.scatter(xs, ys, color='white', marker=MARKER[name], s=(STYLE['marker_size'] * 1.4) ** 2,
                   edgecolor='none', zorder=2)
        ax.scatter(xs, ys, color=COLOR[name], marker=MARKER[name], s=STYLE['marker_size'] ** 2,
                   edgecolor='black', linewidth=0.5, label=LEGEND[name], zorder=3)
        # every frontier point carries (maxPostingSize, n_probe, rescoreLimit):
        # maxPostingSize is the build knob both indexes sweep, while the
        # centroid counts are on different scales (tokens vs one FDE per doc). TachiomFresh's frontier descends steeply,
        # so up-right of each point is clear of its line; HFresh's is mostly
        # flat, so its labels alternate centred above and below the line
        for i, c in enumerate(f):
            p = c["params"]
            # only the newest TachiomFresh version and the newest HFresh
            # version get point labels: frontiers side by side would cover
            # each other's labels. The others' settings are in the table.
            if name not in (HF3 if HF3 in SERIES else HF2 if HF2 in SERIES else HF, NEWEST) and NEWEST in SERIES:
                continue
            # two frontier points of one index almost on top of each other:
            # the second label has no free spot next to its point and
            # ends up far from it; its settings are in the table
            if i > 0 and 100 * (c["r10"] - f[i - 1]["r10"]) < 0.3 and abs(c["qps"] - f[i - 1]["qps"]) < 0.02 * top_qps:
                continue
            if name not in (HF, HF2, HF3):
                probe, rescore = p["kCentroids"], p["kDocsToScore"]
                # step B: (mps, kC, kDocs, R), the rescore limit last
                if "rescoreLimit" in p:
                    rescore = f"{p['kDocsToScore']}, {p['rescoreLimit']}"
                # TachiomFresh's labels sit up-right of their points
                offset, ha = (7, 5), "left"
            else:
                probe, rescore = p["searchProbe"], p["rescoreLimit"]
                above = i % 2 == 0 or c["qps"] < 0.08 * top_qps
                offset, ha = ((0, 9), "center") if above else ((0, -19), "center")
                # a centred label on a point at the left edge would cross
                # the y axis
                if 100 * c["r10"] < 100 * MIN_PLOT_RECALL + 1.0:
                    ha = "left"
            point_labels.append(ax.annotate(
                f"({p['maxPostingSize']}, {probe}, {rescore})", (100 * c["r10"], c["qps"]),
                xytext=offset, textcoords="offset points", ha=ha, fontsize=10, color='#333333'))
    if first_pass:
        # the newest version's first pass: same colour, dashed, hollow
        # markers, no point labels (its settings are in the table)
        f = one_per_bin([c for c in first_pass if c["r10"] >= MIN_PLOT_RECALL])
        xs, ys = [100 * c["r10"] for c in f], [c["qps"] for c in f]
        ax.plot(xs, ys, color=COLOR[NEWEST], linewidth=STYLE['line_width'], alpha=0.85, linestyle="--")
        ax.scatter(xs, ys, color='white', marker=MARKER[NEWEST], s=STYLE['marker_size'] ** 2,
                   edgecolor=COLOR[NEWEST], linewidth=1.5, label=first_passes[NEWEST][0], zorder=3)
    ax.set_xlabel(tex("Accuracy@10 (%)"))
    ax.set_ylabel("QPS (one query at a time, sequential)")
    ax.set_xlim(100 * MIN_PLOT_RECALL, 100.5)
    if under_cutoff:
        # upper right: the frontiers end at the bottom right, so it is clear;
        # the series name as the legend shows it before its bracket, and its
        # best cell
        ax.text(0.98, 0.97, "\n".join(tex(f"{LEGEND[name].split(' (')[0]} best Accuracy@10: {100 * best['r10']:.1f} % at {best['qps']:.1f} QPS")
                                      for name, best in under_cutoff),
                transform=ax.transAxes, ha="right", va="top", fontsize=11, color="0.3")
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.set_ylim(0, None)
    # the legend title explains the point labels.
    # room above the highest point for its label, which otherwise runs into
    # the legend that sits on top of the axes
    ax.set_ylim(top=ax.get_ylim()[1] * 1.07)
    title = LABELS + (f"; {HARNESS_NOTE}" if HARNESS_NOTE else "")
    # above the axes: inside, it covers frontier points, since its title
    # spans the width
    legend(ax, loc="lower center", bbox_to_anchor=(0.5, 1.01), ncol=2, fontsize=12,
           title=tex(title), title_fontsize=12)
    fig.tight_layout()
    declutter(fig, ax, point_labels, points, step=10)
    save(fig, "pareto-r10-qps")

    # --- bar categories: every build with a point on the drawn frontier ----
    # a build has a bar exactly when one of its cells is a point of the
    # Pareto figure, so that all figures describe the same indexes
    best_r10 = {}
    for c in [c for name in SERIES for c in series[name]]:
        key = build_key(c["series"], c["params"])
        best_r10[key] = max(best_r10.get(key, 0.0), c["r10"])
    for key in sorted(best_r10):
        print(f"best recall@10 {key}: {best_r10[key]:.4f}")
    cats = []
    for name in SERIES:
        drawn = {build_key(name, c["params"])
                 for c in one_per_bin([c for c in fronts[name] if c["r10"] >= MIN_PLOT_RECALL])}
        chosen = sorted((b for b in builds[name] if build_key(name, b) in drawn),
                        key=lambda b: int(b["maxPostingSize"]))
        if not chosen:
            print(f"WARNING: no {name} build has a point on the drawn frontier; bars leave it out")
        for b in chosen:
            print(f"bars: {build_key(name, b)} (on the drawn frontier)")
            cats.append((build_key(name, b), name, b))
    # after the frontier is known: its bars are the newest version's builds
    # on the drawn frontier
    memory_breakdown({int(b["maxPostingSize"]) for _, name, b in cats if name == NEWEST})

    def bars(ax, value, ylabel, fmt, with_legend=True):
        """One bar per build on the drawn frontier, the builds of an index
        side by side with a gap between indexes; colour and hatch per index
        as in the shared plot style, maxPostingSize under each bar."""
        x, xs, prev = 0.0, [], None
        for _, name, b in cats:
            if prev is not None and name != prev:
                x += 0.6
            v = value(b)
            ax.bar(x, v, width=0.9, color=COLOR[name], edgecolor='black', linewidth=0.8,
                   hatch=HATCH[name], label=LEGEND[name] if name != prev else None)
            ax.text(x, v, fmt(v), ha="center", va="bottom", fontsize=11)
            xs.append(x)
            prev = name
            x += 1.0
        ax.set_xticks(xs)
        ax.set_xticklabels([b["maxPostingSize"] for _, _, b in cats], fontsize=13)
        ax.set_xlabel("maxPostingSize")
        ax.set_ylabel(ylabel)
        ax.grid(axis="x", visible=False)
        if with_legend:
            # headroom for the legend, one line per index
            ax.set_ylim(0, max(value(b) for _, _, b in cats) * (1.10 + 0.24 * len(SERIES)))
            legend(ax, loc="upper left", fontsize=12, title=HARNESS_NOTE or None, title_fontsize=12)

    # --- figure 2: build time ---------------------------------------------
    if not cats:
        # nothing on the drawn frontier yet (e.g. a run still in progress):
        # the frontier figure and the tables are still worth having
        print("no build on the drawn frontier; build and memory figures skipped")
    fig, ax = plt.subplots(figsize=STYLE['figure_size_wide'])
    if cats:
        bars(ax, lambda b: float(b["build_s"]), "Build time (s)", lambda v: f"{v:.1f}")
    fig.tight_layout()
    save(fig, "build-time")

    # --- figure 3: memory, RAM and disk as two panels on one MB scale -----
    fig, axes = plt.subplots(1, 2, figsize=(STYLE['figure_size_wide'][0] * 1.5, STYLE['figure_size_wide'][1]),
                             sharey=True)
    if cats:
        bars(axes[0], lambda b: int(b["ram_bytes"]) / 1e6, "RAM (MB)", lambda v: f"{v:.0f}")
        bars(axes[1], lambda b: int(b["disk_bytes"]) / 1e6, "Disk (MB)", lambda v: f"{v:.0f}", with_legend=False)
        top = max(max(int(b["ram_bytes"]), int(b["disk_bytes"])) for _, _, b in cats) / 1e6
        axes[0].set_ylim(0, top * (1.10 + 0.18 * len(SERIES)))
        axes[1].tick_params(labelleft=True)
    fig.tight_layout()
    save(fig, "memory")

    # --- tables -------------------------------------------------------------
    out = []
    head = "| config | recall@10 | QPS | mean |\n|---|---|---|---|"
    for name in SERIES:
        label = label_hf if name in (HF, HF2, HF3) else label_tf
        out.append(f"\nFrontier, {name}:\n")
        out.append(head)
        for c in fronts[name]:
            out.append(f"| {label(c['params'])} | {c['r10']:.4f} | {c['qps']:.1f} | {c['mean_ms']:.1f} ms |")
        if name in first_passes:
            out.append(f"\nFrontier, {first_passes[name][0]} (merges still running):\n")
            out.append(head)
            for c in first_passes[name][1]:
                out.append(f"| {label_tf(c['params'])} | {c['r10']:.4f} | {c['qps']:.1f} | {c['mean_ms']:.1f} ms |")
    out.append("\nBuild and memory:\n")
    out.append("| index | build | RAM | disk | centroids |\n|---|---|---|---|---|")
    all_builds = [(build_key(name, b), b) for name in SERIES for b in builds[name]]
    for name, b in all_builds:
        out.append(f"| {name} | {float(b['build_s']):.1f} s | {int(b['ram_bytes']) / 1e6:.1f} MB | "
                   f"{int(b['disk_bytes']) / 1e6:.1f} MB | {b['centroids']} |")
    out.append("\nCells measured: " + ", ".join(f"{name} {len(series[name])}" for name in SERIES) + ".")
    open(os.path.join(TABLES, f"{DATASET}-{PREFIX}compare.md"), "w").write("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
