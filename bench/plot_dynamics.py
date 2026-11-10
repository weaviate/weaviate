"""Figures and table of the update benchmark (the static figures only
measure search on an index that was built once).

    python3 bench/plot_dynamics.py <nfcorpus|scidocs> [--results results/x86]

Reads runs/<dataset>/dynamics-v2.0.txt (TestNFCorpusDynamics, TachiomFresh
v2.0) and runs/<dataset>/dynamics-hfresh.txt (TestNFCorpusMuveraDynamics,
HFresh + MUVERA): the same protocol and seed on both, build, then three
rounds of deleting a random 20 % of the documents and reinserting them under
new doc ids. With --version v4.3 or v4.4 and --results results/x86-lsm, the
object-store runs of v4.3 and v4.4 (runs/<dataset>/v4.x/dynamics-mps128.txt),
which have no HFresh run beside them. Writes
plots/<encoder>/<version>/<corpus>-dynamics-{accuracy,qps,update-cost,size,dead-entries}.png
and tables/<dataset>-<prefix>dynamics.md (folder and prefix as in
plot_compare.py). Style, colours and markers are plot_compare.py's, so an
index looks the same on every figure.
"""
import os
import re

import matplotlib.pyplot as plt

import plot_compare as pc

TF2, HF, TF3, TF4, TF5 = pc.TF2, pc.HF, pc.TF3, pc.TF4, pc.TF5
TF12, TF13, TF20 = pc.TF12, pc.TF13, pc.TF20
# HFresh first, then the TachiomFresh versions, as in plot_compare.py
FILES = {HF: os.path.join(pc.RUNS, "dynamics-hfresh.txt"), TF2: os.path.join(pc.RUNS, "dynamics-v2.0.txt")}
# with --version v3.0 (the default once its runs exist, see plot_compare.py)
# v3.0 joins as a third index and the outputs get the v3.0- prefix
if pc.V3:
    FILES[TF3] = os.path.join(pc.RUNS, "v3.0", "dynamics.txt")
# --version v3.1 adds v3.1 the same way
if pc.V4:
    FILES[TF4] = os.path.join(pc.RUNS, "v3.1", "dynamics.txt")
if pc.V5:
    FILES[TF5] = os.path.join(pc.RUNS, "v3.2", "dynamics.txt")
# the object-store runs (results/x86-lsm, mps 128): v4.3 with
# --version v4.3, and v4.4 (the cleanup of dead entries) too with
# --version v4.4
if pc.V12:
    FILES[TF12] = os.path.join(pc.RUNS, "v4.3", "dynamics-mps128.txt")
if pc.V13:
    FILES[TF13] = os.path.join(pc.RUNS, "v4.4", "dynamics-mps128.txt")
# v7.0 with --version v7.0, the same run as v4.4's
if pc.V70:
    FILES[TF20] = os.path.join(pc.RUNS, "v7.0", "dynamics-mps128.txt")
# an index keeps its palette slot on every figure
SLOT = pc.SLOT
# only the versions run on this dataset
FILES = {name: path for name, path in FILES.items() if os.path.exists(path)}
# the TachiomFresh versions, which report the entries of deleted documents
TFS = [name for name in (TF2, TF3, TF4, TF5, TF12, TF13, TF20) if name in FILES]
STATES = [(0, "built"), (1, "deleted"), (1, "reinserted"), (2, "deleted"), (2, "reinserted"),
          (3, "deleted"), (3, "reinserted")]
STATE_LABELS = ["built", "delete 1", "reinsert 1", "delete 2", "reinsert 2", "delete 3", "reinsert 3"]
# go test -v prints a t.Log line as "    file.go:123: text" (the tests log
# one line per call); runs printed with fmt.Printf have the text at the line
# start. Both parse the same. Indentation alone is kept: some lines are
# indented and must not match.
LOG_PREFIX = re.compile(r"^\s*\S+\.go:\d+: ")


def unprefix(line):
    return LOG_PREFIX.sub("", line, count=1)


CELL = re.compile(r"cell index=\S+ (.*) recall@10=([\d.]+) qps=([\d.]+) mean_ms=([\d.]+)")
DYN = re.compile(r"dynamics (.*)")
HEAD = re.compile(r"corpus: (\d+) docs.*\((\d+) docs per round\)")
# the cell followed across the states: each index's fastest one that reaches
# this recall@10 on the fresh build (its highest-recall one if none does)
TARGET = 0.95


def parse(path):
    run = {"cells": {}, "stats": {}, "times": {}}
    for line in open(path):
        line = unprefix(line)
        m = HEAD.match(line)
        if m:
            run["docs"], run["per_round"] = int(m.group(1)), int(m.group(2))
            continue
        m = CELL.match(line)
        if m:
            p = dict(kv.split("=") for kv in m.group(1).split())
            state = (int(p.pop("round")), p.pop("phase"))
            p.pop("maxPostingSize")
            run["cells"].setdefault(tuple(sorted(p.items())), {})[state] = (float(m.group(2)), float(m.group(3)))
            continue
        m = DYN.match(line)
        if m:
            f = dict(kv.split("=") for kv in m.group(1).split() if "=" in kv)
            run["mps"] = f.get("maxPostingSize", run.get("mps"))
            if "phase" in f:
                run["stats"][(int(f["round"]), f["phase"])] = f
            for key in ("build_s", "delete_s", "reinsert_s"):
                if key in f:
                    run["times"].setdefault(key, []).append(float(f[key]))
    return run


def pick(run):
    built = {cell: states[(0, "built")] for cell, states in run["cells"].items()}
    ok = [cell for cell, (r, _) in built.items() if r >= TARGET]
    if ok:
        return max(ok, key=lambda cell: built[cell][1])
    return max(built, key=lambda cell: built[cell][0])


def cell_label(name, run, cell):
    p = dict(cell)
    if name == HF:
        return f"{name} (mps={run['mps']}, probe={p['searchProbe']}, rescore={p['rescoreLimit']})"
    # pc.LEGEND: the version with what it does (see plot_compare.py)
    return f"{pc.LEGEND[name]}, mps={run['mps']}, kC={p['kCentroids']}, kDocs={p['kDocsToScore']}"


def line(ax, name, ys, label):
    colour, marker = pc.STYLE['palette'][SLOT[name]], pc.STYLE['markers'][SLOT[name]]
    xs = range(len(ys))
    ax.plot(xs, ys, color=colour, linewidth=pc.STYLE['line_width'], alpha=0.85)
    ax.scatter(xs, ys, color=colour, marker=marker, s=pc.STYLE['marker_size'] ** 2,
               edgecolor='black', linewidth=0.5, label=label, zorder=3)


def states_axis(ax):
    ax.set_xticks(range(len(STATE_LABELS)))
    ax.set_xticklabels(STATE_LABELS, rotation=20, ha="right")


def main():
    runs = {name: parse(path) for name, path in FILES.items() if os.path.exists(path)}
    chosen = {name: pick(run) for name, run in runs.items()}
    out = [f"Update benchmark, {pc.DATASETS[pc.DATASET]}: build, then three rounds of deleting a random 20 % "
           "of the documents and reinserting them under new doc ids. Searches run between the batches.\n"]

    # --- accuracy and QPS of one cell per index across the states ----------
    for col, fname, ylabel in ((0, "dynamics-accuracy", pc.tex("Accuracy@10 (%)")),
                               (1, "dynamics-qps", "QPS (one query at a time, sequential)")):
        fig, ax = plt.subplots(figsize=(11.5, 6.0))
        for name, run in runs.items():
            ys = [run["cells"][chosen[name]][s][col] * (100 if col == 0 else 1) for s in STATES]
            line(ax, name, ys, pc.tex(cell_label(name, run, chosen[name])))
        states_axis(ax)
        ax.set_ylabel(ylabel)
        if col == 1:
            ax.set_ylim(0, None)
        # above the axes: inside, it covers points once three indexes share
        # the figure
        pc.legend(ax, loc="lower center", bbox_to_anchor=(0.5, 1.01), ncol=1, fontsize=13)
        fig.tight_layout()
        pc.save(fig, fname)

    # --- update cost: documents per second, per operation ------------------
    fig, ax = plt.subplots(figsize=pc.STYLE['figure_size'])
    ops = [("build", "build_s", "docs"), ("delete", "delete_s", "per_round"), ("reinsert", "reinsert_s", "per_round")]
    width = 0.8 / max(1, len(runs))
    rates = {}
    for idx, (name, run) in enumerate(runs.items()):
        values = []
        for _, key, count in ops:
            times = run["times"][key]
            values.append(run[count] / (sum(times) / len(times)))
        rates[name] = values
        xs = [i + (idx - (len(runs) - 1) / 2) * width for i in range(len(ops))]
        ax.bar(xs, values, width=width, color=pc.STYLE['palette'][SLOT[name]], edgecolor='black', linewidth=0.8,
               hatch=pc.STYLE['hatches'][SLOT[name]], label=pc.tex(f"{pc.LEGEND[name]}, mps={run['mps']}"))
        for x, v in zip(xs, values):
            ax.text(x, v, f"{v:,.0f}", ha="center", va="bottom", fontsize=11)
    ax.set_yscale("log")
    ax.set_xticks(range(len(ops)))
    ax.set_xticklabels([op for op, _, _ in ops])
    ax.set_ylabel("Documents per second (log scale)")
    # headroom for the legend above the tallest bars' labels
    ax.set_ylim(None, ax.get_ylim()[1] * 60)
    pc.legend(ax, loc="upper left", fontsize=13)
    fig.tight_layout()
    pc.save(fig, "dynamics-update-cost")

    # --- index size across the states: centroids relative to the fresh
    # build, and TachiomFresh's entries of deleted documents still in postings
    fig, ax = plt.subplots(figsize=(11.5, 6.0))
    for name, run in runs.items():
        base = int(run["stats"][(0, "built")]["centroids"])
        line(ax, name, [100 * int(run["stats"][s]["centroids"]) / base for s in STATES], pc.tex(f"{pc.LEGEND[name]}: centroids"))
    states_axis(ax)
    ax.set_ylabel(pc.tex("Centroids, fresh build = 100 %"))
    pc.legend(ax, loc="best", fontsize=13)
    fig.tight_layout()
    pc.save(fig, "dynamics-size")

    # --- entries of deleted documents still in the postings. Its own figure:
    # a second y axis on the centroid figure reads badly with more than one
    # TachiomFresh version
    fig, ax = plt.subplots(figsize=(11.5, 6.0))
    for name in TFS:
        if name in runs:
            stats = runs[name]["stats"]
            line(ax, name, [100 * int(stats[s]["dead_entries"]) / int(stats[s]["entries"]) for s in STATES], pc.tex(pc.LEGEND[name]))
            # when the searches of a state trigger cleanups, the after-search
            # lines report the dead entries too (dashed, the state's searches
            # done); runs without cleanups have no such field
            after = [stats.get((r, f"{p}-after-search"), {}) for r, p in STATES]
            if all("dead_entries" in a for a in after):
                ys = [100 * int(a["dead_entries"]) / int(a["entries"]) for a in after]
                ax.plot(range(len(ys)), ys, color=pc.STYLE['palette'][SLOT[name]], linewidth=pc.STYLE['line_width'],
                        alpha=0.85, linestyle="--", label=pc.tex(f"{pc.LEGEND[name]}, after the searches"))
    states_axis(ax)
    ax.set_ylabel(pc.tex("Posting entries of deleted documents (%)"))
    ax.set_ylim(0, None)
    pc.legend(ax, loc="best", fontsize=13)
    fig.tight_layout()
    pc.save(fig, "dynamics-dead-entries")

    # --- table --------------------------------------------------------------
    for name, run in runs.items():
        out.append(f"\n{cell_label(name, run, chosen[name])}:\n")
        out.append("| state | recall@10 | QPS | centroids | dead entries |\n|---|---|---|---|---|")
        for state, label in zip(STATES, STATE_LABELS):
            r, q = run["cells"][chosen[name]][state]
            st = run["stats"][state]
            dead = f"{int(st['dead_entries']):,} of {int(st['entries']):,}" if "dead_entries" in st else "not reported"
            out.append(f"| {label} | {r:.4f} | {q:.1f} | {int(st['centroids']):,} | {dead} |")
        t = run["times"]
        out.append(f"\nbuild {t['build_s'][0]:.1f} s for {run['docs']:,} docs; per round of {run['per_round']:,} docs: "
                   f"delete {', '.join(f'{x:.3f}' for x in t['delete_s'])} s, "
                   f"reinsert {', '.join(f'{x:.1f}' for x in t['reinsert_s'])} s "
                   f"({', '.join(f'{v:,.0f}' for v in rates[name])} docs/s for build, delete, reinsert).")
    open(os.path.join(pc.TABLES, f"{pc.DATASET}-{pc.PREFIX}dynamics.md"), "w").write("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
