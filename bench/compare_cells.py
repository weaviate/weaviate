#!/usr/bin/env python3
"""Per-cell comparison of two TachiomFresh frontier runs: a QPS gap is
judged by the per-cell ratios against the machine's run-to-run drift, never
by one cell.

    bench/compare_cells.py <base frontier.txt> <new frontier.txt> [--name-base v4.4 --name-new v4.5]

Prints, over the cells both runs have (same maxPostingSize, kCentroids,
kDocsToScore): the recall@10 delta (new - base) and the QPS ratio (new /
base), their median, quartiles and extremes, how many cells are higher and
lower, the same per maxPostingSize, the fastest cell of each run at
recall@10 >= 0.90, 0.95 and 0.99, and the build lines side by side.
"""
import argparse
import re
import statistics

ap = argparse.ArgumentParser()
ap.add_argument("base")
ap.add_argument("new")
ap.add_argument("--name-base", default="base")
ap.add_argument("--name-new", default="new")
args = ap.parse_args()

# go test -v prints a t.Log line as "    file.go:123: text" (the tests log
# one line per call); runs printed with fmt.Printf have the text at the line
# start. Both parse the same. Indentation alone is kept: some lines are
# indented and must not match.
LOG_PREFIX = re.compile(r"^\s*\S+\.go:\d+: ")


def unprefix(line):
    return LOG_PREFIX.sub("", line, count=1)


CELL = re.compile(r"^cell index=\S+ maxPostingSize=(\d+) kCentroids=(\d+) kDocsToScore=(\d+) recall@10=([\d.]+) qps=([\d.]+)")
BUILD = re.compile(r"^(build|types|after sweep) .*")


def load(path):
    cells, builds = {}, []
    for line in open(path):
        line = unprefix(line)
        m = CELL.match(line)
        if m:
            mps, kc, kd = (int(m.group(i)) for i in (1, 2, 3))
            cells[(mps, kc, kd)] = (float(m.group(4)), float(m.group(5)))
        elif BUILD.match(line):
            builds.append(line.rstrip()[:160])
    return cells, builds


base, bbuild = load(args.base)
new, nbuild = load(args.new)
common = sorted(set(base) & set(new))
print(f"{args.name_base}: {args.base} ({len(base)} cells)")
print(f"{args.name_new}:  {args.new} ({len(new)} cells)")
print(f"common cells: {len(common)}")


def summary(values, fmt):
    q = statistics.quantiles(values, n=4) if len(values) >= 4 else [min(values), statistics.median(values), max(values)]
    return f"median {fmt % statistics.median(values)}, quartiles {fmt % q[0]} / {fmt % q[-1]}, min {fmt % min(values)}, max {fmt % max(values)}"


def report(cells, label):
    dr = [new[c][0] - base[c][0] for c in cells]
    qr = [new[c][1] / base[c][1] for c in cells]
    print(f"  {label} ({len(cells)} cells)")
    print(f"    recall@10 delta ({args.name_new} - {args.name_base}): {summary(dr, '%+.4f')}; higher {sum(d > 0 for d in dr)}, equal {sum(d == 0 for d in dr)}, lower {sum(d < 0 for d in dr)}")
    print(f"    QPS ratio ({args.name_new} / {args.name_base}):       {summary(qr, '%.3f')}; faster {sum(r > 1 for r in qr)}, slower {sum(r < 1 for r in qr)}")


print()
report(common, "all")
for mps in sorted({c[0] for c in common}):
    report([c for c in common if c[0] == mps], f"maxPostingSize {mps}")

print()
print("fastest cell at recall@10 >= threshold (QPS, cell):")
for thr in (0.90, 0.95, 0.99):
    for name, cells in ((args.name_base, base), (args.name_new, new)):
        ok = [(v[1], c) for c, v in cells.items() if v[0] >= thr]
        best = max(ok) if ok else None
        print(f"  >= {thr:.2f} {name:>6s}: " + (f"{best[0]:.1f} QPS at mps {best[1][0]} kC {best[1][1]} kDocs {best[1][2]}" if best else "never"))

print()
print("cells (mps, kC, kDocs): recall base -> new (delta); qps base -> new (ratio)")
for c in common:
    b, n = base[c], new[c]
    print(f"  {c[0]:3d} {c[1]:3d} {c[2]:5d}: {b[0]:.4f} -> {n[0]:.4f} ({n[0] - b[0]:+.4f}); {b[1]:7.2f} -> {n[1]:7.2f} ({n[1] / b[1]:.3f})")

print()
print("build lines:")
for line in bbuild:
    print(f"  {args.name_base}: {line}")
for line in nbuild:
    print(f"  {args.name_new}:  {line}")
