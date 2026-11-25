# The none cutoff by the pre-registered rule (bench-report.md, 2026-11-24
# 21:50): over the filters at random drawn by count, the number of allowed
# where the direct read stops doing fewer reads than the index does with none
# (computed) at 0.95, by linear interpolation between the two counts that
# bracket it; no extrapolation outside the counts measured. Beside it, the
# same crossover against the reads of the index with its codes in memory, and
# by bytes. The filters of the cache: which side of the cutoff they fall on,
# and whether that is the side that reads less.
import json
import sys

d = json.load(open(sys.argv[1]))
rows = []
print("%-14s %7s | %9s %9s %7s | %6s %5s %9s %9s %9s %9s | %s" % (
    "filter", "allowed", "direct", "MB", "recall", "ef95", "rec", "routing", "none*", "MB rt", "loads", "cheaper by reads (none*)"))
for f in d["filters"]:
    dr = f["direct"]
    p95 = next((p for p in f["index"] if abs(p["target"] - 0.95) < 1e-9), None)
    if p95 is None:
        print("%-14s %7d | %9.1f %9.2f %7.4f | 0.95 not reached" % (f["name"], f["allowed"], dr["reads"], dr["bytes"] / 2**20, dr["recall"]))
        continue
    cheaper = "direct" if dr["reads"] < p95["noneReadsComputed"] else "index"
    ratio = max(dr["reads"], p95["noneReadsComputed"]) / min(dr["reads"], p95["noneReadsComputed"])
    print("%-14s %7d | %9.1f %9.2f %7.4f | %6d %5.3f %9.1f %9.1f %9.2f %9.1f | %s %.2fx" % (
        f["name"], f["allowed"], dr["reads"], dr["bytes"] / 2**20, dr["recall"], p95["ef"], p95["recall"],
        p95["routingReads"], p95["noneReadsComputed"], p95["routingBytes"] / 2**20, p95["loads"], cheaper, ratio))
    rows.append((f, dr, p95))


def crossover(key, direct_key="reads"):
    pts = sorted((f["allowed"], dr[direct_key], p[key]) for f, dr, p in rows if f["drawn"])
    for (a1, d1, i1), (a2, d2, i2) in zip(pts, pts[1:]):
        g1, g2 = d1 - i1, d2 - i2
        if g1 < 0 <= g2:
            x = a1 + (a2 - a1) * (-g1) / (g2 - g1)
            return x, (a1, a2)
    if pts and pts[0][1] >= pts[0][2]:
        return None, "the direct read reads as much or more already at %d" % pts[0][0]
    return None, "the direct read reads less up to %d, the largest count measured" % pts[-1][0]


for label, key, dk in [("reads, against the index with none (computed)", "noneReadsComputed", "reads"),
                       ("reads, against the index with its codes in memory", "routingReads", "reads"),
                       ("bytes, against the index with its codes in memory", "routingBytes", "bytes")]:
    x, why = crossover(key, dk)
    if x is None:
        print("crossover by %s: none inside the counts measured: %s" % (label, why))
    else:
        print("crossover by %s: %.0f allowed (interpolated between %d and %d)" % (label, x, why[0], why[1]))

x, _ = crossover("noneReadsComputed")
if x is not None:
    print("\nthe cutoff, rounded to hundreds: %d" % (round(x / 100) * 100))
    for f, dr, p in rows:
        if f["drawn"]:
            continue
        side = "direct" if f["allowed"] < x else "index"
        best = "direct" if dr["reads"] < p["noneReadsComputed"] else "index"
        print("  %-14s %6d allowed -> %s; reads less: %s%s" % (f["name"], f["allowed"], side, best,
                                                              "" if side == best else "  (WRONG SIDE)"))
