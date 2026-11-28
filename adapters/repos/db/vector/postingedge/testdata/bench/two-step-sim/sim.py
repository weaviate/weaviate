# The tables of the simulation of the insert in two steps, from the
# result.json TestSimTwoStep writes: the coverage, and every scenario next to
# the index as it is (the scenario "base", or for the deletes the scenario
# "del" of the same share).
#
# Usage: python3 -I -B sim.py <result.json> [md]
import json, sys

d = json.load(open(sys.argv[1]))
md = len(sys.argv) > 2 and sys.argv[2] == "md"
print("index: %d vectors, %d live postings, rescore %d, %d queries, dense %s" % (
    d["meta"]["live"], d["livePostings"], d["rescore"], d["queries"], d["dense"]))

if "coverage" in d:
    print("\ncoverage: %d edges of %d vectors that have edges" % (d["coverageEdges"], d["coverageVectorsWithEdges"]))
    print("group | by centroids: edges, mean a vector, vectors with 90 %, with all | by edges: the same")
    for c in d["coverage"]:
        a, b = c["byCentroids"], c["byEdges"]
        print("%5d | %5.1f %5.1f %5.1f %5.1f | %5.1f %5.1f %5.1f %5.1f" % (
            c["group"], 100 * a["edges"], 100 * a["meanPerVector"], 100 * a["vectorsWith90"], 100 * a["vectorsWithAll"],
            100 * b["edges"], 100 * b["meanPerVector"], 100 * b["vectorsWith90"], 100 * b["vectorsWithAll"]))

scenarios = {s["name"]: s for s in d.get("scenarios", [])}


def point(s, target):
    for p in s["search"]["points"]:
        if abs(p["target"] - target) < 1e-9:
            return p
    return None


def grid(s, ef):
    for p in s["search"]["grid"]:
        if p["ef"] == ef:
            return p
    return None


base = scenarios.get("base")
print("\nscenario | edges a vector | none | 0.95: ef loads (vs ref) | 0.99: ef loads (vs ref) | ef 20: recall loads | "
      "ef 160: recall loads | events: connected each, loads, scored, exact, distinct exact, own edges, inverse, CPU ms a vector")
live = d["meta"]["live"]
for name, s in scenarios.items():
    ref = base
    if name.startswith("delra"):
        ref = scenarios.get("del:" + name.split(":")[1], base)
    cells = [name, "%.2f" % (s["edges"] / (live * (1 - s.get("removedShare", 0)))), str(s["vectorsWithNoEdge"])]
    for target in (0.95, 0.99):
        p = point(s, target)
        r = point(ref, target) if ref else None
        if p is None:
            cells.append("not reached")
        elif r is None or ref is s:
            cells.append("%d %.2f" % (p["ef"], p["loads"]))
        else:
            cells.append("%d %.2f (%+.1f %%)" % (p["ef"], p["loads"], 100 * (p["loads"] / r["loads"] - 1)))
    for ef in (20, 160):
        p = grid(s, ef)
        cells.append("%.4f %.2f" % (p["recall"], p["loads"]))
    c = s.get("events")
    if c and c["vectorsConnected"]:
        n = c["vectorsConnected"]
        cells.append("%.1f, %.2f, %.0f, %.1f, %.1f, %.2f, %.2f, %.3f" % (
            c["connectedPerEvent"], c["postingsLoaded"] / n, c["candidatesScoredByCode"] / n, c["exactDistances"] / n,
            c["exactVectorsDistinctPerEvent"] / n, c["ownEdges"] / n, c["inverseEdges"] / n, 1e3 * c["cpuSeconds"] / n))
    else:
        cells.append("")
    cells.append("%.0f s" % s["seconds"])
    print((" | " if md else "  |  ").join(cells))
