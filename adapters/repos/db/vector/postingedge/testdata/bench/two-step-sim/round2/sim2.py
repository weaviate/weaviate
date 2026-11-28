# The tables of the simulation of the insert in two steps, both rounds, from
# the result.json files TestSimTwoStep writes: the coverage, the spread of the
# most recent vectors, and every scenario next to the index as it is (the
# scenario "base" of its own file, or for the deletes the scenario "del" of
# the same share).
#
# Usage: python3 -I -B sim2.py <result.json> [<result.json> ...]
import json, sys


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


for path in sys.argv[1:]:
    d = json.load(open(path))
    print("== %s\nindex: %d vectors, %d live postings, rescore %d, %d queries, dense %s" % (
        path, d["meta"]["live"], d["livePostings"], d["rescore"], d["queries"], d["dense"]))
    if "coverage" in d:
        print("\ncoverage: %d edges of %d vectors that have edges" % (d["coverageEdges"], d["coverageVectorsWithEdges"]))
        print("group | by centroids: edges, mean a vector, vectors with 90 %, with all | by edges: the same")
        for c in d["coverage"]:
            a, b = c["byCentroids"], c["byEdges"]
            print("%5d | %5.1f %5.1f %5.1f %5.1f | %5.1f %5.1f %5.1f %5.1f" % (
                c["group"], 100 * a["edges"], 100 * a["meanPerVector"], 100 * a["vectorsWith90"], 100 * a["vectorsWithAll"],
                100 * b["edges"], 100 * b["meanPerVector"], 100 * b["vectorsWith90"], 100 * b["vectorsWithAll"]))
    if "recentSpread" in d:
        print("\nthe share of the members of a posting that are of the set, over the postings:")
        print("set | vectors | mean | min p1 p10 p25 p50 p75 p90 p99 max | postings with none | with twice the share (and "
              "the part of the set in them) | edges out, edges in a vector")
        for row in d["recentSpread"]:
            for kind in ("recent", "random"):
                r = row[kind]
                print("%2.0f %% %-6s | %8d | %.3f | %s | %4d | %4d (%.1f %%) | %.2f, %.2f" % (
                    100 * row["share"], kind, r["vectors"], r["meanShareOfAPosting"],
                    " ".join("%.3f" % r[k] for k in ("min", "p1", "p10", "p25", "p50", "p75", "p90", "p99", "max")),
                    r["postingsWithNone"], r["postingsWithTwiceTheShare"], 100 * r["ofTheSetInThose"],
                    r["outEdgesAVector"], r["inEdgesAVector"]))

    scenarios = {s["name"]: s for s in d.get("scenarios", [])}
    base = scenarios.get("base")
    live = d["meta"]["live"]
    print("\nscenario | edges a vector | none | taken away: out, in | 0.95: ef loads (vs ref) | 0.99: ef loads (vs ref) | "
          "ef 20: recall loads | ef 160: recall loads | provisional: companions, endpoints by code, edges, us of CPU a "
          "vector, left with none | events: connected each, loads, scored, centroid distances, exact, distinct exact, own "
          "edges, inverse, CPU ms a vector | seconds")
    for name, s in scenarios.items():
        ref = base
        if name.startswith("delra"):
            ref = scenarios.get("del:" + name.split(":")[1], base)
        cells = [name, "%.2f" % (s["edges"] / (live * (1 - s.get("removedShare", 0)))), str(s["vectorsWithNoEdge"]),
                 "%d, %d" % (s.get("ownEdgesTakenAway", 0), s.get("inverseEdgesTakenAway", 0))]
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
        v = s.get("provisional")
        if v and v["vectors"]:
            n = v["vectors"]
            cells.append("%.0f, %.0f, %.2f, %.1f, %d, in %.2f, notices %.2f (%d hold some, most %d)" % (
                v["companionsScoredByCode"] / n, v["endpointsScoredByCode"] / n, v["edges"] / n, 1e6 * v["cpuSeconds"] / n,
                v["vectorsLeftWithNone"], v.get("inverseEdges", 0) / n, v.get("notices", 0) / n,
                v.get("vectorsWithNotices", 0), v.get("mostNoticesOnAVector", 0)))
        else:
            cells.append("")
        c = s.get("events")
        if c and c["vectorsConnected"]:
            n = c["vectorsConnected"]
            cells.append("%.1f, %.2f, %.0f, %.0f, %.1f, %.1f, %.2f, %.2f, %.3f" % (
                c["connectedPerEvent"], c["postingsLoaded"] / n, c["candidatesScoredByCode"] / n,
                c.get("centroidDistances", 0) / n, c["exactDistances"] / n, c["exactVectorsDistinctPerEvent"] / n,
                c["ownEdges"] / n, c["inverseEdges"] / n, 1e3 * c["cpuSeconds"] / n))
        else:
            cells.append("")
        cells.append("%.0f s" % s["seconds"])
        print("  |  ".join(cells))
    print()
