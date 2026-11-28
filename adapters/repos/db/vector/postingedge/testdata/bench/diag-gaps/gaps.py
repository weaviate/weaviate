# The passes of the gaps of the diagnostic (diag/gaps/<size>/<pass>): the
# queries alone and next to the inserts, the degree, and the search of an
# insert with and without the marks of the deleted vectors.
#
# Usage: python3 -I -B gaps.py <gaps dir>
import json, os, sys

root = sys.argv[1]


def load(size, name):
    path = os.path.join(root, size, name, "result.json")
    return json.load(open(path)) if os.path.exists(path) else None


for size in ("100k", "10m"):
    print("==", size)
    point = os.path.join(root, size, "point.json")
    if os.path.exists(point):
        p = json.load(open(point))
        print("   0.95 at ef %d: recall %.4f, %.2f loads" % (p["ef"], p["recall"], p["loads"]))
    print("   %-5s %2s %2s %7s %8s | latency p50 %7s p95 %7s p99 %7s p99.9 %8s max | wait for the index p50 p99 p99.9 max, "
          "over 1 ms, over 10 ms | search p50 p99 | inserts/s | loads | GC" % ("pass", "q", "c", "queries", "QPS", "", "", "", ""))
    for name in ("q1", "q8", "i8", "qi1", "qi8"):
        d = load(size, name)
        if d is None:
            continue
        l, w, s = d["latency"], d["lockWait"], d["search"]
        n = max(d["queries"], 1)
        print("   %-5s %2d %2d %7d %8.1f | %7.3f %7.3f %7.3f %8.3f %8.1f | %.3f %.3f %.3f %.1f, %.2f %%, %.2f %% | %.3f %.3f | "
              "%7.1f | %.2f | %d" % (
                  name, d["queryThreads"], d["callers"], d["queries"], d["qps"], l["p50ms"], l["p95ms"], l["p99ms"],
                  l["p999ms"], l["maxms"], w["p50ms"], w["p99ms"], w["p999ms"], w["maxms"],
                  100 * d["lockWaitsOver1ms"] / n, 100 * d["lockWaitsOver10ms"] / n, s["p50ms"], s["p99ms"],
                  d.get("insertsPerSecond", 0), d["loadsPerQuery"], d["numGC"]))
        ca, cb = d.get("clocksAfter") or {}, d.get("clocksBefore") or {}
        if ca and d["callers"] > 0:
            delta = {k: ca[k] - cb.get(k, 0) for k in ca}
            batches, plans = max(delta.get("batches", 0), 1), max(delta.get("plans", 0), 1)
            took = d["seconds"]
            print("         per batch of %.1f plans: drain %.2f ms, the lock of the index for writing: wait %.2f ms, held "
                  "%.2f ms | %.1f batches a second, the index held for writing %.1f %% of the time, asked or held %.1f %%" % (
                      plans / batches, delta.get("drainWaitNs", 0) / 1e6 / batches,
                      delta.get("applyLockWaitNs", 0) / 1e6 / batches, delta.get("applyNs", 0) / 1e6 / batches,
                      batches / took, 100 * delta.get("applyNs", 0) / 1e9 / took,
                      100 * (delta.get("applyNs", 0) + delta.get("applyLockWaitNs", 0)) / 1e9 / took))
    d = load(size, "degree")
    if d is not None:
        hist = d["histogram"]
        total = sum(hist)
        cum, p50, p90 = 0, None, None
        for degree, count in enumerate(hist):
            cum += count
            if p50 is None and cum >= 0.5 * total:
                p50 = degree
            if p90 is None and cum >= 0.9 * total:
                p90 = degree
        print("   degree: %d postings, %d members (%.1f a posting), %d edges: %.2f a vector, p50 %d, p90 %d, most %d; %.1f %% "
              "of the members with none; %.1f %% of the edges to the same posting; %.0f distinct endpoints a posting" % (
                  d["postings"], d["members"], d["membersPerPosting"], d["edges"], d["edgesPerVector"], p50, p90,
                  d["maxDegree"], 100 * d["membersWithNoEdge"] / max(d["members"], 1), 100 * d["edgesToTheSamePosting"],
                  d["distinctEndpointsPerPosting"]))
        print("      histogram:", hist[:20])
    for name in ("search-marks-a", "search-nomarks-a", "search-marks-b", "search-nomarks-b"):
        d = load(size, name)
        if d is None:
            continue
        pl = d["plans"]
        n = max(pl["plans"], 1)
        print("   %-17s no marks %-5s: %d plans, %.3f ms a plan (search %.3f: fetch %.3f, visit %.3f, rescore %.3f) | "
              "%.0f members a plan, visit %.1f ns a member | %.2f loads" % (
                  name, d.get("noDeleted"), n, (pl["assignNs"] + pl["searchNs"]) / 1e6 / n, pl["searchNs"] / 1e6 / n,
                  pl["searchFetchNs"] / 1e6 / n, pl["searchVisitNs"] / 1e6 / n, pl["searchRescoreNs"] / 1e6 / n,
                  pl["searchMembers"] / n, pl["searchVisitNs"] / max(pl["searchMembers"], 1), pl["searchLoads"] / n))
