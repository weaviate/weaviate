# The tables by doc id over the 10M index, before and after: every round of
# every measure, the median of each binary, how far apart the rounds of one
# binary are, and what the 3 % of the rule says.
# Usage: python3 -I -B tables10m.py <tb10m dir>
import json, os, statistics, sys

root = sys.argv[1]


def load(name):
    path = os.path.join(root, name, "result.json")
    return json.load(open(path)) if os.path.exists(path) else None


def search(name):
    r = load(name)
    if r is None:
        return None
    p, n = r["plans"], r["plans"]["plans"]
    ru = {k: r["rusageAfter"][k] - r["rusageBefore"][k] for k in r["rusageAfter"]}
    return {
        "visit ms": p["searchVisitNs"] / 1e6 / n, "plan ms": (p["assignNs"] + p["searchNs"]) / 1e6 / n,
        "fetch ms": p["searchFetchNs"] / 1e6 / n, "rescore ms": p["searchRescoreNs"] / 1e6 / n,
        "cpu ms": (ru["userNs"] + ru["systemNs"]) / 1e6 / n, "user ms": ru["userNs"] / 1e6 / n,
        "Minstr": ru["instructions"] / 1e6 / n, "Mcyc": ru["cycles"] / 1e6 / n, "rd KB": ru["diskBytesRead"] / 1e3 / n,
        "per s": n / r["seconds"],
        "counts": (p["searchLoads"], p["searchRounds"], p["searchRows"], p["searchRowBytes"], p["searchMembers"],
                   p["searchRescored"]),
    }


def inserts(name):
    r = load(name)
    if r is None:
        return None
    a, b = r["stretches"][0], r["stretches"][-1]
    n, secs = b["inserted"] - a["inserted"], b["seconds"] - a["seconds"]
    ru = {k: b["rusage"][k] - a["rusage"][k] for k in b["rusage"]}
    s = lambda k: b["stats"][k] - a["stats"].get(k, 0)
    return {
        "ins/s": n / secs, "cpu ms": (ru["userNs"] + ru["systemNs"]) / 1e6 / n, "user ms": ru["userNs"] / 1e6 / n,
        "plan ms": (s("assignNs") + s("searchNs")) / 1e6 / n, "Minstr": ru["instructions"] / 1e6 / n,
        "rd KB": ru["diskBytesRead"] / 1e3 / n, "loads": s("searchLoads") / n, "cores": (ru["userNs"] + ru["systemNs"]) / 1e9 / secs,
    }


def show(title, kind, rounds, keys, verdict_key, worse_is_higher):
    print("## " + title)
    med = {}
    for b in ("before", "after"):
        rows = [kind(rounds % (b, r)) for r in (1, 2, 3, 4, 5)]
        rows = [x for x in rows if x]
        if not rows:
            continue
        for k in keys:
            vals = [x[k] for x in rows]
            m = statistics.median(vals)
            spread = (max(vals) - min(vals)) / m * 100
            med[(b, k)] = (m, spread)
            print("  %-6s %-10s rounds %s  median %.3f  spread %.1f %%%s" % (
                b, k, "  ".join("%.3f" % v for v in vals), m, spread, "  MARKED" if spread >= 3 and k == verdict_key else ""))
        if kind is search:
            print("  %-6s counts %s" % (b, sorted({x["counts"] for x in rows})))
    for k in keys:
        if ("before", k) in med and ("after", k) in med:
            a, b = med[("before", k)][0], med[("after", k)][0]
            d = (b - a) / a * 100
            flag = ""
            if k == verdict_key:
                bad = d >= 3 if worse_is_higher else d <= -3
                flag = "  <- the rule: " + ("DOES NOT MEET" if bad else "meets")
            print("  after against before, %-10s %+.2f %%%s" % (k, d, flag))


show("the search of an insert alone, 1 thread", search, "search-t1-%s-r%d",
     ["visit ms", "plan ms", "fetch ms", "rescore ms", "cpu ms", "user ms", "Minstr", "Mcyc", "rd KB", "per s"], "visit ms", True)
show("20 000 inserts, 8 callers", inserts, "inserts-c8-%s-r%d",
     ["ins/s", "cpu ms", "user ms", "plan ms", "Minstr", "rd KB", "loads", "cores"], "ins/s", False)
show("20 000 inserts, 1 caller", inserts, "inserts-c1-%s-r%d",
     ["ins/s", "cpu ms", "user ms", "plan ms", "Minstr", "rd KB", "loads", "cores"], "ins/s", False)
for b in ("before", "after"):
    p = os.path.join(root, "heap-" + b, "heap.json")
    if os.path.exists(p):
        h = json.load(open(p))
        print("heap %-6s alloc %.1f MB, in use %.1f MB, opened in %.1f s, keys %d/%d, pending heap %.1f MB" % (
            b, h["heapAlloc"] / 1e6, h["heapInuse"] / 1e6, h["openSeconds"], h["liveSlots"], h["deletedLen"], h["pendingHeap"] / 1e6))
