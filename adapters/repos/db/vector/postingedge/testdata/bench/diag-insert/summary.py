# What every pass of the diagnostic of the inserts measured, by insert: one
# line a pass, from its result.json (bench_diag_test.go) and the monitor from
# outside. A pass is the difference between its last stretch and its first.
# The times of a caller are per insert: over all the callers, what they spent
# in a part, between the inserts of the pass.
#
# Usage: python3 -I -B summary.py <runs dir of a size> [tsv]
import json, os, sys

runs = sys.argv[1]
tsv = len(sys.argv) > 2 and sys.argv[2] == "tsv"

CLOCKS = ["admitWaitNs", "admitNs", "enterWaitNs", "readWaitNs", "plansNs", "leaveWaitNs", "drainWaitNs",
          "applyLockWaitNs", "applyNs", "applyAfterNs", "appliedWaitNs"]


def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def delta(b, a):
    """The difference of two snapshots of counters, nested ones too."""
    out = {}
    for k, v in b.items():
        if num(v):
            out[k] = v - (a.get(k, 0) if a else 0)
        elif isinstance(v, dict):
            out[k] = delta(v, a.get(k, {}) if a else {})
    return out


def monitor(path, t0, t1, n):
    """Of the monitor from outside, between two instants of the wall clock."""
    if not os.path.exists(path):
        return {}
    ms = [json.loads(l) for l in open(path) if l.strip()]
    ms = [m for m in ms if not m.get("gone") and m.get("disk") and m.get("task")]
    inside = [m for m in ms if t0 <= m["t"] <= t1]
    if len(inside) < 2:
        return {}
    a, b = inside[0], inside[-1]
    dt = b["t"] - a["t"]
    d = lambda k: b["disk"][k] - a["disk"][k]
    u32 = lambda k: (b["task"][k] - a["task"][k]) % 2**32
    mean = lambda f: sum(f(m) for m in inside) / len(inside)
    page = b["vm"]["pageSize"]
    share = dt / (t1 - t0)  # the part of the pass the monitor saw
    return {
        "disk rd ops/ins": d("readOps") / (n * share), "disk rd ms/op": d("readNs") / 1e6 / max(1, d("readOps")),
        "disk rd KB/op": d("readBytes") / 1e3 / max(1, d("readOps")),
        "disk wr ops/ins": d("writeOps") / (n * share), "disk wr ms/op": d("writeNs") / 1e6 / max(1, d("writeOps")),
        "task faults/ins": u32("faults") / (n * share), "task csw/ins": u32("csw") / (n * share),
        "task sysc/ins": u32("syscalls_unix") / (n * share),
        "thr R": mean(lambda m: m["threads"].get("R", 0)), "thr U": mean(lambda m: m["threads"].get("U", 0)),
        "thr S": mean(lambda m: m["threads"].get("S", 0)),
        "cache GB": mean(lambda m: m["vm"]["File-backed pages"]) * page / 1e9,
        "monitor samples": len(inside),
    }


def one(name):
    path = os.path.join(runs, name, "result.json")
    if not os.path.exists(path):
        return None
    r = json.load(open(path))
    row = {"pass": name}
    if "stretches" not in r:  # TestDiagSearch
        n, threads, secs = r["plans"]["plans"], r["threads"], r["seconds"]
        p = r["plans"]
        ru = delta(r["rusageAfter"], r["rusageBefore"])
        rt = delta(r["runtimeAfter"], r["runtimeBefore"])
        st = delta(r["storeAfter"], r["storeBefore"])
        row.update({
            "callers": threads, "n": n, "ins/s": n / secs, "caller ms": threads * secs * 1e3 / n,
            "plan ms": (p["assignNs"] + p["searchNs"]) / 1e6 / n, "assign ms": p["assignNs"] / 1e6 / n,
            "search ms": p["searchNs"] / 1e6 / n, "fetch ms": p["searchFetchNs"] / 1e6 / n,
            "visit ms": p["searchVisitNs"] / 1e6 / n, "rescore ms": p["searchRescoreNs"] / 1e6 / n,
            "member rd ms": p["searchMemberReadNs"] / 1e6 / n, "edge rd ms": p["searchEdgeReadNs"] / 1e6 / n,
            "loads": p["searchLoads"] / n, "rounds": p["searchRounds"] / n, "rows": p["searchRows"] / n,
            "rows MB": p["searchRowBytes"] / 1e6 / n, "members": p["searchMembers"] / n,
            "vectors": p["searchVectorReads"] / n,
        })
        t0 = t1 = None
    else:
        a, b = r["stretches"][0], r["stretches"][-1]
        n = b["inserted"] - a["inserted"]
        callers, secs = r["callers"], b["seconds"] - a["seconds"]
        add, flush = b["addSeconds"] - a["addSeconds"], b["flushSeconds"] - a["flushSeconds"]
        ru, rt = delta(b["rusage"], a["rusage"]), delta(b["runtime"], a["runtime"])
        st, s = delta(b["store"], a["store"]), delta(b["stats"], a["stats"])
        rates = []
        for x, y in zip(r["stretches"], r["stretches"][1:]):
            if y["inserted"] > x["inserted"]:
                rates.append((y["inserted"] - x["inserted"]) / (y["seconds"] - x["seconds"]))
        row.update({
            "callers": callers, "n": n, "kind": r["kind"], "clocks": r["clocksOn"], "branch": r["diagnosticBranch"],
            "ins/s": n / secs, "ins/s first": rates[0] if rates else 0, "ins/s last": rates[-1] if rates else 0,
            "ins/s min": min(rates) if rates else 0, "ins/s max": max(rates) if rates else 0,
            "caller ms": callers * secs * 1e3 / n, "add ms": callers * add * 1e3 / n,
            "flush ms": callers * flush * 1e3 / n, "between ms": callers * (secs - add - flush) * 1e3 / n,
        })
        if b.get("clocks"):
            c = delta(b["clocks"], a.get("clocks") or {})
            for k in CLOCKS:
                row["c " + k[:-2]] = c[k] / 1e6 / n
            row["c total"] = c["totalNs"] / 1e6 / n
            # "caller ms" and "add ms" take every round for one of all the
            # callers, which the last round of a pass may not be: where they
            # differ from "c total", that one is the time of the callers.
            row["calls"] = c["calls"]
            row["c unnamed"] = (c["totalNs"] - sum(c[k] for k in CLOCKS)) / 1e6 / n
            row["plans/window"] = c["plans"] / max(1, c["windows"])
            row["plans/batch"] = c["batchPlans"] / max(1, c["batches"])
        g = lambda k: s.get(k, 0)
        gn = lambda k, f: s.get(k, {}).get(f, 0)
        row.update({
            "assign ms": g("assignNs") / 1e6 / n, "search ms": g("searchNs") / 1e6 / n,
            "fetch ms": g("searchFetchNs") / 1e6 / n, "visit ms": g("searchVisitNs") / 1e6 / n,
            "rescore ms": g("searchRescoreNs") / 1e6 / n, "member rd ms": g("searchMemberReadNs") / 1e6 / n,
            "edge rd ms": g("searchEdgeReadNs") / 1e6 / n,
            "write ms": g("writeNs") / 1e6 / n, "inverse ms": g("inverseNs") / 1e6 / n,
            "split ms": g("splitNs") / 1e6 / n, "row ms": g("rowNs") / 1e6 / n,
            "journal ms": g("journalNs") / 1e6 / n, "hand ms": g("handNs") / 1e6 / n,
            "syncs": g("syncs"), "sync ms": (g("syncGraphNs") + g("syncWalNs") + g("syncMetaNs") + g("syncJournalNs")) / 1e6 / n,
            "sync graph ms": g("syncGraphNs") / 1e6 / n, "sync wal ms": g("syncWalNs") / 1e6 / n,
            "sync meta ms": g("syncMetaNs") / 1e6 / n, "sync journal ms": g("syncJournalNs") / 1e6 / n,
            "loads": g("searchLoads") / n, "rounds": g("searchRounds") / n, "rows": gn("searchRows", "n") / n,
            "rows MB": gn("searchRows", "bytes") / 1e6 / n, "members": g("searchMembers") / n,
            "vectors": gn("searchVectors", "n") / n, "inv checks": g("inverseChecks") / n,
            "inv edges": g("inverseEdges") / n, "inv list reads": g("inverseListReads") / n,
            "splits/1000": 1000 * g("splits") / n, "row writes": gn("rowWrites", "n") / n,
            "row writes KB": gn("rowWrites", "bytes") / 1e3 / n, "row writes budget": g("rowWritesBudget") / n,
            "journal KB": (b["journal"]["bytes"] - a["journal"]["bytes"]) / 1e3 / n,
            "journal records": (b["journal"]["n"] - a["journal"]["n"]),
            "postings": b["postings"], "live": b["live"], "heap GB": b["heapInuse"] / 1e9,
        })
        if g("workPlans"):
            w = g("workPlans")
            row.update({"w scanned": g("workVectorsScanned") / w, "w edges": g("workEdgesFollowed") / w,
                        "w in loaded": g("workEndpointsInLoaded") / w, "w endpoints": g("workEndpointsScored") / w})
        for k, v in sorted((b.get("filesGrownByClass") or {}).items()):
            row["KB " + k] = (v - (a.get("filesGrownByClass") or {}).get(k, 0)) / 1e3 / n
        row["segments start"] = sum(a["segments"].values())
        row["segments end"] = sum(b["segments"].values())
        row["segments"] = " ".join("%s:%d>%d" % (k.replace("postingedge_", "").replace("_main", ""), a["segments"].get(k, 0), v)
                                   for k, v in sorted(b["segments"].items()))
        t1 = os.path.getmtime(path)
        # The file is written at the end of the pass, right after its last stretch.
        t0 = t1 - secs
    cpu = ru["userNs"] + ru["systemNs"]
    row.update({
        "cores": cpu / 1e9 / secs, "user ms": ru["userNs"] / 1e6 / n, "sys ms": ru["systemNs"] / 1e6 / n,
        "ready": (ru["runnableNs"] - cpu) / 1e9 / secs, "Minstr": ru["instructions"] / 1e6 / n,
        "Mcyc": ru["cycles"] / 1e6 / n, "IPC": ru["instructions"] / max(1, ru["cycles"]),
        "rd KB": ru["diskBytesRead"] / 1e3 / n, "wr KB": ru["diskBytesWritten"] / 1e3 / n,
        "pageins": ru["pageins"] / n, "major faults": ru["majorFaults"] / n, "minor faults": ru["minorFaults"] / n,
        "vol csw": ru["voluntarySwitches"] / n, "invol csw": ru["involuntarySwitches"] / n,
        "mutex wait ms": rt.get("/sync/mutex/wait/total:seconds", 0) * 1e3 / n,
        "sched lat ms": rt.get("/sched/latencies:seconds:sum", 0) * 1e3 / n,
        "sched events": rt.get("/sched/latencies:seconds:count", 0) / n,
        "sched over 1ms": rt.get("/sched/latencies:seconds:over1ms", 0) / n,
        "gc cpu ms": rt.get("/cpu/classes/gc/total:cpu-seconds", 0) * 1e3 / n,
        "gc assist ms": rt.get("/cpu/classes/gc/mark/assist:cpu-seconds", 0) * 1e3 / n,
        "go user cpu ms": rt.get("/cpu/classes/user:cpu-seconds", 0) * 1e3 / n,
        "go idle cores": rt.get("/cpu/classes/idle:cpu-seconds", 0) / secs,
        "gc cycles": rt.get("/gc/cycles/total:gc-cycles", 0), "alloc MB": rt.get("/gc/heap/allocs:bytes", 0) / 1e6 / n,
        "alloc objects": rt.get("/gc/heap/allocs:objects", 0) / n,
    })
    if st.get("reads"):
        row.update({"store reads": st["reads"] / n, "seg held/read": st["segmentsHeld"] / st["reads"],
                    "seg found/read": st["segmentsFound"] / st["reads"], "bloom false/read": st["bloomFalse"] / st["reads"]})
    if t0 is not None:
        row.update(monitor(os.path.join(runs, name, "monitor.jsonl"), t0, t1, n))
    return row


names = sorted(d for d in os.listdir(runs) if os.path.isdir(os.path.join(runs, d)))
rows = [r for r in (one(n) for n in names) if r]
cols = []
for r in rows:
    for k in r:
        if k not in cols:
            cols.append(k)


def fmt(v):
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, float):
        return "%.3f" % v if abs(v) < 100 else "%.1f" % v
    return str(v)


if tsv:
    print("\t".join(cols))
    for r in rows:
        print("\t".join(fmt(r[c]) if c in r else "" for c in cols))
else:
    for r in rows:
        print("== " + r["pass"])
        print("   " + "  ".join("%s=%s" % (c, fmt(r[c])) for c in cols if c in r and c != "pass"))
