# The passes of the control of the start of the 1M curve: from the grow.json
# of each (def1m/control/<pass>/grow.json), the inserts a second, the CPU of
# the process and what the index counts per insert.
#
# Usage: python3 -I -B control.py <control dir> [md]
import json, os, sys

root = sys.argv[1]
md = len(sys.argv) > 2 and sys.argv[2] == "md"
names = ["a8-1", "a8-2", "b8-1", "b8-2", "a1", "b1"]
rows = []
for name in names:
    path = os.path.join(root, name, "grow.json")
    if not os.path.exists(path):
        continue
    g = json.load(open(path))
    st, last = g["stats"], g["stretches"][-1]
    n = g["inserted"] - g["base"]
    ins = st["inserted"]
    first = g["stretches"][0]
    rows.append({
        "pass": name, "kind": "dense" if name.startswith("a") else "spread", "callers": g["callers"],
        "keys": g["meta"]["keys"], "s": "%.1f" % g["seconds"], "ins/s": "%.0f" % (n / g["seconds"]),
        "first stretch": "%.0f" % first["perSecond"],
        "ms CPU/ins": "%.2f" % ((last["rusage"]["userNs"] + last["rusage"]["systemNs"]) / 1e6 / n),
        "cores": "%.2f" % ((last["rusage"]["userNs"] + last["rusage"]["systemNs"]) / 1e9 / last["seconds"]),
        "plan search ms": "%.2f" % (st["searchNs"] / 1e6 / ins), "assign ms": "%.3f" % (st["assignNs"] / 1e6 / ins),
        "loads": "%.2f" % (st["searchLoads"] / ins), "rounds": "%.2f" % (st["searchRounds"] / ins),
        "rows": "%.1f" % (st["searchRows"]["n"] / ins), "rows MB": "%.2f" % (st["searchRows"]["bytes"] / 1e6 / ins),
        "wr KB/ins": "%.1f" % (last["rusage"]["diskBytesWritten"] / 1e3 / n),
        "postings": last["livePostings"], "splits": st["splits"], "numGC": last["numGC"],
        "heap GB max": "%.2f" % max(s["heapInuse"] / 1e9 for s in g["stretches"]), "done": g.get("done"),
    })
if not rows:
    sys.exit("no pass yet")
cols = list(rows[0].keys())
if md:
    print("| " + " | ".join(cols) + " |")
    print("|---|" + "---:|" * (len(cols) - 1))
    for r in rows:
        print("| " + " | ".join(str(r[c]) for c in cols) + " |")
else:
    width = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    print("  ".join(c.rjust(width[c]) for c in cols))
    for r in rows:
        print("  ".join(str(r[c]).rjust(width[c]) for c in cols))
