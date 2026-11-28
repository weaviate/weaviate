# The passes of amendment 2 of the control (def1m/control3/<pass>): the
# harness of the measure at 100k with the code of today. From result.json,
# the inserts a second of the clock of the inserts and of the whole stretch,
# and what the index counts per insert; from run.log, the collections
# gctrace printed; from monitor.jsonl, the CPU of the process up to its last
# reading (which includes the base and the searches at the end).
#
# Usage: python3 -I -B control3.py <control3 dir>
import json, os, re, sys

root = sys.argv[1]
names = sorted((d for d in os.listdir(root) if os.path.exists(os.path.join(root, d, "result.json"))),
               key=lambda d: os.path.getmtime(os.path.join(root, d, "result.json")))
rows = []
for name in names:
    d = json.load(open(os.path.join(root, name, "result.json")))
    st, n = d["stats"], d["stats"]["inserted"]
    gcs = sum(1 for l in open(os.path.join(root, name, "run.log"), errors="replace") if re.match(r"gc \d+ @", l))
    stretch = d["stretches"][-1]
    row = {
        "pass": name, "ins/s (insert clock)": "%.0f" % d["insertsPerSecond"], "insert s": "%.1f" % d["insertSeconds"],
        "ins/s (whole stretch)": "%.0f" % stretch["perSecond"],
        "plan search ms": "%.2f" % (st["searchNs"] / 1e6 / n), "assign ms": "%.3f" % (st["assignNs"] / 1e6 / n),
        "loads": "%.2f" % (st["searchLoads"] / n), "rows": "%.1f" % (st["searchRows"]["n"] / n),
        "rows MB": "%.2f" % (st["searchRows"]["bytes"] / 1e6 / n), "journal records": d.get("journalRecords", ""),
        "gctrace lines": gcs, "GC/s over the stretch": "%.1f" % (gcs / stretch["seconds"]) if gcs else "",
        "points": "; ".join("ef %d: %.4f, %.2f loads" % (p["ef"], p["recall"], p["loads"])
                            for p in d["stages"][-1].get("points", [])),
    }
    mon = os.path.join(root, name, "monitor.jsonl")
    if os.path.exists(mon):
        m = [json.loads(l) for l in open(mon) if l.strip()]
        m = [x for x in m if not x.get("gone")]
        if m:
            row["process CPU s (monitor)"] = "%.0f" % ((m[-1]["rusage"]["user_time_ns"] + m[-1]["rusage"]["system_time_ns"]) / 1e9)
            row["max footprint GB"] = "%.2f" % (max(x["rusage"]["phys_footprint"] for x in m) / 1e9)
    rows.append(row)
if not rows:
    sys.exit("no pass yet")
for r in rows:
    print(r.pop("pass"))
    for k, v in r.items():
        print("   %-26s %s" % (k, v))
