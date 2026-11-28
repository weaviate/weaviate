# The gate of the insert in two steps (bench-report.md, "La puerta del insert
# en dos tiempos", pre-registered on 2026-11-28) over result.json files of
# TestSimTwoStep: of the scenarios that connect everything by events
# (po:100:D:G and pv:100:D:G:P), which stay within 5 % of the loads of the
# base of their file at 0.95 and within 10 % at 0.99, with no more CPU a
# vector than the limit given in ms; and the row of the provisional edges with
# 10 % pending.
#
# Usage: python3 -I -B gate.py <mode> <cpu limit ms> <result.json> [...]
#   table      every scenario of the gate, with its verdict
#   any        "yes" if a scenario that connects everything passes
#   best       the one that passes with the least CPU a vector
#   passers256 those that load 256 postings and pass, comma separated
#   evprov     the scenario evprov:10:16:... of the best
import json, sys

mode, limit, paths = sys.argv[1], float(sys.argv[2]), sys.argv[3:]


def point(s, target):
    for p in s["search"]["points"]:
        if abs(p["target"] - target) < 1e-9:
            return p
    return None


rows = []
for path in paths:
    try:
        d = json.load(open(path))
    except Exception:
        continue
    sc = {s["name"]: s for s in d.get("scenarios", [])}
    base = sc.get("base")
    if base is None:
        continue
    for name, s in sc.items():
        kind = name.split(":")[0]
        everything = kind in ("po", "pv") and name.split(":")[1] == "100"
        if not everything and name not in ("prov:10:16", "provi:10:16") and kind not in (
                "evprov", "proto", "protop", "protoi", "protoip"):
            continue
        deltas = []
        for target in (0.95, 0.99):
            p, r = point(s, target), point(base, target)
            deltas.append(None if p is None or r is None else p["loads"] / r["loads"] - 1)
        c = s.get("events")
        cpu = 1e3 * c["cpuSeconds"] / c["vectorsConnected"] if c and c["vectorsConnected"] else 0.0
        loads = deltas[0] is not None and deltas[1] is not None and deltas[0] <= 0.05 and deltas[1] <= 0.10
        rows.append({"path": path, "name": name, "everything": everything, "d95": deltas[0], "d99": deltas[1], "cpu": cpu,
                     "loadsOK": loads, "ok": loads and cpu <= limit})

passers = sorted((r for r in rows if r["everything"] and r["ok"]), key=lambda r: r["cpu"])
if mode == "table":
    for r in rows:
        fmt = lambda x: "not reached" if x is None else "%+.1f %%" % (100 * x)
        print("%-22s | 0.95 %s | 0.99 %s | %.3f ms of CPU a vector (limit %.2f) | %s | %s" % (
            r["name"], fmt(r["d95"]), fmt(r["d99"]), r["cpu"], limit,
            "PASSES" if r["ok"] else ("loads in, CPU out" if r["loadsOK"] else "out"), r["path"].split("/")[-2]))
elif mode == "any":
    print("yes" if passers else "no")
elif mode == "best":
    print(passers[0]["name"] if passers else "")
elif mode == "passers256":
    print(",".join(sorted({r["name"] for r in passers if r["name"].split(":")[3] == "256"})))
elif mode == "evprov":
    if passers:
        parts = passers[0]["name"].split(":")
        print("evprov:10:16:%s:%s:%s" % (parts[2], parts[3], parts[4] if len(parts) > 4 else "0"))
    else:
        print("")
