# The whole 10M growth by stretch: what stretches.py gives (the harness's
# rusage and the monitor from outside), plus what the index itself counts of
# every insert (growStats), from the copies of grow.json kept after every
# stretch (snap.py), as differences between consecutive copies; plus what the
# harness counts of the writes of every stretch.
#
# Usage: python3 -I -B full.py <grow.json> <monitor.jsonl> <samples.log> <grow-snap dir> <out.tsv>
import json, os, sys
from datetime import datetime, timedelta

grow = json.load(open(sys.argv[1]))
monitor = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
monitor = [m for m in monitor if not m.get("gone")]
samples = []
for line in open(sys.argv[3]):
    f = line.split()
    if len(f) >= 4 and f[2] in ("start", "end") and f[3].startswith("sample-"):
        samples.append((f[2], datetime.strptime(f[0] + " " + f[1], "%Y-%m-%d %H:%M:%S").timestamp()))
snaps = {}
for name in os.listdir(sys.argv[4]):
    d = json.load(open(os.path.join(sys.argv[4], name)))
    snaps[d["stretches"][-1]["inserted"]] = d["stats"]
snaps[grow["stretches"][-1]["inserted"]] = grow["stats"]
start_day = datetime.strptime("2026-11-27 17:12:00", "%Y-%m-%d %H:%M:%S")


def stamp(at, after):
    t = datetime.strptime(start_day.strftime("%Y-%m-%d ") + at, "%Y-%m-%d %H:%M:%S")
    while t.timestamp() < after - 1:
        t += timedelta(days=1)
    return t.timestamp()


def near(t):
    best = min(monitor, key=lambda m: abs(m["t"] - t), default=None)
    return best if best is not None and abs(best["t"] - t) <= 15 else None


def u32(a, b):
    return (b - a) % 2**32


cols = ["at", "M in", "ins/s", "user", "sys", "ms CPU/ins", "rd MB/s", "wr MB/s", "rd KB/ins", "wr KB/ins", "WA",
        "pageins/s", "heap GB", "disk GB", "postings", "journal KB/ins", "rows KB/ins", "pending KB/ins",
        "| ready", "Minstr/ins", "Mcyc/ins", "IPC", "rd ops/s", "rd ms/op", "wr ops/s", "wr ms/op", "csw/ins",
        "sysc/ins", "faults/ins", "thr R", "thr U", "cache GB", "anon GB", "compr GB", "swap GB", "sys pgin/s",
        "sys pgout/s",
        "| plan ms", "assign ms", "apply ms", "loads", "rounds", "rows", "rows MB", "vectors", "inv checks",
        "inv edges", "splits/1000", "note"]
rows = []
prev, prev_t = None, start_day.timestamp()
for st in grow["stretches"]:
    t = stamp(st["at"], prev_t)
    if prev is None:
        first = {"inserted": grow["base"], "seconds": 0}
        prev_for = first
    else:
        prev_for = prev
    dt = st["seconds"] - prev_for["seconds"]
    n = st["inserted"] - prev_for["inserted"]
    if dt <= 1 or n <= 0:
        prev, prev_t = st, t
        continue
    zero = {"rusage": {k: 0 for k in st["rusage"]}, "journalBytes": 0, "rowWrites": {"bytes": 0},
            "rowDistWrites": {"bytes": 0}, "pendingLogWrites": {"bytes": 0}}
    p = prev if prev is not None else zero
    ru = lambda k: st["rusage"][k] - p["rusage"][k]
    row = {
        "at": st["at"], "M in": "%.2f" % (st["inserted"] / 1e6), "ins/s": "%.0f" % (n / dt),
        "WA": "%.1f" % (ru("diskBytesWritten") / (n * 4 * grow["dims"])),
        "heap GB": "%.1f" % (st["heapInuse"] / 1e9), "disk GB": "%.1f" % (st["dirBytes"] / 1e9),
        "postings": "%d" % st["livePostings"],
        "journal KB/ins": "%.1f" % ((st["journalBytes"] - p["journalBytes"]) / 1e3 / n),
        "rows KB/ins": "%.1f" % ((st["rowWrites"]["bytes"] - p["rowWrites"]["bytes"]
                                  + st["rowDistWrites"]["bytes"] - p["rowDistWrites"]["bytes"]) / 1e3 / n),
        "pending KB/ins": "%.2f" % ((st["pendingLogWrites"]["bytes"] - p["pendingLogWrites"]["bytes"]) / 1e3 / n),
        "rd MB/s": "%.1f" % (ru("diskBytesRead") / 1e6 / dt), "wr MB/s": "%.1f" % (ru("diskBytesWritten") / 1e6 / dt),
        "rd KB/ins": "%.0f" % (ru("diskBytesRead") / 1e3 / n), "wr KB/ins": "%.0f" % (ru("diskBytesWritten") / 1e3 / n),
        "pageins/s": "%.1f" % (ru("pageins") / dt),
    }
    if prev is not None:
        # The first stretch's CPU includes the base and the opening.
        row.update({"user": "%.2f" % (ru("userNs") / 1e9 / dt), "sys": "%.2f" % (ru("systemNs") / 1e9 / dt),
                    "ms CPU/ins": "%.1f" % ((ru("userNs") + ru("systemNs")) / 1e6 / n)})
    a, b = (near(prev_t), near(t)) if prev is not None else (None, None)
    if a is not None and b is not None and b["t"] > a["t"]:
        mdt = b["t"] - a["t"]
        inside = [m for m in monitor if a["t"] <= m["t"] <= b["t"]]
        r = lambda k: b["rusage"][k] - a["rusage"][k]
        d = lambda k: b["disk"][k] - a["disk"][k]
        v = lambda k: b["vm"][k] - a["vm"][k]
        mean = lambda f: sum(f(m) for m in inside) / len(inside)
        page = b["vm"]["pageSize"]
        row.update({
            "| ready": "%.2f" % ((r("runnable_time_ns") - r("user_time_ns") - r("system_time_ns")) / 1e9 / mdt),
            "Minstr/ins": "%.0f" % (r("instructions") / 1e6 / n * dt / mdt),
            "Mcyc/ins": "%.0f" % (r("cycles") / 1e6 / n * dt / mdt),
            "IPC": "%.2f" % (r("instructions") / max(1, r("cycles"))),
            "rd ops/s": "%.0f" % (d("readOps") / mdt),
            "rd ms/op": "%.2f" % (d("readNs") / 1e6 / max(1, d("readOps"))),
            "wr ops/s": "%.0f" % (d("writeOps") / mdt),
            "wr ms/op": "%.2f" % (d("writeNs") / 1e6 / max(1, d("writeOps"))),
            "csw/ins": "%.0f" % (u32(a["task"]["csw"], b["task"]["csw"]) / n * dt / mdt),
            # The system's counter of syscalls is a signed 32 bits that stops at
            # its top (reached at 23:22): no figure from there on.
            "sysc/ins": "" if b["task"]["syscalls_unix"] >= 2**31 - 1 else
            "%.0f" % (u32(a["task"]["syscalls_unix"], b["task"]["syscalls_unix"]) / n * dt / mdt),
            "faults/ins": "%.0f" % (u32(a["task"]["faults"], b["task"]["faults"]) / n * dt / mdt),
            "thr R": "%.1f" % mean(lambda m: m["threads"].get("R", 0)),
            "thr U": "%.1f" % mean(lambda m: m["threads"].get("U", 0)),
            "cache GB": "%.1f" % (mean(lambda m: m["vm"]["File-backed pages"]) * page / 1e9),
            "anon GB": "%.1f" % (mean(lambda m: m["vm"]["Anonymous pages"]) * page / 1e9),
            "compr GB": "%.1f" % (mean(lambda m: m["vm"]["Pages occupied by compressor"]) * page / 1e9),
            "swap GB": "%.1f" % (mean(lambda m: m["vm"].get("swapUsedMB", 0)) / 1e3),
            "sys pgin/s": "%.0f" % (v("Pageins") / mdt), "sys pgout/s": "%.0f" % (v("Pageouts") / mdt),
        })
    sa = snaps.get(prev["inserted"]) if prev is not None else None
    sb = snaps.get(st["inserted"])
    if sa is not None and sb is not None and sb["inserted"] > sa["inserted"]:
        k = sb["inserted"] - sa["inserted"]
        s = lambda key: sb[key] - sa[key]
        sn = lambda key, f: sb[key][f] - sa[key][f]
        row.update({
            "| plan ms": "%.2f" % (s("searchNs") / 1e6 / k), "assign ms": "%.2f" % (s("assignNs") / 1e6 / k),
            "apply ms": "%.2f" % ((s("writeNs") + s("inverseNs") + s("splitNs") + s("rowNs")) / 1e6 / k),
            "loads": "%.1f" % (s("searchLoads") / k), "rounds": "%.1f" % (s("searchRounds") / k),
            "rows": "%.1f" % (sn("searchRows", "n") / k), "rows MB": "%.2f" % (sn("searchRows", "bytes") / 1e6 / k),
            "vectors": "%.1f" % (sn("searchVectors", "n") / k), "inv checks": "%.1f" % (s("inverseChecks") / k),
            "inv edges": "%.2f" % (s("inverseEdges") / k), "splits/1000": "%.2f" % (1000 * s("splits") / k),
        })
    notes = []
    if any(prev_t <= ts <= t for _, ts in samples):
        notes.append("sampled")
    if prev_t <= datetime.strptime("2026-11-27 20:56:06", "%Y-%m-%d %H:%M:%S").timestamp() <= t:
        notes.append("Go cache deleted")
    row["note"] = ", ".join(notes)
    rows.append(row)
    prev, prev_t = st, t

with open(sys.argv[5], "w") as f:
    f.write("\t".join(cols) + "\n")
    for row in rows:
        f.write("\t".join(row.get(c, "") for c in cols) + "\n")
print(len(rows), "stretches")
