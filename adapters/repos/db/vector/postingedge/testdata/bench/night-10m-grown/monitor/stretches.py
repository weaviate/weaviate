# The stretches of the 10M growth with what the process and the system were
# doing in each: from grow.json (what the harness reads of the process at the
# end of every stretch, since the start) and from monitor.jsonl (what
# monitor.py reads from outside every 10 s, since it was started). The
# stretches a sample of the stacks fell in are marked from samples.log.
#
# Usage: python3 -I -B stretches.py <grow.json> <monitor.jsonl> <samples.log> [tsv]
import json, sys, time
from datetime import datetime, timedelta

grow = json.load(open(sys.argv[1]))
monitor = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
monitor = [m for m in monitor if not m.get("gone")]
tsv = len(sys.argv) > 4 and sys.argv[4] == "tsv"

# The hour of a stretch is that of the day the run started, or the next.
samples = []
for line in open(sys.argv[3]):
    f = line.split()
    if len(f) >= 4 and f[2] in ("start", "end"):
        samples.append((f[2], datetime.strptime(f[0] + " " + f[1], "%Y-%m-%d %H:%M:%S").timestamp()))
start_day = datetime.strptime("2026-11-27 17:12:00", "%Y-%m-%d %H:%M:%S")


def stamp(at, after):
    t = datetime.strptime(start_day.strftime("%Y-%m-%d ") + at, "%Y-%m-%d %H:%M:%S")
    while t.timestamp() < after - 1:
        t += timedelta(days=1)
    return t.timestamp()


def near(t):
    """The sample of the monitor nearest to t, if within 15 s."""
    best = min(monitor, key=lambda m: abs(m["t"] - t), default=None)
    return best if best is not None and abs(best["t"] - t) <= 15 else None


def u32(a, b):
    return (b - a) % 2**32


cols = ["at", "M in", "ins/s", "user", "sys", "ms CPU/ins", "rd MB/s", "wr MB/s", "rd KB/ins", "wr KB/ins",
        "pageins/s", "heap GB", "| ready", "Minstr/ins", "Mcyc/ins", "IPC", "rd ops/s", "rd ms/op", "wr ops/s", "wr ms/op", "csw/ins", "sysc/ins",
        "thr R", "thr U", "cache GB", "anon GB", "compr GB", "swap GB", "sys pgin/s", "sys pgout/s", "note"]
rows = []
prev, prev_t = None, start_day.timestamp()
for st in grow["stretches"]:
    t = stamp(st["at"], prev_t)
    if prev is None:
        prev, prev_t = st, t
        continue
    dt = st["seconds"] - prev["seconds"]
    n = st["inserted"] - prev["inserted"]
    if dt <= 0 or n <= 0:
        prev, prev_t = st, t
        continue
    ru = lambda k: st["rusage"][k] - prev["rusage"][k]
    row = {
        "at": st["at"], "M in": "%.2f" % (st["inserted"] / 1e6), "ins/s": "%.0f" % (n / dt),
        "user": "%.2f" % (ru("userNs") / 1e9 / dt), "sys": "%.2f" % (ru("systemNs") / 1e9 / dt),
        "ms CPU/ins": "%.1f" % ((ru("userNs") + ru("systemNs")) / 1e6 / n),
        "rd MB/s": "%.1f" % (ru("diskBytesRead") / 1e6 / dt), "wr MB/s": "%.1f" % (ru("diskBytesWritten") / 1e6 / dt),
        "rd KB/ins": "%.0f" % (ru("diskBytesRead") / 1e3 / n), "wr KB/ins": "%.0f" % (ru("diskBytesWritten") / 1e3 / n),
        "pageins/s": "%.1f" % (ru("pageins") / dt), "heap GB": "%.1f" % (st["heapInuse"] / 1e9),
    }
    a, b = near(prev_t), near(t)
    if a is not None and b is not None and b["t"] > a["t"]:
        mdt = b["t"] - a["t"]
        inside = [m for m in monitor if a["t"] <= m["t"] <= b["t"]]
        r = lambda k: b["rusage"][k] - a["rusage"][k]
        d = lambda k: b["disk"][k] - a["disk"][k]
        v = lambda k: b["vm"][k] - a["vm"][k]
        mean = lambda f: sum(f(m) for m in inside) / len(inside)
        page = b["vm"]["pageSize"]
        row.update({
            "| ready": "%.2f" % (r("runnable_time_ns") / 1e9 / mdt),
            "Minstr/ins": "%.0f" % (r("instructions") / 1e6 / n * dt / mdt),
            "Mcyc/ins": "%.0f" % (r("cycles") / 1e6 / n * dt / mdt),
            "IPC": "%.2f" % (r("instructions") / max(1, r("cycles"))),
            "rd ops/s": "%.0f" % (d("readOps") / mdt),
            "rd ms/op": "%.2f" % (d("readNs") / 1e6 / max(1, d("readOps"))),
            "wr ops/s": "%.0f" % (d("writeOps") / mdt),
            "wr ms/op": "%.2f" % (d("writeNs") / 1e6 / max(1, d("writeOps"))),
            "csw/ins": "%.0f" % (u32(a["task"]["csw"], b["task"]["csw"]) / n * dt / mdt),
            "sysc/ins": "%.0f" % (u32(a["task"]["syscalls_unix"], b["task"]["syscalls_unix"]) / n * dt / mdt),
            "thr R": "%.1f" % mean(lambda m: m["threads"].get("R", 0)),
            "thr U": "%.1f" % mean(lambda m: m["threads"].get("U", 0)),
            "cache GB": "%.1f" % (mean(lambda m: m["vm"]["File-backed pages"]) * page / 1e9),
            "anon GB": "%.1f" % (mean(lambda m: m["vm"]["Anonymous pages"]) * page / 1e9),
            "compr GB": "%.1f" % (mean(lambda m: m["vm"]["Pages occupied by compressor"]) * page / 1e9),
            "swap GB": "%.1f" % (mean(lambda m: m["vm"].get("swapUsedMB", 0)) / 1e3),
            "sys pgin/s": "%.0f" % (v("Pageins") / mdt), "sys pgout/s": "%.0f" % (v("Pageouts") / mdt),
        })
    notes = []
    if any(prev_t <= ts <= t for _, ts in samples):
        notes.append("sampled")
    if prev_t <= datetime.strptime("2026-11-27 20:56:06", "%Y-%m-%d %H:%M:%S").timestamp() <= t:
        notes.append("Go cache deleted")
    row["note"] = ", ".join(notes)
    rows.append(row)
    prev, prev_t = st, t

if tsv:
    print("\t".join(cols))
    for row in rows:
        print("\t".join(row.get(c, "") for c in cols))
else:
    width = {c: max(len(c), max((len(row.get(c, "")) for row in rows), default=0)) for c in cols}
    print("  ".join(c.rjust(width[c]) for c in cols))
    for row in rows:
        print("  ".join(row.get(c, "").rjust(width[c]) for c in cols))
