# A growth of the definitive measures at 1M by stretch: what the harness reads
# of the process at the end of every stretch (grow.json: rusage, what the
# index counts of its writes) and what monitor.py reads from outside every
# 10 s (monitor.jsonl). It writes every stretch to a TSV and prints the same
# columns by groups of stretches, cut at the border of a stretch nearest to
# every multiple of <step> vectors in, and for the whole run.
#
# The CPU and the bytes are of the whole process: the inserts, and also the
# compaction, the flushes, the GC and the harness. The first stretch starts
# at the base (its rusage includes opening and building the base).
#
# Usage: python3 -I -B stretches.py <grow.json> <monitor.jsonl> <start "YYYY-MM-DD HH:MM:SS"> <out.tsv> <step>
import json, sys
from datetime import datetime, timedelta

grow = json.load(open(sys.argv[1]))
monitor = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
monitor = [m for m in monitor if not m.get("gone")]
start_day = datetime.strptime(sys.argv[3], "%Y-%m-%d %H:%M:%S")
step = int(sys.argv[5])
vector = 4 * grow["dims"]


def stamp(at, after):
    t = datetime.strptime(start_day.strftime("%Y-%m-%d ") + at, "%Y-%m-%d %H:%M:%S")
    while t.timestamp() < after - 1:
        t += timedelta(days=1)
    return t.timestamp()


def near(t):
    best = min(monitor, key=lambda m: abs(m["t"] - t), default=None)
    return best if best is not None and abs(best["t"] - t) <= 15 else None


# The stretches that count: the harness writes one more at the end, of no
# inserts, with the index settled.
zero = {"at": start_day.strftime("%H:%M:%S"), "inserted": grow["base"], "seconds": 0.0, "journalBytes": 0,
        "journalRecords": 0, "rowWrites": {"n": 0, "bytes": 0}, "rowWritesBudget": 0,
        "rowDistWrites": {"n": 0, "bytes": 0}, "pendingLogWrites": {"n": 0, "bytes": 0}, "livePostings": 0,
        "dirBytes": 0, "heapInuse": 0, "pendingHeap": 0}
zero["rusage"] = {k: 0 for k in grow["stretches"][0]["rusage"]}
marks = [zero]
t = start_day.timestamp()
zero["t"] = t
for st in grow["stretches"]:
    t = stamp(st["at"], t)
    st["t"] = t
    if st["inserted"] > marks[-1]["inserted"]:
        marks.append(st)

cols = ["at", "k in", "s", "ins/s", "user", "sys", "ms CPU/ins", "rd KB/ins", "wr KB/ins", "WA", "journal KB/ins",
        "rows KB/ins", "pending KB/ins", "row writes/ins", "by budget/ins", "postings", "disk GB", "heap GB",
        "pending MB", "| ready", "Minstr/ins", "IPC", "rd ops/s", "rd ms/op", "wr ops/s", "wr ms/op", "thr R",
        "thr U", "cache GB", "anon GB", "compr GB", "swap GB"]


def span(a, b):
    """The columns of what happened between two marks."""
    dt = b["seconds"] - a["seconds"]
    n = b["inserted"] - a["inserted"]
    ru = lambda k: b["rusage"][k] - a["rusage"][k]
    row = {
        "at": b["at"], "k in": "%.0f" % (b["inserted"] / 1e3), "s": "%.0f" % dt, "ins/s": "%.0f" % (n / dt),
        "rd KB/ins": "%.1f" % (ru("diskBytesRead") / 1e3 / n), "wr KB/ins": "%.1f" % (ru("diskBytesWritten") / 1e3 / n),
        "WA": "%.2f" % (ru("diskBytesWritten") / (n * vector)),
        "journal KB/ins": "%.2f" % ((b["journalBytes"] - a["journalBytes"]) / 1e3 / n),
        "rows KB/ins": "%.2f" % ((b["rowWrites"]["bytes"] - a["rowWrites"]["bytes"]
                                  + b["rowDistWrites"]["bytes"] - a["rowDistWrites"]["bytes"]) / 1e3 / n),
        "pending KB/ins": "%.2f" % ((b["pendingLogWrites"]["bytes"] - a["pendingLogWrites"]["bytes"]) / 1e3 / n),
        "row writes/ins": "%.3f" % ((b["rowWrites"]["n"] - a["rowWrites"]["n"]) / n),
        "by budget/ins": "%.3f" % ((b["rowWritesBudget"] - a["rowWritesBudget"]) / n),
        "postings": "%d" % b["livePostings"], "disk GB": "%.2f" % (b["dirBytes"] / 1e9),
        "heap GB": "%.2f" % (b["heapInuse"] / 1e9), "pending MB": "%.1f" % (b["pendingHeap"] / 1e6),
    }
    if a is not zero:
        row.update({"user": "%.2f" % (ru("userNs") / 1e9 / dt), "sys": "%.2f" % (ru("systemNs") / 1e9 / dt),
                    "ms CPU/ins": "%.2f" % ((ru("userNs") + ru("systemNs")) / 1e6 / n)})
    ma, mb = near(a["t"]), near(b["t"])
    if a is not zero and ma is not None and mb is not None and mb["t"] > ma["t"]:
        mdt = mb["t"] - ma["t"]
        inside = [m for m in monitor if ma["t"] <= m["t"] <= mb["t"]]
        r = lambda k: mb["rusage"][k] - ma["rusage"][k]
        d = lambda k: mb["disk"][k] - ma["disk"][k]
        mean = lambda f: sum(f(m) for m in inside) / len(inside)
        page = mb["vm"]["pageSize"]
        row.update({
            "| ready": "%.2f" % ((r("runnable_time_ns") - r("user_time_ns") - r("system_time_ns")) / 1e9 / mdt),
            "Minstr/ins": "%.1f" % (r("instructions") / 1e6 / n * dt / mdt),
            "IPC": "%.2f" % (r("instructions") / max(1, r("cycles"))),
            "rd ops/s": "%.0f" % (d("readOps") / mdt), "rd ms/op": "%.2f" % (d("readNs") / 1e6 / max(1, d("readOps"))),
            "wr ops/s": "%.0f" % (d("writeOps") / mdt),
            "wr ms/op": "%.2f" % (d("writeNs") / 1e6 / max(1, d("writeOps"))),
            "thr R": "%.1f" % mean(lambda m: m["threads"].get("R", 0)),
            "thr U": "%.1f" % mean(lambda m: m["threads"].get("U", 0)),
            "cache GB": "%.1f" % (mean(lambda m: m["vm"]["File-backed pages"]) * page / 1e9),
            "anon GB": "%.1f" % (mean(lambda m: m["vm"]["Anonymous pages"]) * page / 1e9),
            "compr GB": "%.1f" % (mean(lambda m: m["vm"]["Pages occupied by compressor"]) * page / 1e9),
            "swap GB": "%.1f" % (mean(lambda m: m["vm"].get("swapUsedMB", 0)) / 1e3),
        })
    return row


with open(sys.argv[4], "w") as f:
    f.write("\t".join(cols) + "\n")
    for a, b in zip(marks, marks[1:]):
        row = span(a, b)
        f.write("\t".join(row.get(c, "") for c in cols) + "\n")

# The groups: from a cut to the next, the cut being the mark nearest to every
# multiple of the step.
cuts = [marks[0]]
k = step
last = marks[-1]["inserted"]
while k < last:
    best = min(marks, key=lambda m: abs(m["inserted"] - k))
    if best is not cuts[-1] and best is not marks[-1]:
        cuts.append(best)
    k += step
cuts.append(marks[-1])
show = ["at", "k in", "s", "ins/s", "user", "sys", "ms CPU/ins", "rd KB/ins", "wr KB/ins", "WA", "journal KB/ins",
        "rows KB/ins", "pending KB/ins", "row writes/ins", "postings", "disk GB", "heap GB", "pending MB", "| ready",
        "Minstr/ins", "IPC", "thr R", "thr U"]
print("\t".join(show))
for a, b in zip(cuts, cuts[1:]):
    row = span(a, b)
    print("\t".join(row.get(c, "") for c in show))
# The whole run, and the whole run without its first stretch (whose CPU has
# the opening and the base).
row = span(marks[0], marks[-1])
row["at"] = "all"
print("\t".join(row.get(c, "") for c in show))
row = span(marks[1], marks[-1])
row["at"] = "all but the first stretch"
print("\t".join(row.get(c, "") for c in show))
print("stretches:", len(marks) - 1, "| last stretch: %d s, %d inserts" % (
    marks[-1]["seconds"] - marks[-2]["seconds"], marks[-1]["inserted"] - marks[-2]["inserted"]))
print("total: %.1f s of inserts, %.1f inserts/s, written %.2f GB = %.2f times the vectors (%.2f GB), read %.2f GB" % (
    marks[-1]["seconds"], (marks[-1]["inserted"] - grow["base"]) / marks[-1]["seconds"],
    marks[-1]["rusage"]["diskBytesWritten"] / 1e9,
    marks[-1]["rusage"]["diskBytesWritten"] / (grow["rows"] * vector), grow["rows"] * vector / 1e9,
    marks[-1]["rusage"]["diskBytesRead"] / 1e9))
print("CPU: %.2f h user, %.2f h system, %.2f ms per insert" % (
    marks[-1]["rusage"]["userNs"] / 3.6e12, marks[-1]["rusage"]["systemNs"] / 3.6e12,
    (marks[-1]["rusage"]["userNs"] + marks[-1]["rusage"]["systemNs"]) / 1e6 / (marks[-1]["inserted"] - grow["base"])))
