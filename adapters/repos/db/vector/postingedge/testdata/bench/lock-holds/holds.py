# The holds of the index for writing of a pass of TestDiagQueries of the
# diagnostic branch (result.json with "holds" and "longWaits"): how long by
# kind, what the longest are made of, the holds with rows of edges written by
# budget, the splits by part, and which hold every long wait of a query fell
# in.
#
# Usage: python3 -I -B holds.py <result.json>
import json, sys

d = json.load(open(sys.argv[1]))
holds = d.get("holds") or []
begin = d["beginUnixNano"]
took = d["seconds"]
print("%d query threads, %d callers, %.0f s: %d queries, %s inserted (%.1f/s)" % (
    d["queryThreads"], d["callers"], took, d["queries"], d.get("inserted"), d.get("insertsPerSecond", 0)))
w = d["lockWait"]
print("the wait of a query for the index: p50 %.3f ms, p99 %.3f, p99.9 %.3f, max %.1f; %d over 10 ms" % (
    w["p50ms"], w["p99ms"], w["p999ms"], w["maxms"], d["lockWaitsOver10ms"]))


def pct(values, q):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


kinds = {}
for h in holds:
    kinds.setdefault(h["kind"], []).append(h)
print("\nkind | holds | a second | p50 ms | p90 | p99 | max | held in all, %% of the pass")
for kind, hs in sorted(kinds.items()):
    ms = [h["ns"] / 1e6 for h in hs]
    print("%-7s | %5d | %6.2f | %7.2f | %7.2f | %7.2f | %8.1f | %7.1f s, %.1f %%" % (
        kind, len(hs), len(hs) / took, pct(ms, 0.5), pct(ms, 0.9), pct(ms, 0.99), max(ms), sum(ms) / 1e3,
        100 * sum(ms) / 1e3 / took))

PARTS = ("writeNs", "inverseNs", "splitNs", "rowNs", "journalNs", "handNs", "syncNs")


def line(h):
    parts = " ".join("%s %.1f" % (k[:-2], h.get(k, 0) / 1e6) for k in PARTS)
    rest = h["ns"] - sum(h.get(k, 0) for k in PARTS)
    splits = h.get("splits") or []
    return "at %7.2f s | %7.1f ms | %-6s | %3d plans | %s | rest %.1f | rows %d (%d by budget, %d by share, %.1f MB) | %d splits | journal %.1f MB | %d dist reads" % (
        (h["atUnixNano"] - begin) / 1e9, h["ns"] / 1e6, h["kind"], h.get("plans", 0), parts, rest / 1e6,
        h.get("rowWrites", 0), h.get("rowWritesBudget", 0), h.get("rowWritesFull", 0), h.get("rowBytes", 0) / 1e6,
        len(splits), h.get("journalBytes", 0) / 1e6, h.get("inverseDistReads", 0))


print("\nthe 15 longest holds:")
for h in sorted(holds, key=lambda h: -h["ns"])[:15]:
    print("  " + line(h))

budget = [h for h in holds if h.get("rowWritesBudget", 0) > 0]
print("\nholds with rows written by budget: %d" % len(budget))
for h in budget:
    print("  " + line(h))
if len(budget) > 1:
    gaps = [(b["atUnixNano"] - a["atUnixNano"]) / 1e9 for a, b in zip(budget, budget[1:])]
    print("  seconds between them:", " ".join("%.1f" % g for g in gaps))

batches = kinds.get("batch", [])
plain = [h for h in batches if h.get("rowWritesBudget", 0) == 0]
if plain:
    n = len(plain)
    total = sum(h["ns"] for h in plain)
    print("\nthe batches without rows by budget: %d, %.2f ms each; by part, %% of their time: %s; rest %.1f %%" % (
        n, total / 1e6 / n, ", ".join("%s %.1f" % (k[:-2], 100 * sum(h.get(k, 0) for h in plain) / total) for k in PARTS),
        100 * (total - sum(h.get(k, 0) for h in plain for k in PARTS)) / total))
    for name, pick in (("with no split", lambda h: not h.get("splits")), ("with one split", lambda h: len(h.get("splits") or []) == 1),
                       ("with two or more", lambda h: len(h.get("splits") or []) > 1)):
        ms = [h["ns"] / 1e6 for h in plain if pick(h)]
        if ms:
            print("  %-16s: %4d, p50 %.2f ms, p99 %.2f, max %.1f" % (name, len(ms), pct(ms, 0.5), pct(ms, 0.99), max(ms)))

splits = [s for h in batches for s in (h.get("splits") or [])]
if splits:
    n = len(splits)
    keys = ("rowsNs", "vectorsNs", "meansNs", "writesNs", "graphNs", "restNs")
    total = [sum(s[k] for k in keys) / 1e6 for s in splits]
    print("\nsplits: %d, one every %.2f s; %.2f ms each (p50 %.2f, p99 %.2f, max %.1f), %.0f members; by part, ms: %s" % (
        n, took / n, sum(total) / n, pct(total, 0.5), pct(total, 0.99), max(total), sum(s["members"] for s in splits) / n,
        ", ".join("%s %.2f" % (k[:-2], sum(s[k] for s in splits) / 1e6 / n) for k in keys)))
    worst = max(splits, key=lambda s: sum(s[k] for k in keys))
    print("  the longest: " + ", ".join("%s %.1f" % (k[:-2], worst[k] / 1e6) for k in keys))

waits = sorted(d.get("longWaits") or [], key=lambda x: -x["waitNs"])
print("\nqueries that waited over 5 ms: %d; the 10 longest, and the hold each fell in:" % len(waits))
for x in waits[:10]:
    asked, until = x["askedUnixNano"], x["askedUnixNano"] + x["waitNs"]
    inside = [h for h in holds if h["atUnixNano"] < until and h["atUnixNano"] + h["ns"] > asked]
    print("  asked at %7.2f s, waited %7.1f ms: %s" % ((asked - begin) / 1e9, x["waitNs"] / 1e6, "; ".join(
        "%s of %.1f ms taken at %.2f s (%d rows by budget, %d splits)" % (
            h["kind"], h["ns"] / 1e6, (h["atUnixNano"] - begin) / 1e9, h.get("rowWritesBudget", 0), len(h.get("splits") or []))
        for h in inside) or "no hold recorded over it"))
over = [x for x in waits if x["waitNs"] > 100e6]
inb = 0
for x in over:
    asked, until = x["askedUnixNano"], x["askedUnixNano"] + x["waitNs"]
    if any(h.get("rowWritesBudget", 0) > 0 and h["atUnixNano"] < until and h["atUnixNano"] + h["ns"] > asked for h in holds):
        inb += 1
print("waits over 100 ms: %d, of them over a hold with rows by budget: %d" % (len(over), inb))
