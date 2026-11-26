# The heap in use after opening the night's index, by component, from
# `go tool pprof -raw` of a profile taken with -test.memprofilerate=1, in
# decimal MB. The lines are those of grow.go at feat/posting-edge-integration-m2
# 67fc89114c (openGrowIndex starts at 1209). lsmkv is broken down by what
# allocates inside it.
import re, sys
from collections import defaultdict
def load(path):
    lines = open(path).read().split("\n")
    samples, locs, cur = [], {}, None
    i = lines.index("Samples:") + 2
    while not lines[i].startswith("Locations"):
        m = re.match(r"\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+):\s+([\d ]+)$", lines[i])
        if m: samples.append((int(m.group(4)), [int(x) for x in m.group(5).split()]))
        i += 1
    for l in lines[i + 1:]:
        m = re.match(r"\s+(\d+): 0x\S+ M=\d+ (\S+) (\S+?):(\d+):\d+ s=\d+", l)
        if m:
            cur = int(m.group(1)); locs[cur] = [(m.group(2), m.group(3).split("/")[-1], int(m.group(4)))]; continue
        m = re.match(r"\s+(\S+) (\S+?):(\d+):\d+ s=\d+", l)
        if m and cur is not None: locs[cur].append((m.group(1), m.group(2).split("/")[-1], int(m.group(3))))
    return [(v, [f for x in ids for f in locs.get(x, [])]) for v, ids in samples if v > 0]
def has(fr, *subs):
    return any(any(s in f[0] for s in subs) for f in fr)
def at(fr, line):
    return any("openGrowIndex" in f[0] and f[1] == "grow.go" and f[2] == line for f in fr)
def first(fr, sub):
    for f in fr:
        if sub in f[0]: return "%s %s:%d" % (f[0].split("/")[-1], f[1], f[2])
    return "?"
def category(fr):
    if has(fr, "compressionhelpers"): return "centroid graph: codes (RQ8) and their cache"
    if at(fr, 1335): return "routing codes"
    if at(fr, 1277) or at(fr, 1283) or at(fr, 1285): return "centroid vectors (float32)"
    if has(fr, "openCentroidGraph", "vector/hnsw.", "hnsw/packedconn", "vector/cache.", "hnsw/compact."):
        if has(fr, "packedconn"): return "centroid graph: connections"
        if has(fr, "convertEntityNodes"): return "centroid graph: nodes"
        if has(fr, "vector/cache."): return "centroid graph: vector cache"
        return "centroid graph: commit log and the rest"
    if at(fr, 1333): return "docID -> posting"
    if at(fr, 1307) or has(fr, "openSlots"): return "docID -> slot"
    if has(fr, "openBuilt"): return "docID -> when its edges were built"
    if at(fr, 1340): return "live per docID, size per posting"
    if at(fr, 1394) or at(fr, 1401) or at(fr, 1402): return "per posting: retired, halo and list sequences"
    if at(fr, 1413): return "tombstones (deleted per docID)"
    if has(fr, "keepPending", "growPendingDecode", "loadPending"): return "edge changes waiting to consolidate"
    if has(fr, "lsmkv."):
        if has(fr, "BloomFilter", "bloom"): return "lsmkv: bloom filters"
        if has(fr, "emtable", "ommitLogger"): return "lsmkv: memtables and WALs"
        if has(fr, "segmentindex"): return "lsmkv: segment indexes"
        if has(fr, "newSegment", "segment)"): return "lsmkv: segments, other: " + first(fr, "lsmkv.")
        return "lsmkv: other: " + first(fr, "lsmkv.")
    if has(fr, "vector/postingedge."):
        return "index, other: " + first(fr, "vector/postingedge.")
    return "runtime and test"
by = defaultdict(int)
for v, fr in load(sys.argv[1]):
    by[category(fr)] += v
total = sum(by.values())
heapalloc, heapinuse = float(sys.argv[2]), float(sys.argv[3])
named = lambda k: k.startswith(("centroid", "routing", "docID", "live", "per posting", "tombstones", "edge", "lsmkv"))
small = sum(v for k, v in by.items() if v < 1e6 and not named(k))
for k, v in sorted(by.items(), key=lambda kv: -kv[1]):
    if v >= 1e6 or (named(k) and v >= 1e4): print("%-72s %9.2f" % (k, v / 1e6))
lsm = sum(v for k, v in by.items() if k.startswith("lsmkv"))
print("%-72s %9.2f" % ("(lsmkv in all)", lsm / 1e6))
print("%-72s %9.2f" % ("the rest, each under 1 MB (%d groups)" % sum(1 for k, v in by.items() if v < 1e6 and not named(k)), small / 1e6))
print("%-72s %9.2f" % ("= in the profile", total / 1e6))
print("%-72s %9.2f" % ("+ allocated, not in the profile", heapalloc - total / 1e6))
print("%-72s %9.2f" % ("= HeapAlloc", heapalloc))
print("%-72s %9.2f" % ("+ free in spans in use", heapinuse - heapalloc))
print("%-72s %9.2f" % ("= HeapInuse (the figure reported)", heapinuse))
