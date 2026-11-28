# The source lines the running threads of a sample were on: the leaf frames
# of the call graph of /usr/bin/sample that are in the test binary, by
# address, turned into lines with go tool addr2line. A frame the sample
# merged from several addresses lists two of them at most and one count: the
# count is shared evenly among those it lists, so the split between the lines
# of a function is approximate, and the total of a function is exact.
#
# Usage: python3 -I -B lines.py <sample.txt> <binary> <inserts a second> [top]
import re, subprocess, sys
from collections import defaultdict

path, binary, rate = sys.argv[1], sys.argv[2], float(sys.argv[3])
top = int(sys.argv[4]) if len(sys.argv) > 4 else 40
lines = open(path, errors="replace").read().split("\n")
load = int(next(l for l in lines if l.startswith("Load Address:")).split()[-1], 16)
samples = None
frame = re.compile(r"^([ +!:|]*?)(\d+) (.*?)  \(in ([^)]+)\) \+ ([\d,.]+)  \[([0-9a-fx,.]+)\]")
# The call graph: a frame is a leaf when the next frame is not deeper.
graph = []
for l in lines:
    if l.startswith("Total number in stack") or l.startswith("Sort by top of stack"):
        break
    m = frame.match(l)
    if m:
        graph.append((len(m.group(1)), int(m.group(2)), m.group(3), m.group(4), m.group(6)))
    elif re.match(r"^[ +!:|]*\d+ ", l):
        graph.append((len(re.match(r"^([ +!:|]*)", l).group(1)), 0, None, None, None))
    m = re.match(r"^\s+(\d+) Thread_\d+", l)
    if m and samples is None:
        samples = int(m.group(1))
by_addr = defaultdict(float)
by_func = defaultdict(float)
for i, (depth, count, name, image, addrs) in enumerate(graph):
    if name is None or not image.endswith(".test"):
        continue
    # What its children do not account for is what was on this frame itself.
    j, below = i + 1, 0
    child_depth = None
    while j < len(graph) and graph[j][0] > depth:
        if child_depth is None:
            child_depth = graph[j][0]
        if graph[j][0] == child_depth:
            below += graph[j][1]
        j += 1
    own = count - below
    if own <= 0:
        continue
    listed = [int(a, 16) for a in addrs.split(",") if a.startswith("0x")]
    for a in listed:
        by_addr[a - load + 0x100000000] += own / len(listed)
    by_func[name] += own
addrs = sorted(by_addr, key=lambda a: -by_addr[a])
out = subprocess.run(["go", "tool", "addr2line", binary], input="\n".join("%x" % a for a in addrs), capture_output=True,
                     text=True).stdout.split("\n")
where = {}
for k, a in enumerate(addrs):
    where[a] = (out[2 * k].strip(), out[2 * k + 1].strip().split("/")[-1])
by_line = defaultdict(float)
for a, c in by_addr.items():
    by_line[where[a]] += c
total = sum(by_func.values())
print("%s: %d samples a thread; %.0f samples on frames of the binary itself, %.2f threads" % (
    path.split("/")[-2], samples, total, total / samples))
print("  threads   ms/insert  function, by what was on the frame itself")
for name, c in sorted(by_func.items(), key=lambda kv: -kv[1])[:top]:
    print("  %7.3f  %9.3f  %s" % (c / samples, c / samples / rate * 1e3, name.split("/")[-1]))
print("  threads   ms/insert  line (approximate within a function)")
for (name, line), c in sorted(by_line.items(), key=lambda kv: -kv[1])[:top]:
    print("  %7.3f  %9.3f  %s  %s" % (c / samples, c / samples / rate * 1e3, line, name.split("/")[-1]))
