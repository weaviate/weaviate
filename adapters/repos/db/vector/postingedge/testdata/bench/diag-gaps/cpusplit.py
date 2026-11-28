# The CPU of a process by what its threads were running, from a report of
# /usr/bin/sample: every sample of a thread that was running (its leaf is not
# a call that waits) goes to the first of these groups any frame of its stack
# matches, in this order: the collector (its workers and what an allocation
# is made to help with), the compaction of the store, its flushes, applying a
# batch, the plans (the search of an insert and what it reads), the harness,
# the scheduler of the runtime, and the rest. In threads: 1.0 is one core.
#
# Usage: python3 -I -B cpusplit.py <sample.txt>...
import re, sys
from collections import defaultdict

WAITING = {"__psynch_cvwait", "pread", "read", "preadv", "readv", "pwrite", "write", "writev", "ftruncate", "fsync",
           "fcntl", "fdatasync", "msync", "__fcntl", "open", "close", "rename", "unlink", "__open", "fstat", "stat",
           "lstat", "getdirentries64", "__getdirentries64", "openat", "__openat", "fstatat", "mkdir", "__unlink",
           "__rename", "__semwait_signal", "nanosleep", "usleep", "kevent", "__psynch_mutexwait", "__ulock_wait",
           "mach_msg2_trap", "mach_msg_trap", "__workq_kernreturn", "semaphore_wait_trap", "__sigwait", "__select"}
GROUPS = [
    ("the collector: its workers", ("runtime.gcBgMarkWorker", "runtime.bgsweep", "runtime.bgscavenge")),
    ("the collector: inside an allocation (assist, sweep)", ("runtime.gcAssistAlloc", "runtime.gcDrain",
                                                              "runtime.gcMarkTermination", "runtime.sweepone",
                                                              "runtime.gcStart", "runtime.markroot", "runtime.scanobject",
                                                              "runtime.greyobject", "runtime.gcMarkDone",
                                                              "runtime.(*sweepLocked)", "runtime.deductSweepCredit")),
    ("store: compaction", ("lsmkv.(*SegmentGroup).compact", "lsmkv.(*compactor", "lsmkv.newCompactor", "compactOnce",
                           "compactIfLevelsMatch", "lsmkv.(*segmentCleaner")),
    ("store: flush of a memtable", ("lsmkv.(*Bucket).FlushAndSwitch", "lsmkv.(*Memtable).flush",
                                    "lsmkv.(*Bucket).atomicallySwitchMemtable", "flushAndSwitch")),
    ("apply (the writes of a batch, the journal)", ("postingedge.(*Persistent).applyBatch",)),
    ("flush of a round (sync)", ("postingedge.(*Persistent).Flush", "postingedge.(*growIndex).sync")),
    ("plans: exact vectors", ("postingedge.(*Index).rescore", "postingedge.(*growIndex).exact",
                              "postingedge.(*growIndex).readVector", "postingedge.(*Index).exactDistance")),
    ("plans: rows from the store", ("postingedge.(*growIndex).fetch", "postingedge.(*growIndex).fetchMembers",
                                    "postingedge.(*growIndex).fetchLists", "postingedge.timedRead",
                                    "postingedge.(*growIndex).joinLists", "postingedge.(*growBlock)",
                                    "postingedge.(*growIndex).row")),
    ("plans: traversal", ("postingedge.(*postingSearch).visit", "postingedge.(*searchScratch)", "postingedge.(*sidecar)")),
    ("plans: the rest (centroids, ranking, the plan itself)", ("postingedge.(*Persistent).searchWindow",
                                                              "postingedge.(*growIndex).plan",
                                                              "postingedge.(*fetchedSearch)", "postingedge.(*growPlan)")),
    ("index, other", ("postingedge.(",)),
    ("store, other", ("lsmkv.",)),
    ("harness", ("postingedge.Test", "postingedge.diag", "postingedge.readDiag", "testing.")),
    ("runtime: scheduler (looking for work, parking, waking)", ("runtime.schedule", "runtime.findRunnable",
                                                               "runtime.mcall", "runtime.park_m", "runtime.stopm",
                                                               "runtime.startm", "runtime.wakep", "runtime.sysmon",
                                                               "runtime.goschedImpl", "runtime.ready", "runtime.mstart",
                                                               "runtime.gcstopm", "runtime.exitsyscall")),
]
LINE = re.compile(r"^([ +!:|]*)(\d+) (.*)$")


def frame_name(rest):
    name = re.split(r"\s+\(in ", rest)[0]
    name = re.sub(r"\s+\[0x.*$", "", name)
    return re.sub(r"\s+\+ [\d,.]+$", "", name).strip()


def parse(path):
    lines = open(path, errors="replace").read().split("\n")
    start = lines.index("Call graph:") + 1
    stack, out, threads = [], [], {}

    def close(upto):
        while stack and stack[-1][0] >= upto:
            indent, count, name, children = stack.pop()
            own = count - children
            if own > 0 and len(stack) >= 1:
                out.append((stack[0][2], own, [f[2] for f in stack[1:]] + [name]))

    for line in lines[start:]:
        if not line.strip():
            break
        m = LINE.match(line)
        if not m:
            continue
        indent, count, name = len(m.group(1)), int(m.group(2)), frame_name(m.group(3))
        close(indent)
        if stack:
            stack[-1][3] += count
        else:
            threads[name] = count
        stack.append([indent, count, name, 0])
    close(0)
    return out, threads


for path in sys.argv[1:]:
    samples, threads = parse(path)
    per = max(threads.values())
    by, waiting = defaultdict(int), 0
    for thread, own, stack in samples:
        if stack[-1] in WAITING:
            waiting += own
            continue
        group = "other (no frame of the index, the store or the runtime's scheduler)"
        for label, names in GROUPS:
            if any(any(n in frame for n in names) for frame in stack):
                group = label
                break
        by[group] += own
    running = sum(by.values())
    print("%s: %d threads, %d samples a thread; running %.2f threads, waiting %.2f" % (
        path.split("/")[-3] + "/" + path.split("/")[-2], len(threads), per, running / per, waiting / per))
    for label, n in sorted(by.items(), key=lambda kv: -kv[1]):
        print("  %-68s %6.2f threads  %5.1f %%" % (label, n / per, 100 * n / running))
