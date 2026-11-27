# Counts what the threads of a process were doing in a report of
# /usr/bin/sample: for every sample of every thread, the frame it was in
# (the leaf of its stack) and, for those that were running Go code, the part
# of the index its stack goes through. In threads: samples over the samples
# of one thread, so that 1.0 is one thread doing that all the time.
#
# A thread inside a libc call (pread, pwrite, the wait of a condition
# variable) shows only that call: Go makes them on the system stack, and the
# Go stack above is not in the report. A goroutine that waits for a
# sync.Mutex is not a thread: its thread parks or runs something else.
#
# Usage: python3 -I -B census.py [-functions] <sample.txt>...
import re, sys
from collections import defaultdict

LEAF = [
    ("parked (condition variable)", ("__psynch_cvwait",)),
    ("reading (pread, read)", ("pread", "read", "preadv", "readv")),
    ("writing (pwrite, write)", ("pwrite", "write", "writev", "ftruncate")),
    ("syncing (fsync, fcntl)", ("fsync", "fcntl", "fdatasync", "msync", "__fcntl")),
    ("files (open, close, rename, unlink, stat)", ("open", "close", "rename", "unlink", "__open", "fstat", "stat",
                                                   "lstat", "getdirentries64", "__getdirentries64", "openat",
                                                   "__openat", "fstatat", "mkdir", "__unlink", "__rename")),
    ("sleeping (usleep)", ("__semwait_signal", "nanosleep", "usleep")),
    ("netpoll (kevent)", ("kevent",)),
    ("waking a thread (cond signal)", ("__psynch_cvsignal", "pthread_cond_signal", "__psynch_cvbroad")),
    ("kernel lock (mutex wait)", ("__psynch_mutexwait", "__psynch_mutexdrop", "__ulock_wait", "__ulock_wake")),
    ("memory calls (madvise, mmap)", ("madvise", "mmap", "munmap", "__mmap", "mach_vm_map", "vm_map",
                                      "mach_vm_deallocate", "mprotect")),
]
LEAF_OF = {name: label for label, names in LEAF for name in names}

# The part of the index a running thread is in: the first of these that any
# frame of its stack matches, in this order.
PARTS = [
    ("GC", ("runtime.gcBgMarkWorker", "runtime.gcDrain", "runtime.gcAssistAlloc", "runtime.bgsweep",
            "runtime.bgscavenge", "runtime.gcMarkTermination", "runtime.sweepone", "runtime.gcStart",
            "runtime.markroot", "runtime.scanobject", "runtime.greyobject")),
    ("store: compaction", ("lsmkv.(*SegmentGroup).compact", "lsmkv.(*compactor", "lsmkv.newCompactor",
                           "compactOnce", "compactIfLevelsMatch", "Compaction")),
    ("store: flush of a memtable", ("lsmkv.(*Bucket).FlushAndSwitch", "lsmkv.(*Memtable).flush",
                                    "lsmkv.(*Bucket).atomicallySwitchMemtable", "flushAndSwitch")),
    ("layout pass", ("postingedge.(*growLayout)", "postingedge.(*Persistent).layOut")),
    ("apply: split", ("postingedge.(*growIndex).split", "postingedge.(*growIndex).applySplit", ".split")),
    ("apply: the rest (rows written, inverse edges, journal)", (
        "postingedge.(*Persistent).applyBatch", "postingedge.(*growIndex).apply", "postingedge.(*growIndex).insert",
        "postingedge.(*growIndex).write", "postingedge.(*growBucket).SetList", "postingedge.(*growIndex).journal",
        "postingedge.(*growJournal)")),
    ("plan: exact vectors (rescore)", ("postingedge.(*growIndex).exact", "postingedge.(*Index).rescore",
                                       "postingedge.(*growIndex).readVector", ".rescore")),
    ("plan: rows read from the store", ("postingedge.(*growIndex).fetch", "postingedge.growRows.fetch",
                                        "postingedge.(*growIndex).getRow", "postingedge.(*growIndex).fetchMembers",
                                        "postingedge.(*growIndex).joinRows", "postingedge.(*growBlock).decode",
                                        "postingedge.timedRead")),
    ("plan: centroids", ("postingedge.(*centroid", "hnsw.(*hnsw).Search", "hnsw.(*hnsw).search",
                         "hnsw.(*hnsw).knnSearchByVector", "postingedge.(*state).nearestPostings",
                         "postingedge.(*fetchedSearch).entries")),
    ("plan: traversal of the postings", ("postingedge.(*postingSearch).visit", "postingedge.(*searchScratch)",
                                         "postingedge.(*sidecar)", "postingedge.(*fetchedSearch).run")),
    ("plan: the rest", ("postingedge.(*growIndex).plan", "postingedge.(*growPlan)",
                        "postingedge.(*Persistent).searchWindow")),
    ("harness (reads the train rows, the stretch)", ("postingedge.TestNightGrow", "postingedge.(*nightTrain)",
                                                     "postingedge.nightDirBytes")),
    ("index, other", ("postingedge.",)),
    ("store, other", ("lsmkv.",)),
    ("runtime: scheduler", ("runtime.schedule", "runtime.findRunnable", "runtime.mcall", "runtime.park_m",
                            "runtime.stopm", "runtime.startm", "runtime.wakep", "runtime.sysmon",
                            "runtime.goschedImpl", "runtime.ready", "runtime.runqgrab", "runtime.stealWork")),
    ("runtime: allocation", ("runtime.mallocgc", "runtime.(*mheap)", "runtime.(*mcache)", "runtime.(*mcentral)",
                             "runtime.newobject", "runtime.makeslice", "runtime.growslice")),
]

LINE = re.compile(r"^([ +!:|]*)(\d+) (.*)$")


def frame_name(rest):
    name = re.split(r"\s+\(in ", rest)[0]
    name = re.sub(r"\s+\[0x.*$", "", name)
    return re.sub(r"\s+\+ [\d,.]+$", "", name).strip()


def parse(path):
    """Yields (thread, self samples, stack as a list of names, leaf first is last)."""
    lines = open(path, errors="replace").read().split("\n")
    start = lines.index("Call graph:") + 1
    stack = []  # (indent, count, name, children)
    out = []
    threads = {}

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
    return threads, out


def short(name):
    return name.replace("github.com/weaviate/weaviate/adapters/repos/db/", "").replace(
        "github.com/weaviate/weaviate/", "")


# The stack of a thread that runs Go code is cut at its leaf in most samples
# (the walk by frame pointer does not get past the assembly kernels and the
# leaf functions), so the leaf tells the group first, and the path only where
# the leaf tells nothing.
LEAVES = [
    ("GC (mark, sweep, stack scan)", ("runtime.greyobject", "runtime.scanobject", "runtime.scanObject",
                                      "runtime.gcDrain", "runtime.tryDeferToSpanScan", "runtime.(*spanSet)",
                                      "runtime.sweepone", "runtime.(*gcWork)", "runtime.markBits", "runtime.scanblock",
                                      "runtime.(*mspan).sweep", "runtime.(*sweepLocked)", "runtime.pcvalue",
                                      "runtime.findfunc", "runtime.(*stkframe)", "runtime.scanframeworker",
                                      "runtime.(*unwinder)", "runtime.newMarkBits", "runtime.spanOf",
                                      "runtime.findObject", "runtime.(*gcBits)", "runtime.gcAssist",
                                      "runtime.bgscavenge", "runtime.(*scavenge", "runtime.(*pageAlloc).scav",
                                      "runtime.heapBits", "runtime.typePointers", "runtime.(*mspan).typePointers",
                                      "runtime.(*mspan).heapBits", "runtime.scanConservative", "runtime.markroot")),
    ("allocation", ("runtime.mallocgc", "runtime.(*mheap)", "runtime.(*mcache)", "runtime.(*mcentral)",
                    "runtime.(*fixalloc)", "runtime.(*pallocBits)", "runtime.(*pallocData)", "runtime.(*pageAlloc)",
                    "runtime.memclrNoHeapPointers", "runtime.newobject", "runtime.makeslice", "runtime.growslice",
                    "runtime.(*consistentHeapStats)", "runtime.nextFreeFast", "runtime.(*mspan)",
                    "runtime.deductAssistCredit", "runtime.publicationBarrier", "runtime.bulkBarrierPreWrite",
                    "runtime.wbBufFlush", "runtime.gcWriteBarrier", "runtime.(*wbBuf)")),
    ("scoring: codes of members and endpoints", ("compressionhelpers.(*CenteredBinaryRQDistancer)",
                                                 "compressionhelpers.(*CappedCenteredBinaryRQDistancer)",
                                                 "compressionhelpers.weightedHamming", "asm.HammingBitwise",
                                                 "asm.hamming_bitwise", "postingedge.(*sidecar).score",
                                                 "asm.PrefetchN", "postingedge.(*sidecar).prefetch",
                                                 "compressionhelpers.(*BinaryRotationalQuantizer)",
                                                 "compressionhelpers.(*RQDistancer)")),
    ("traversal: following edges, the visited set, ranking", ("visited.(*SparseSet)",
                                                             "postingedge.(*searchScratch)",
                                                             "postingedge.(*postingSearch)", "postingedge.(*topK)",
                                                             "postingedge.(*fetchedSearch)")),
    ("rows: decoding and joining what the store returned", ("postingedge.(*growBlock)", "postingedge.(*growIndex).fetch",
                                                           "postingedge.(*growIndex).joinRows",
                                                           "postingedge.(*growIndex).getRow", "postingedge.growRows",
                                                           "postingedge.timedRead", "postingedge.(*growPlan)")),
    ("store: reads (segments, memtable, bloom)", ("lsmkv.(*segment)", "segmentindex.", "lsmkv.(*binarySearchNodeMulti)",
                                                  "bloom/v3.", "lsmkv.(*Bucket).SetList", "lsmkv.(*Bucket).getCollection",
                                                  "lsmkv.(*SegmentGroup).get", "lsmkv.(*Memtable).get",
                                                  "lsmkv.(*Bucket).setListFromConsistentView", "lsmkv.(*segment).get")),
    ("copying bytes (memmove)", ("runtime.memmove",)),
    ("map lookups and string keys", ("runtime.mapaccess", "runtime.mapassign", "internal/runtime/maps.",
                                     "runtime.slicebytetostring", "runtime.memequal", "cmpbody", "runtime.memhash",
                                     "runtime.strhash", "aeshashbody", "runtime.mapdelete", "runtime.mapiter")),
    ("centroid graph (RQ8 distances, the queue)", ("asm.dotByteUDOT", "asm.DotByteUDOT",
                                                   "compressionhelpers.(*quantizedCompressorDistancer",
                                                   "priorityqueue.", "hnsw.(*hnsw)", "compressionhelpers.(*RotationalQuantizer)",
                                                   "cache.(*shardedLockCache", "hnsw/packedconn")),
    ("exact vectors (float dot, the read)", ("asm.dot_neon", "asm.Dot", "postingedge.(*growIndex).exact",
                                             "postingedge.(*Index).rescore", "distancer.(*DotProduct",
                                             "distancer.(*Cosine", "asm.dot_")),
    ("clock reads", ("mach_absolute_time", "runtime.nanotime", "runtime.walltime", "clock_gettime", "gettimeofday",
                     "__commpage_gettimeofday", "time.Now", "time.Since", "time.now", "runtime.time")),
    ("scheduler and runtime locks", ("runtime.lock2", "runtime.unlock2", "runtime.findRunnable", "runtime.schedule",
                                     "runtime.runq", "runtime.stealWork", "runtime.park_m", "runtime.mcall",
                                     "runtime.gopark", "runtime.goready", "runtime.ready", "runtime.wakep",
                                     "runtime.startm", "runtime.stopm", "runtime.execute", "runtime.gogo",
                                     "runtime.casgstatus", "runtime.sem", "runtime.notewakeup", "runtime.notesleep",
                                     "runtime.pidle", "runtime.checkTimers", "runtime.netpoll", "runtime.osyield",
                                     "runtime.procyield", "sync.(*Mutex)", "sync.(*RWMutex)", "sync.(*WaitGroup)",
                                     "internal/sync.", "runtime.chan", "runtime.send", "runtime.recv",
                                     "runtime.selectgo", "runtime.newproc", "runtime.gfget", "runtime.malg",
                                     "runtime.goexit", "runtime.gdestroy", "runtime.acquireSudog",
                                     "runtime.releaseSudog", "runtime.save", "runtime.morestack", "runtime.newstack",
                                     "runtime.copystack", "runtime.systemstack", "runtime.mstart")),
    ("on the way into or out of a libc call", ("runtime.asmcgocall", "runtime.libcCall", "runtime.syscall",
                                               "runtime.entersyscall", "runtime.exitsyscall", "runtime.reentersyscall",
                                               "syscall.", "runtime.pthread_", "runtime.pread", "runtime.read_",
                                               "runtime.write", "runtime.usleep", "runtime.kevent", "internal/poll.",
                                               "os.(*File)", "runtime.cgocall", "_pthread_cond_wait",
                                               "pthread_cond_", "_pthread_", "pthread_")),
]


def group(stack):
    leaf = stack[-1]
    for label, needles in LEAVES:
        if any(n in leaf for n in needles):
            return label
    return part(stack)


def part(stack):
    for label, needles in PARTS:
        for frame in stack:
            if any(n in frame for n in needles):
                return label
    return "other (runtime, libc)"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    functions = "-functions" in sys.argv
    for path in args:
        threads, samples = parse(path)
        per = max(threads.values()) if threads else 1
        state = defaultdict(int)
        parts = defaultdict(int)
        inclusive = defaultdict(int)
        leaves = defaultdict(int)
        for thread, own, stack in samples:
            leaf = stack[-1]
            label = LEAF_OF.get(leaf)
            if label is None:
                label = "running"
                parts[group(stack)] += own
                leaves[stack[-1]] += own
                for frame in set(stack):
                    inclusive[frame] += own
            state[label] += own
        total = sum(state.values())
        print("%s: %d threads, %d samples a thread" % (path.split("/")[-1], len(threads), per))
        for label, n in sorted(state.items(), key=lambda kv: -kv[1]):
            print("  %-46s %7.2f threads" % (label, n / per))
        print("  %-46s %7.2f" % ("= all", total / per))
        print("  running, by what the leaf of the stack is:")
        for label, n in sorted(parts.items(), key=lambda kv: -kv[1]):
            print("    %-56s %6.2f threads  %5.1f %%" % (label, n / per, 100 * n / max(1, state["running"])))
        if functions:
            print("  running, by leaf function:")
            for frame, n in sorted(leaves.items(), key=lambda kv: -kv[1])[:70]:
                print("    %6.2f  %-40s %s" % (n / per, group([frame])[:40], short(frame)))


main()
