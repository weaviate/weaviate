# Watches the process that grows the 10M index from outside, read-only: what
# the system accounts to it (proc_pid_rusage, proc_pidinfo), the state of its
# threads (ps -M), the counters of the internal disk (ioreg), the memory of
# the system (vm_stat, swap). One JSON line every PERIOD seconds, until the
# process is gone. It sends no signal and opens nothing of the index.
#
# Usage: python3 -I -B monitor.py <pid> <out.jsonl> [period seconds]
import ctypes, json, re, subprocess, sys, time

pid = int(sys.argv[1])
out = sys.argv[2]
period = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0

libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)


class TimebaseInfo(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


tb = TimebaseInfo()
libc.mach_timebase_info(ctypes.byref(tb))


def ns(mach):
    return mach * tb.numer // tb.denom


RUSAGE_FIELDS = [
    "user_time", "system_time", "pkg_idle_wkups", "interrupt_wkups", "pageins", "wired_size", "resident_size",
    "phys_footprint", "proc_start_abstime", "proc_exit_abstime", "child_user_time", "child_system_time",
    "child_pkg_idle_wkups", "child_interrupt_wkups", "child_pageins", "child_elapsed_abstime",
    "diskio_bytesread", "diskio_byteswritten", "cpu_time_qos_default", "cpu_time_qos_maintenance",
    "cpu_time_qos_background", "cpu_time_qos_utility", "cpu_time_qos_legacy", "cpu_time_qos_user_initiated",
    "cpu_time_qos_user_interactive", "billed_system_time", "serviced_system_time", "logical_writes",
    "lifetime_max_phys_footprint", "instructions", "cycles", "billed_energy", "serviced_energy",
    "interval_max_phys_footprint", "runnable_time",
]


class RusageInfoV4(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [(name, ctypes.c_uint64) for name in RUSAGE_FIELDS]


class ProcTaskInfo(ctypes.Structure):
    _fields_ = [
        ("virtual_size", ctypes.c_uint64), ("resident_size", ctypes.c_uint64),
        ("total_user", ctypes.c_uint64), ("total_system", ctypes.c_uint64),
        ("threads_user", ctypes.c_uint64), ("threads_system", ctypes.c_uint64),
        ("policy", ctypes.c_int32), ("faults", ctypes.c_uint32), ("pageins", ctypes.c_uint32),
        ("cow_faults", ctypes.c_uint32), ("messages_sent", ctypes.c_uint32), ("messages_received", ctypes.c_uint32),
        ("syscalls_mach", ctypes.c_uint32), ("syscalls_unix", ctypes.c_uint32), ("csw", ctypes.c_uint32),
        ("threadnum", ctypes.c_int32), ("numrunning", ctypes.c_int32), ("priority", ctypes.c_int32),
    ]


def rusage():
    info = RusageInfoV4()
    if libc.proc_pid_rusage(pid, 4, ctypes.byref(info)) != 0:
        return None
    keep = ["pkg_idle_wkups", "interrupt_wkups", "pageins", "resident_size", "phys_footprint", "diskio_bytesread",
            "diskio_byteswritten", "logical_writes", "instructions", "cycles"]
    r = {k: getattr(info, k) for k in keep}
    for k in ["user_time", "system_time", "runnable_time", "cpu_time_qos_default", "cpu_time_qos_utility",
              "cpu_time_qos_background", "cpu_time_qos_user_initiated"]:
        r[k + "_ns"] = ns(getattr(info, k))
    return r


def taskinfo():
    info = ProcTaskInfo()
    if libc.proc_pidinfo(pid, 4, ctypes.c_uint64(0), ctypes.byref(info), ctypes.sizeof(info)) != ctypes.sizeof(info):
        return None
    return {k: getattr(info, k) for k in ["faults", "pageins", "cow_faults", "syscalls_mach", "syscalls_unix", "csw",
                                           "threadnum", "numrunning"]}


def run(*cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout
    except Exception as e:  # the watch goes on without what one tool gives
        return ""


def threads():
    # ps -M: one line a thread; the state is the token after the %CPU.
    states = {}
    for line in run("ps", "-M", "-p", str(pid)).split("\n")[1:]:
        m = re.search(r"\s(\d+\.\d)\s+([A-Z][A-Za-z+<>]*)\s+\d+[A-Z]?\s", line)
        if m:
            key = m.group(2)[0]
            states[key] = states.get(key, 0) + 1
    return states


def disks():
    # The block storage drivers and their counters since boot; the internal
    # disk is the one with the most bytes.
    best = None
    for m in re.finditer(r'"Statistics" = \{([^}]*)\}', run("ioreg", "-c", "IOBlockStorageDriver", "-r", "-w", "0")):
        stats = {k: int(v) for k, v in re.findall(r'"([^"]+)"=(\d+)', m.group(1))}
        if best is None or stats.get("Bytes (Read)", 0) + stats.get("Bytes (Write)", 0) > \
                best.get("Bytes (Read)", 0) + best.get("Bytes (Write)", 0):
            best = stats
    if best is None:
        return None
    return {"readOps": best.get("Operations (Read)"), "writeOps": best.get("Operations (Write)"),
            "readBytes": best.get("Bytes (Read)"), "writeBytes": best.get("Bytes (Write)"),
            "readNs": best.get("Total Time (Read)"), "writeNs": best.get("Total Time (Write)")}


def vm():
    text = run("vm_stat")
    page = re.search(r"page size of (\d+) bytes", text)
    r = {"pageSize": int(page.group(1)) if page else 0}
    for name, value in re.findall(r'^"?([A-Za-z -]+)"?:\s+(\d+)\.', text, re.M):
        r[name.strip()] = int(value)
    swap = re.search(r"used = ([\d.]+)M", run("sysctl", "-n", "vm.swapusage"))
    if swap:
        r["swapUsedMB"] = float(swap.group(1))
    level = run("sysctl", "-n", "kern.memorystatus_vm_pressure_level").strip()
    if level.isdigit():
        r["pressureLevel"] = int(level)
    return r


with open(out, "a", buffering=1) as f:
    nxt = time.time()
    while True:
        usage = rusage()
        if usage is None:
            f.write(json.dumps({"t": time.time(), "at": time.strftime("%H:%M:%S"), "gone": True}) + "\n")
            break
        line = {"t": time.time(), "at": time.strftime("%H:%M:%S"), "rusage": usage, "task": taskinfo(),
                "threads": threads(), "disk": disks(), "vm": vm()}
        f.write(json.dumps(line) + "\n")
        nxt += period
        time.sleep(max(0.5, nxt - time.time()))
