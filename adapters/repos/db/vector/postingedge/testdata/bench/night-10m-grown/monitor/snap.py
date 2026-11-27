# Keeps a copy of grow.json after every stretch of the 10M growth: the
# harness writes it again at the end of each, with the counters of the index
# so far (growStats), and only the last version stays on disk. Read-only;
# ends when the JSON says the run is done or the process is gone.
# Usage: python3 -I -B snap.py <grow.json> <dir> <pid>
import json, os, subprocess, sys, time
src, dst, pid = sys.argv[1], sys.argv[2], sys.argv[3]
last = None
while True:
    try:
        raw = open(src).read()
        d = json.loads(raw)
        at = d["stretches"][-1]["at"] if d.get("stretches") else None
        if at is not None and at != last:
            name = "grow-%s-%d.json" % (at.replace(":", ""), d["stretches"][-1]["inserted"])
            with open(os.path.join(dst, name), "w") as f:
                f.write(raw)
            last = at
        if d.get("done"):
            break
    except (OSError, ValueError, KeyError, IndexError):
        pass  # being written: the next look reads it whole
    if subprocess.run(["ps", "-p", pid], capture_output=True).returncode != 0:
        break
    time.sleep(20)
