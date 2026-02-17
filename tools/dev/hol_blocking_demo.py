#!/usr/bin/env python3 -u
"""
hol_blocking_demo.py — Proves that compaction is HOL-blocked on stable/v1.34
                        and not blocked on the async-deletion fix branch.

HOW IT WORKS
============
Within a single shard, LSM buckets (objects, property_text, property__id, …)
share a SEQUENTIAL compaction cycle (routinesLimit=1). Their callbacks run one
by one in registration order — and "objects" is always registered first.

When the objects bucket finishes compacting (switchInMemory), it calls
deleteOldSegmentsFromDisk which waits for all reader refs to drop to zero.
On stable/v1.34 this wait is SYNCHRONOUS and BLOCKS the whole cycle, so
every sibling bucket (property_text, …) never gets a turn.

This script holds a consistent view on the objects bucket (pinning refs > 0)
then checks whether the property_text bucket in the same shard can compact:

  stable/v1.34:  property_text stays at ≥2 same-level segments → HOL-BLOCKED
  fix branch:    property_text compacts those segments         → NOT BLOCKED

SEGMENT DETECTION
=================
Flushed (level-0) segments are named  segment-{nanoseconds}.db
Compacted segments are named          segment-{id1}_{id2}[.l{N}.s{M}].db

We parse the level from the filename:
  - If the name contains `.l{N}.s{M}` the level is N.
  - If the name has no such suffix and no underscore in the id portion,
    it is a fresh flush at level 0.
  - Otherwise (underscore present, no explicit level) assume level 1.

USAGE
=====
  # From the repo root, on the branch you want to test:
  python3 tools/dev/hol_blocking_demo.py

REQUIREMENTS
============
  pip install requests
"""

import collections
import glob
import os
import random
import re
import shutil
import socket
import string
import subprocess
import sys
import tempfile
import time
import uuid

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
API_PORT      = 8099
GRPC_PORT     = 50099
PPROF_PORT    = 6099
RAFT_PORT     = 8399
RAFT_RPC_PORT = 8400
API_URL       = f"http://127.0.0.1:{API_PORT}"
DEBUG_URL     = f"http://127.0.0.1:{PPROF_PORT}"

COLLECTION    = "HolBlockingDemo"

# Each object has TEXT_SIZE chars of random words.
# Random text ensures property_text's inverted index also grows.
OBJECTS_PER_BATCH = 1_000
TEXT_SIZE         = 10_000   # 10 KB per object → ~10 MB per 1000-object batch

# How long to wait for property_text to compact while view is held on objects
COMPACT_WAIT = 60   # seconds

# ---------------------------------------------------------------------------
# Weaviate helpers
# ---------------------------------------------------------------------------

def _r(method, path, body=None):
    url = f"{API_URL}{path}"
    r = getattr(requests, method)(url, json=body, timeout=30)
    if not r.ok:
        raise RuntimeError(f"{method.upper()} {path} → {r.status_code}: {r.text[:300]}")
    return r.json() if r.text else {}


def wait_ready(timeout=90):
    print(f"  Polling {API_URL}/v1/.well-known/ready ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{API_URL}/v1/.well-known/ready", timeout=2)
            if r.ok:
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("Weaviate did not become ready within timeout")


def create_collection(name):
    _r("post", "/v1/schema", {
        "class":      name,
        "vectorizer": "none",
        "properties": [{"name": "text", "dataType": ["text"]}],
    })


def random_text(n):
    """Random lowercase words so every object has unique tokens."""
    words = []
    total = 0
    while total < n:
        wlen = random.randint(4, 10)
        words.append(''.join(random.choices(string.ascii_lowercase, k=wlen)))
        total += wlen + 1
    return ' '.join(words)[:n]


def import_batch(col, n):
    objects = [
        {"class": col, "id": str(uuid.uuid4()),
         "properties": {"text": random_text(TEXT_SIZE)}}
        for _ in range(n)
    ]
    _r("post", "/v1/batch/objects", {"objects": objects})


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def segment_level(filename):
    """
    Parse the compaction level from a segment filename.

    Filename formats:
      segment-{nanoseconds}.db               → fresh flush, level 0
      segment-{id1}_{id2}.db                 → compacted, level 1 (no explicit info)
      segment-{id1}_{id2}.l{N}.s{M}.db       → compacted, explicit level N
    """
    base = os.path.basename(filename)
    # Explicit level embedded in name (WriteSegmentInfoIntoFileName=true)
    m = re.search(r'\.l(\d+)\.s\d+\.db$', base)
    if m:
        return int(m.group(1))
    # Strip .db suffix, then check if the id portion contains '_'
    stem = base[len("segment-"):].split(".")[0]   # everything after "segment-" up to first "."
    if "_" in stem:
        return 1   # compacted but no explicit level → level 1
    return 0       # pure numeric timestamp → fresh flush, level 0


def segments_per_level(data_path, col, bucket):
    """
    Return a Counter mapping level → count for all on-disk segments
    (ignoring .deleteme and .tmp files).
    """
    pattern = os.path.join(data_path, col.lower(), "*", "lsm", bucket, "*.db")
    files = glob.glob(pattern)
    # Exclude .db.deleteme (they contain an extra dot)
    files = [f for f in files if f.endswith(".db") and not f.endswith(".deleteme")]
    c = collections.Counter()
    for f in files:
        c[segment_level(f)] += 1
    return c


def max_same_level(data_path, col, bucket):
    """Maximum number of segments at any single level (compaction eligibility)."""
    lvls = segments_per_level(data_path, col, bucket)
    return max(lvls.values(), default=0)


def get_shard_name(data_path, col):
    col_path = os.path.join(data_path, col.lower())
    entries = [e for e in os.listdir(col_path)
               if os.path.isdir(os.path.join(col_path, e))]
    if not entries:
        raise RuntimeError(f"No shard directory found under {col_path}")
    return entries[0]


# ---------------------------------------------------------------------------
# Debug endpoint helpers
# ---------------------------------------------------------------------------

def hold_view(col, shard, bucket):
    url = f"{DEBUG_URL}/debug/consistent-view/{col}/shards/{shard}/buckets/{bucket}"
    r = requests.post(url, timeout=5)
    if not r.ok:
        raise RuntimeError(f"hold_view failed: {r.status_code} {r.text}")
    data = r.json()
    print(f"  [debug] Held view on {col}/{shard}/{bucket}: {data}")
    return data


def release_view(col, shard, bucket):
    url = f"{DEBUG_URL}/debug/consistent-view/{col}/shards/{shard}/buckets/{bucket}"
    r = requests.delete(url, timeout=5)
    if not r.ok:
        raise RuntimeError(f"release_view failed: {r.status_code} {r.text}")
    print(f"  [debug] Released view on {col}/{shard}/{bucket}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    data_path = tempfile.mkdtemp(prefix="weaviate-hol-demo-")

    banner = "=" * 70
    print(f"\n{banner}")
    print("  HOL-Blocking Compaction Demo")
    print(f"  Repo:     {REPO_ROOT}")
    print(f"  Data dir: {data_path}")
    print(f"  API port: {API_PORT}   pprof port: {PPROF_PORT}")
    print(f"{banner}\n")

    # Pre-flight: ensure ports are free
    for port in (API_PORT, PPROF_PORT, RAFT_PORT, RAFT_RPC_PORT, GRPC_PORT):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                print(f"ERROR: port {port} is already in use. Kill it first.")
                sys.exit(1)

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------
    binary = os.path.join(data_path, "weaviate-server")
    print("Step 1: Building weaviate-server ...")
    t0 = time.time()
    result = subprocess.run(
        ["go", "build", "-o", binary, "./cmd/weaviate-server"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("BUILD FAILED:\n", result.stderr)
        sys.exit(1)
    print(f"  Build OK ({time.time()-t0:.1f}s)\n")

    # ------------------------------------------------------------------
    # Start server
    # ------------------------------------------------------------------
    env = {
        **os.environ,
        "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
        "PERSISTENCE_DATA_PATH": data_path,
        # Cluster / RAFT (isolated single-node)
        "CLUSTER_HOSTNAME":         "hol-demo",
        "CLUSTER_GOSSIP_BIND_PORT": "7299",
        "CLUSTER_DATA_BIND_PORT":   "7300",
        "RAFT_BOOTSTRAP_EXPECT":    "1",
        "RAFT_PORT":                str(RAFT_PORT),
        "RAFT_INTERNAL_RPC_PORT":   str(RAFT_RPC_PORT),
        "GRPC_PORT":                str(GRPC_PORT),
        # Misc
        "PROMETHEUS_MONITORING_ENABLED": "false",
        "DISABLE_TELEMETRY":             "true",
        "GO_PROFILING_PORT":             str(PPROF_PORT),
    }

    log_path = os.path.join(data_path, "weaviate.log")
    log_file = open(log_path, "w")

    print("Step 2: Starting Weaviate ...")
    proc = subprocess.Popen(
        [binary, "--scheme", "http", "--host", "127.0.0.1",
         "--port", str(API_PORT),
         "--read-timeout=600s", "--write-timeout=600s"],
        env=env, stdout=log_file, stderr=log_file,
    )

    try:
        wait_ready(timeout=90)
        print("  Ready.\n")

        # ------------------------------------------------------------------
        # Create collection
        # ------------------------------------------------------------------
        print(f"Step 3: Creating collection {COLLECTION} ...")
        create_collection(COLLECTION)
        print("  Done.\n")

        # ------------------------------------------------------------------
        # Import batches until BOTH buckets have ≥2 segments at the SAME
        # level.  Same-level segments are eligible for compaction — holding
        # refs on those segments will block deleteOldSegmentsFromDisk and
        # therefore the whole sequential compaction cycle for this shard.
        # ------------------------------------------------------------------
        print("Step 4: Importing until both buckets have ≥2 same-level segments ...")
        print(f"  (each batch: {OBJECTS_PER_BATCH} objects × {TEXT_SIZE} chars "
              f"≈ {OBJECTS_PER_BATCH * TEXT_SIZE // 1_000_000} MB)\n")

        shard = None
        batch_num = 0
        while True:
            batch_num += 1
            print(f"  Batch {batch_num}: importing {OBJECTS_PER_BATCH} objects ...",
                  end="  ", flush=True)
            import_batch(COLLECTION, OBJECTS_PER_BATCH)

            if shard is None:
                try:
                    shard = get_shard_name(data_path, COLLECTION)
                except RuntimeError:
                    print("(shard not yet visible)", flush=True)
                    continue

            obj_lvls = segments_per_level(data_path, COLLECTION, "objects")
            txt_lvls = segments_per_level(data_path, COLLECTION, "property_text")
            obj_max  = max(obj_lvls.values(), default=0)
            txt_max  = max(txt_lvls.values(), default=0)

            print(f"objects={dict(obj_lvls)}  property_text={dict(txt_lvls)}",
                  flush=True)

            if obj_max >= 2 and txt_max >= 2:
                print(f"\n  Both buckets have ≥2 same-level segments — ready.\n")
                break

            if batch_num >= 50:
                raise RuntimeError(
                    "Could not build ≥2 same-level segments in either bucket "
                    "after 50 batches. Check server logs."
                )

        # ------------------------------------------------------------------
        # Hold a consistent view on objects bucket.
        #
        # The sequential cycle processes "objects" first.  With held refs:
        #
        #   stable/v1.34:  objects compacts, then BLOCKS the entire cycle in
        #                  deleteOldSegmentsFromDisk(waitForReferenceCountToReachZero)
        #                  → property_text callback NEVER RUNS.
        #
        #   fix branch:    objects compacts, dispatches deletion to goroutine,
        #                  returns immediately → property_text CAN compact.
        # ------------------------------------------------------------------
        print(f"Step 5: Holding consistent view on objects bucket ...")
        hold_view(COLLECTION, shard, "objects")
        print()

        # ------------------------------------------------------------------
        # Measure: does property_text compact while the view is held?
        # ------------------------------------------------------------------
        initial_txt_lvls = segments_per_level(data_path, COLLECTION, "property_text")
        initial_max = max(initial_txt_lvls.values(), default=0)
        print(f"Step 6: Checking whether property_text compacts "
              f"(levels={dict(initial_txt_lvls)}, timeout={COMPACT_WAIT}s) ...\n")

        t_start      = time.time()
        deadline     = t_start + COMPACT_WAIT
        txt_compacted = False

        print(f"  {'elapsed':>8}  {'obj levels':>20}  {'txt levels':>20}")
        while time.time() < deadline:
            obj_lvls = segments_per_level(data_path, COLLECTION, "objects")
            txt_lvls = segments_per_level(data_path, COLLECTION, "property_text")
            txt_max  = max(txt_lvls.values(), default=0)
            elapsed  = time.time() - t_start
            print(f"  [{elapsed:6.1f}s]  {str(dict(obj_lvls)):>20}  "
                  f"{str(dict(txt_lvls)):>20}")
            if txt_max < initial_max:
                txt_compacted = True
                print(f"\n  => property_text COMPACTED in {elapsed:.1f}s  "
                      f"(max same-level {initial_max} → {txt_max})")
                break
            time.sleep(0.5)

        if not txt_compacted:
            print(f"\n  => property_text did NOT compact within {COMPACT_WAIT}s "
                  f"while view is held on objects.")
            print(f"     This is the HOL-blocking bug.\n")

        # ------------------------------------------------------------------
        # Release the view; verify property_text compacts
        # ------------------------------------------------------------------
        print(f"\nStep 7: Releasing view on objects ...")
        release_view(COLLECTION, shard, "objects")

        deadline2 = time.time() + 30
        while time.time() < deadline2:
            txt_lvls = segments_per_level(data_path, COLLECTION, "property_text")
            if max(txt_lvls.values(), default=0) <= 1:
                print(f"  property_text compacted after release (levels={dict(txt_lvls)})")
                break
            time.sleep(0.5)

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        print(f"\n{banner}")
        if txt_compacted:
            print("  RESULT: ✓ PASS — property_text compacted while view was held"
                  " on objects.")
            print("           The fix is working: deletion is async, no HOL blocking.")
        else:
            print("  RESULT: ✗ FAIL — property_text was HOL-blocked by the view on"
                  " objects.")
            print("           stable/v1.34 behaviour confirmed.")
        print(f"{banner}\n")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback; traceback.print_exc()
        print(f"\nLast 20 lines of server log ({log_path}):")
        try:
            with open(log_path) as f:
                lines = f.readlines()
            for line in lines[-20:]:
                print(" ", line.rstrip())
        except Exception:
            pass
        sys.exit(1)

    finally:
        print("Stopping server ...")
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
        log_file.close()
        shutil.rmtree(data_path, ignore_errors=True)


if __name__ == "__main__":
    main()
