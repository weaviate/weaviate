#!/usr/bin/env python3 -u
"""
hol_blocking_demo.py — Demonstrates PERSISTENCE_LSM_MAX_PENDING_ASYNC_DELETIONS.

HOW IT WORKS
============
Within a single shard, LSM buckets (objects, property_text, property__id, …)
share a SEQUENTIAL compaction cycle (routinesLimit=1).  Their callbacks run one
by one in registration order — "objects" is always first.

With limit=0 (old synchronous behaviour / stable/v1.34):
  When objects finishes a merge, it calls deleteOldSegmentsFromDisk which waits
  for all reader refs to reach zero — synchronously.  If a consistent view is
  held on objects, this wait never returns, and every sibling bucket (including
  property_text) is HOL-blocked for the duration.

With limit>0 (async deletion fix):
  The deletion is dispatched to a background goroutine up to `limit` times.
  The compaction cycle returns immediately, and property_text gets its turn.

SIGNAL
======
We hold a consistent view on the objects bucket from the moment its very first
level-0 segment appears, then import 30 batches while watching both buckets.

  limit=0  → objects compacts ONCE (then the sync deletion blocks forever on
             the pinned ref), property_text never compacts.   HOL-BLOCKED.
  limit>0  → Both buckets accumulate multiple compactions.   NOT BLOCKED.

A "compaction" is visible as a level-1+ segment on disk (merge output files).
Segment files are named:
  segment-{nanoseconds}.db              fresh flush, level 0
  segment-{id1}_{id2}[.l{N}.s{M}].db   merged, level N (or 1 if no explicit N)
.deleteme files (old inputs awaiting deletion) are excluded from the count.

PASS/FAIL
=========
  PASS  if property_text completed ≥1 compaction after the view was held.
  FAIL  if property_text completed 0 compactions (HOL-blocked).

USAGE
=====
  # Default limit (10 = async deletion enabled):
  python3 tools/dev/hol_blocking_demo.py

  # Disable async deletion (old synchronous behaviour):
  PERSISTENCE_LSM_MAX_PENDING_ASYNC_DELETIONS=0 python3 tools/dev/hol_blocking_demo.py

  # Custom limit:
  PERSISTENCE_LSM_MAX_PENDING_ASYNC_DELETIONS=3 python3 tools/dev/hol_blocking_demo.py

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

IMPORT_BATCHES = 30

# Read the limit from the environment (mirrors what the server will use).
ASYNC_LIMIT = int(os.environ.get("PERSISTENCE_LSM_MAX_PENDING_ASYNC_DELETIONS", "10"))

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


def higher_level_count(levels):
    """Number of segments at level >= 1 (each represents at least one completed merge)."""
    return sum(cnt for lvl, cnt in levels.items() if lvl >= 1)


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
    limit_desc = "all-sync (old behaviour)" if ASYNC_LIMIT == 0 else f"async up to {ASYNC_LIMIT} goroutines"
    print(f"\n{banner}")
    print("  HOL-Blocking Compaction Demo")
    print(f"  Repo:        {REPO_ROOT}")
    print(f"  Data dir:    {data_path}")
    print(f"  API port:    {API_PORT}   pprof port: {PPROF_PORT}")
    print(f"  Async limit: {ASYNC_LIMIT}  ({limit_desc})")
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
        "PERSISTENCE_LSM_MAX_PENDING_ASYNC_DELETIONS": str(ASYNC_LIMIT),
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
        # Step 4: Import 30 batches, hold view at first level-0 segment.
        #
        # We hold a consistent view on the objects bucket as soon as its
        # first level-0 segment appears.  This pins that segment's ref-count
        # so that any deletion touching it will block.
        #
        # After holding the view we count how many compactions complete per
        # bucket (increases in level-1+ segment count).  The results reveal
        # whether the compaction cycle was HOL-blocked:
        #
        #   limit=0  → objects: 1 compaction, property_text: 0 compactions
        #              (sync deletion blocks the whole cycle on the pinned ref)
        #   limit>0  → both buckets accumulate multiple compactions
        #              (async deletion unblocks the cycle for sibling buckets)
        # ------------------------------------------------------------------
        print(f"Step 4: Importing {IMPORT_BATCHES} batches "
              f"({OBJECTS_PER_BATCH} objects × {TEXT_SIZE} chars each).")
        print(f"  View held on objects at its first level-0 segment.\n")
        print(f"  {'batch':>5}  {'objects levels':>28}  {'txt levels':>28}  notes")
        print(f"  {'-'*5}  {'-'*28}  {'-'*28}  -----")

        shard = None
        view_held = False
        # Snapshot of compaction counts at the moment the view was held
        obj_compact_at_hold = 0
        txt_compact_at_hold = 0
        # Running max of compactions seen after the view was held
        obj_compactions_after = 0
        txt_compactions_after = 0

        obj_final_lvls = collections.Counter()
        txt_final_lvls = collections.Counter()

        for batch_num in range(1, IMPORT_BATCHES + 1):
            import_batch(COLLECTION, OBJECTS_PER_BATCH)

            if shard is None:
                try:
                    shard = get_shard_name(data_path, COLLECTION)
                except RuntimeError:
                    print(f"  {batch_num:>5}  (shard not yet visible)")
                    continue

            obj_lvls = segments_per_level(data_path, COLLECTION, "objects")
            txt_lvls = segments_per_level(data_path, COLLECTION, "property_text")
            obj_final_lvls = obj_lvls
            txt_final_lvls = txt_lvls

            obj_c = higher_level_count(obj_lvls)
            txt_c = higher_level_count(txt_lvls)

            notes = []

            # Hold the view as soon as objects has its first level-0 segment.
            # This pins that segment before a second one appears, ensuring the
            # compaction pair contains a ref-held segment.
            if not view_held and obj_lvls.get(0, 0) >= 1:
                hold_view(COLLECTION, shard, "objects")
                view_held = True
                obj_compact_at_hold = obj_c
                txt_compact_at_hold = txt_c
                notes.append("VIEW HELD")

            if view_held:
                new_obj = obj_c - obj_compact_at_hold
                new_txt = txt_c - txt_compact_at_hold
                if new_obj > obj_compactions_after:
                    delta = new_obj - obj_compactions_after
                    notes.append(f"+{delta} obj compact")
                    obj_compactions_after = new_obj
                if new_txt > txt_compactions_after:
                    delta = new_txt - txt_compactions_after
                    notes.append(f"+{delta} txt compact")
                    txt_compactions_after = new_txt

            print(f"  {batch_num:>5}  {str(dict(obj_lvls)):>28}  "
                  f"{str(dict(txt_lvls)):>28}  {' | '.join(notes)}", flush=True)

        if not view_held:
            raise RuntimeError("objects never produced a level-0 segment in 30 batches")

        # ------------------------------------------------------------------
        # Step 5: Report
        # ------------------------------------------------------------------
        print(f"\nStep 5: Results after {IMPORT_BATCHES} batches "
              f"(limit={ASYNC_LIMIT}):\n")
        print(f"  Final objects     levels : {dict(obj_final_lvls)}")
        print(f"  Final txt index   levels : {dict(txt_final_lvls)}")
        print()
        print(f"  Compactions in objects     after view held: {obj_compactions_after}")
        print(f"  Compactions in txt index   after view held: {txt_compactions_after}")

        if ASYNC_LIMIT == 0:
            print(f"\n  Expected (limit=0): objects=1, txt=0  "
                  f"(sync deletion HOL-blocks the cycle)")
        else:
            print(f"\n  Expected (limit={ASYNC_LIMIT}): objects>1, txt>0  "
                  f"(async deletion keeps the cycle moving)")

        # ------------------------------------------------------------------
        # Release the view
        # ------------------------------------------------------------------
        print(f"\nStep 6: Releasing view on objects ...")
        release_view(COLLECTION, shard, "objects")

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        txt_passed = txt_compactions_after >= 1

        print(f"\n{banner}")
        if txt_passed:
            print("  RESULT: PASS — property_text compacted while the view was held.")
            print(f"           Async deletion (limit={ASYNC_LIMIT}) prevents HOL blocking.")
        else:
            print("  RESULT: FAIL — property_text was HOL-blocked by the view on objects.")
            if ASYNC_LIMIT == 0:
                print("           limit=0: synchronous deletion confirmed (old behaviour).")
            else:
                print(f"           Unexpected: limit={ASYNC_LIMIT} but property_text still blocked.")
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
