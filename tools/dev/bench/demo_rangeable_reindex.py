#!/usr/bin/env python3
"""
Runtime Rangeable Reindex Benchmark

Demonstrates the performance benefit of enabling indexRangeFilters at runtime.
Range queries (>, <, >=, <=) on numeric properties become dramatically faster
after the filterable→rangeable migration, while exact-match queries stay the same.

Usage:
    .venv/bin/python tools/dev/bench/demo_rangeable_reindex.py
"""

import atexit
import os
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time

import requests
import weaviate
from weaviate.classes.config import Configure, DataType, Property
from weaviate.classes.query import Filter

NUM_OBJECTS = 10_000_000
BATCH_SIZE = 10_000
QUERY_REPEATS = 3
QUERY_LIMIT = 10_000

# ── Weaviate process management ────────────────────────────────────────────

_weaviate_proc = None
_tmpdir = None


def _cleanup():
    global _weaviate_proc, _tmpdir
    if _weaviate_proc is not None:
        print("\nShutting down Weaviate…")
        _weaviate_proc.terminate()
        try:
            _weaviate_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _weaviate_proc.kill()
        _weaviate_proc = None
    if _tmpdir is not None:
        import shutil

        shutil.rmtree(_tmpdir, ignore_errors=True)
        _tmpdir = None


def _signal_handler(signum, _frame):
    _cleanup()
    sys.exit(128 + signum)


atexit.register(_cleanup)
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def build_weaviate(binary_path: str):
    print("Building Weaviate binary…")
    t0 = time.monotonic()
    subprocess.check_call(
        ["go", "build", "-o", binary_path, "./cmd/weaviate-server"],
        cwd=os.path.dirname(os.path.abspath(__file__)) + "/../../..",
    )
    elapsed = time.monotonic() - t0
    print(f"  Built in {elapsed:.1f}s")


def start_weaviate(binary_path: str, data_dir: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
        "CLUSTER_IN_LOCALHOST": "true",
        "RAFT_BOOTSTRAP_EXPECT": "1",
        "GO_PROFILING_PORT": "6060",
        "DISABLE_TELEMETRY": "true",
        "LOG_LEVEL": "error",
        "PERSISTENCE_DATA_PATH": data_dir,
    }
    print("Starting Weaviate…")
    proc = subprocess.Popen(
        [binary_path, "--scheme=http", "--host=0.0.0.0", "--port=8080"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Poll readiness
    for _ in range(120):
        time.sleep(1)
        try:
            r = requests.get("http://localhost:8080/v1/.well-known/ready", timeout=2)
            if r.status_code == 200:
                print("  Weaviate is ready.")
                return proc
        except requests.ConnectionError:
            pass
    proc.kill()
    raise RuntimeError("Weaviate failed to start within 120s")


# ── Collection setup ───────────────────────────────────────────────────────


def create_collection(client: weaviate.WeaviateClient):
    print("Creating collection RangeableBench…")
    client.collections.create(
        name="RangeableBench",
        vectorizer_config=Configure.Vectorizer.none(),
        properties=[
            Property(name="value", data_type=DataType.INT),
        ],
    )


# ── Data import ────────────────────────────────────────────────────────────


def import_data(client: weaviate.WeaviateClient):
    collection = client.collections.get("RangeableBench")
    print(f"Importing {NUM_OBJECTS:,} objects…")
    t0 = time.monotonic()

    for start in range(0, NUM_OBJECTS, BATCH_SIZE):
        end = min(start + BATCH_SIZE, NUM_OBJECTS)
        objects = [{"value": i} for i in range(start, end)]
        collection.data.insert_many(objects)

        done = end
        if done % 100_000 == 0 or done == NUM_OBJECTS:
            rate = done / (time.monotonic() - t0)
            print(f"  {done:>10,} / {NUM_OBJECTS:,}  ({rate:,.0f} obj/s)")

    elapsed = time.monotonic() - t0
    print(f"  Import complete in {elapsed:.1f}s ({NUM_OBJECTS / elapsed:,.0f} obj/s)")


# ── Benchmark ──────────────────────────────────────────────────────────────

RANGE_QUERIES = [
    ("value > 9999000", Filter.by_property("value").greater_than(9_999_000)),
    ("value < 1000", Filter.by_property("value").less_than(1_000)),
    ("value >= 5000000", Filter.by_property("value").greater_or_equal(5_000_000)),
    ("value <= 100", Filter.by_property("value").less_or_equal(100)),
    ("value > 9990000", Filter.by_property("value").greater_than(9_990_000)),
    ("value < 10000", Filter.by_property("value").less_than(10_000)),
    ("value >= 9000000", Filter.by_property("value").greater_or_equal(9_000_000)),
    ("value <= 1000000", Filter.by_property("value").less_or_equal(1_000_000)),
    (
        "value > 4999000 AND < 5001000",
        Filter.all_of(
            [
                Filter.by_property("value").greater_than(4_999_000),
                Filter.by_property("value").less_than(5_001_000),
            ]
        ),
    ),
    (
        "value >= 0 AND <= 9999999",
        Filter.all_of(
            [
                Filter.by_property("value").greater_or_equal(0),
                Filter.by_property("value").less_or_equal(9_999_999),
            ]
        ),
    ),
]

EXACT_QUERIES = [
    ("value == 0", Filter.by_property("value").equal(0)),
    ("value == 999999", Filter.by_property("value").equal(999_999)),
    ("value == 5000000", Filter.by_property("value").equal(5_000_000)),
    ("value == 9999999", Filter.by_property("value").equal(9_999_999)),
    ("value == 42", Filter.by_property("value").equal(42)),
    ("value == 1000000", Filter.by_property("value").equal(1_000_000)),
    ("value == 7777777", Filter.by_property("value").equal(7_777_777)),
    ("value == 3333333", Filter.by_property("value").equal(3_333_333)),
    ("value == 123456", Filter.by_property("value").equal(123_456)),
    ("value == 8888888", Filter.by_property("value").equal(8_888_888)),
]


def run_query(collection, filt):
    t0 = time.monotonic()
    collection.query.fetch_objects(filters=filt, limit=QUERY_LIMIT)
    return (time.monotonic() - t0) * 1000  # ms


def benchmark(client: weaviate.WeaviateClient, queries, label: str):
    collection = client.collections.get("RangeableBench")
    results = {}
    print(f"  Running {label}…")
    for name, filt in queries:
        times = [run_query(collection, filt) for _ in range(QUERY_REPEATS)]
        median = statistics.median(times)
        results[name] = median
    return results


# ── Reindex trigger ───────────────────────────────────────────────────────


def trigger_reindex():
    print("\nTriggering filterable → rangeable reindex…")
    t0 = time.monotonic()
    r = requests.get(
        "http://localhost:6060/debug/reindex/enable-rangeable",
        params={"collection": "RangeableBench", "property": "value"},
        timeout=600,
    )
    elapsed = time.monotonic() - t0
    assert r.status_code == 200, f"Reindex failed: {r.status_code} {r.text}"
    print(f"  Reindex complete in {elapsed:.1f}s")
    print(f"  Response: {r.json()}")


# ── Output ─────────────────────────────────────────────────────────────────


def print_table(title: str, before: dict, after: dict):
    print(f"\n{title}:")
    print(f"  {'Query':<40} {'Before (ms)':>12} {'After (ms)':>12} {'Speedup':>10}")
    print(f"  {'─' * 40} {'─' * 12} {'─' * 12} {'─' * 10}")
    for name in before:
        b = before[name]
        a = after[name]
        speedup = b / a if a > 0 else float("inf")
        print(f"  {name:<40} {b:>12.1f} {a:>12.1f} {speedup:>9.1f}x")


# ── Main ───────────────────────────────────────────────────────────────────


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def main():
    global _weaviate_proc, _tmpdir

    # Unbuffered stdout so progress is visible when run in background.
    sys.stdout.reconfigure(line_buffering=True)

    for port in (8080, 50051, 6060):
        if _port_in_use(port):
            print(f"ERROR: port {port} is already in use. Stop the existing process first.")
            sys.exit(1)

    _tmpdir = tempfile.mkdtemp(prefix="weaviate-bench-")
    binary_path = os.path.join(_tmpdir, "weaviate-server")
    data_dir = os.path.join(_tmpdir, "data")
    os.makedirs(data_dir)

    build_weaviate(binary_path)
    _weaviate_proc = start_weaviate(binary_path, data_dir)

    client = weaviate.connect_to_local(port=8080, grpc_port=50051)
    try:
        create_collection(client)
        import_data(client)

        print(f"\n=== Rangeable Reindex Benchmark ({NUM_OBJECTS:,} objects) ===")

        print("\nBenchmarking BEFORE reindex…")
        range_before = benchmark(client, RANGE_QUERIES, "range queries")
        exact_before = benchmark(client, EXACT_QUERIES, "exact-match queries")

        trigger_reindex()

        print("\nBenchmarking AFTER reindex…")
        range_after = benchmark(client, RANGE_QUERIES, "range queries")
        exact_after = benchmark(client, EXACT_QUERIES, "exact-match queries")

        print(f"\n{'=' * 80}")
        print(f"=== Rangeable Reindex Benchmark ({NUM_OBJECTS:,} objects) ===")
        print(f"{'=' * 80}")
        print_table("Range Queries", range_before, range_after)
        print_table("Exact-Match Queries", exact_before, exact_after)
        print()
    finally:
        client.close()


if __name__ == "__main__":
    main()
