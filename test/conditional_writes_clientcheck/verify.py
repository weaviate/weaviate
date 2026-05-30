"""
Client-side verification of Weaviate conditional writes.

Phase 1: insert_if_not_exists existence-check (new->inserted, repeat->skipped,
         count stays 1, concurrent burst -> exactly one winner).

Write-path note
---------------
The official Weaviate Python client v4 has no native ?condition= parameter
support on its data-insert methods. First-class Python-client conditional
support is tracked as a separate-repo follow-on task in the weaviate-python-
client repository (tracked follow-on: add conditional= kwarg to data.insert).

This script uses the Python client for:
  - Cluster connect and readiness check
  - Collection schema create / delete
  - Aggregate count queries

For the conditional write itself it issues an httpx POST directly to the same
REST endpoint (POST /v1/objects?condition=insert_if_not_exists) that the
Python client talks to. Both the Python client and httpx target the same
cluster URL, so this is a genuine client-side Python verification.

Extensibility
-------------
A --mode argument selects the check suite. Default is "insert_if_not_exists"
(Phase 1). Pass --mode version for version-CAS / If-Match checks once
Phase 2 is implemented.

Usage
-----
    uv run verify.py --url http://localhost:8080 --grpc localhost:50051
    uv run verify.py --url http://node1:8080 --grpc node1:50051 [--rf 3]
    uv run verify.py --url http://localhost:8080 --grpc localhost:50051 \\
        --mode insert_if_not_exists --burst-threads 20 --burst-uuids 5
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Graceful import of runtime deps with a helpful skip message.
# The weaviate-client and httpx packages are only available after `uv sync`.
# ---------------------------------------------------------------------------
try:
    import httpx
    import weaviate
    import weaviate.classes as wvc
    from weaviate.classes.config import Configure, DataType, Property
    _CLIENT_AVAILABLE = True
except ImportError:
    _CLIENT_AVAILABLE = False


# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------
_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


def _ok(msg: str) -> None:
    print(f"{_GREEN}  PASS{_RESET}  {msg}")


def _fail(msg: str) -> None:
    print(f"{_RED}  FAIL{_RESET}  {msg}")


def _info(msg: str) -> None:
    print(f"        {msg}")


# ---------------------------------------------------------------------------
# Result accumulator
# ---------------------------------------------------------------------------
class Results:
    def __init__(self) -> None:
        self._checks: list[tuple[str, bool, str]] = []

    def record(self, name: str, passed: bool, detail: str = "") -> None:
        self._checks.append((name, passed, detail))
        if passed:
            _ok(name)
        else:
            _fail(name)
        if detail:
            _info(detail)

    def summary(self) -> tuple[int, int]:
        """Return (passed, failed) counts."""
        passed = sum(1 for _, ok, _ in self._checks if ok)
        return passed, len(self._checks) - passed

    def exit_code(self) -> int:
        _, failed = self.summary()
        return 0 if failed == 0 else 1


# ---------------------------------------------------------------------------
# Low-level HTTP helper for conditional writes
# ---------------------------------------------------------------------------

def _conditional_insert(
    base_url: str,
    collection_name: str,
    object_id: str,
    properties: dict[str, Any],
    *,
    condition: str = "insert_if_not_exists",
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    """
    POST /v1/objects?condition=<condition> via httpx.

    Returns (http_status_code, parsed_response_body).
    Raises httpx.HTTPError on network failure.
    """
    url = f"{base_url.rstrip('/')}/v1/objects?condition={condition}"
    payload = {
        "class": collection_name,
        "id": object_id,
        "properties": properties,
    }
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(url, json=payload)
    try:
        body = resp.json()
    except Exception:
        body = {}
    return resp.status_code, body


# ---------------------------------------------------------------------------
# Phase-1: insert_if_not_exists suite
# ---------------------------------------------------------------------------

def run_insert_if_not_exists(
    client: "weaviate.WeaviateClient",
    base_url: str,
    *,
    rf: int,
    burst_threads: int,
    burst_uuids: int,
    results: Results,
) -> None:
    """
    Exercises the insert_if_not_exists condition from the Python client side.

    Checks:
      1. First write of a UUID -> HTTP 201 + outcome=inserted
      2. Repeat of the same UUID -> HTTP 200 + outcome=skipped (NOT overwrite,
         NOT error)
      3. Aggregate count for that UUID's collection == 1 (exactly one object)
      4. Concurrent burst: N threads each inserting the SAME UUID M times ->
         total count for each UUID remains 1 (exactly one winner per UUID)
    """
    collection_name = f"CWClientCheck{int(time.time())}"
    print(f"\n  Collection: {collection_name}  (RF={rf})")

    # -- Create collection via Python client ----------------------------------
    try:
        replication_cfg = Configure.replication(factor=rf)
        collection = client.collections.create(
            name=collection_name,
            replication_config=replication_cfg,
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                Property(name="testfield", data_type=DataType.TEXT),
            ],
        )
        results.record(
            "Collection created via Python client",
            passed=True,
            detail=f"name={collection_name} rf={rf}",
        )
    except Exception as exc:
        results.record(
            "Collection created via Python client",
            passed=False,
            detail=f"{exc}",
        )
        return

    try:
        _run_insert_checks(
            collection=collection,
            base_url=base_url,
            collection_name=collection_name,
            burst_threads=burst_threads,
            burst_uuids=burst_uuids,
            results=results,
        )
    finally:
        try:
            client.collections.delete(collection_name)
        except Exception:
            pass


def _run_insert_checks(
    collection: Any,
    base_url: str,
    collection_name: str,
    burst_threads: int,
    burst_uuids: int,
    results: Results,
) -> None:
    """Inner check loop; called inside a finally so cleanup always runs."""

    # Check 1: first write -> 201 + outcome=inserted
    first_uuid = str(uuid.uuid4())
    status, body = _conditional_insert(
        base_url, collection_name, first_uuid,
        {"testfield": "value-for-first"},
    )
    outcome = _extract_outcome(body)
    results.record(
        "First write: HTTP 201 + outcome=inserted",
        passed=(status == 201 and outcome == "inserted"),
        detail=f"status={status} outcome={outcome!r} body={json.dumps(body)[:200]}",
    )

    # Check 2: repeat of the same UUID -> 200 + outcome=skipped
    status2, body2 = _conditional_insert(
        base_url, collection_name, first_uuid,
        {"testfield": "value-overwrite-attempt"},
    )
    outcome2 = _extract_outcome(body2)
    results.record(
        "Repeat same UUID: HTTP 200 + outcome=skipped",
        passed=(status2 == 200 and outcome2 == "skipped"),
        detail=f"status={status2} outcome={outcome2!r} body={json.dumps(body2)[:200]}",
    )

    # Check 3: aggregate count == 1 via Python client
    count = _aggregate_count(collection)
    results.record(
        "Aggregate count == 1 after two attempts on same UUID",
        passed=(count == 1),
        detail=f"count={count} (expected 1)",
    )

    # Check 4: concurrent burst - multiple threads each insert the same K UUIDs
    burst_uids = [str(uuid.uuid4()) for _ in range(burst_uuids)]
    print(
        f"\n  Concurrent burst: {burst_threads} threads x {burst_uuids} UUIDs "
        f"({burst_threads * burst_uuids} total requests)"
    )

    def _burst_insert(uid: str) -> tuple[str, int, str]:
        status, body = _conditional_insert(
            base_url, collection_name, uid,
            {"testfield": f"burst-{uid}"},
        )
        return uid, status, _extract_outcome(body)

    # One round-trip per UUID per thread; all threads race on the same set
    with ThreadPoolExecutor(max_workers=burst_threads) as pool:
        futures = [
            pool.submit(_burst_insert, uid)
            for _ in range(burst_threads)
            for uid in burst_uids
        ]
        burst_results: list[tuple[str, int, str]] = []
        for f in as_completed(futures):
            try:
                burst_results.append(f.result())
            except Exception as exc:
                _info(f"burst thread exception: {exc}")

    # Tally inserted / skipped per UUID
    inserted_counts: dict[str, int] = {uid: 0 for uid in burst_uids}
    skipped_counts: dict[str, int] = {uid: 0 for uid in burst_uids}
    for uid, st, oc in burst_results:
        if oc == "inserted":
            inserted_counts[uid] += 1
        elif oc == "skipped":
            skipped_counts[uid] += 1

    # Each UUID must have exactly one insert across all threads
    all_exactly_one = all(inserted_counts[uid] == 1 for uid in burst_uids)
    results.record(
        f"Concurrent burst: each of {burst_uuids} UUIDs inserted exactly once",
        passed=all_exactly_one,
        detail=(
            "inserted_counts=" + str(dict(list(inserted_counts.items())[:5]))
            + (" ..." if burst_uuids > 5 else "")
            + "  skipped_counts=" + str(dict(list(skipped_counts.items())[:5]))
        ),
    )

    # Verify via aggregate count: total = initial 1 (first_uuid) + burst_uuids
    expected_total = 1 + burst_uuids
    total_count = _aggregate_count(collection)
    results.record(
        f"Aggregate count after burst == {expected_total} (1 initial + {burst_uuids} burst UUIDs)",
        passed=(total_count == expected_total),
        detail=f"count={total_count} expected={expected_total}",
    )


def _extract_outcome(body: dict[str, Any]) -> str:
    """
    Pull the outcome string from either:
      {"conditional_result": {"outcome": "..."}}  (conditional write response)
      {}  (empty / error body)
    """
    return body.get("conditional_result", {}).get("outcome", "")


def _aggregate_count(collection: Any) -> int:
    """Return total_count for collection via the Python client aggregate API."""
    result = collection.aggregate.over_all(total_count=True)
    return result.total_count


# ---------------------------------------------------------------------------
# Phase-2 stub (version-CAS / If-Match) -- implement when --mode version is used
# ---------------------------------------------------------------------------

def run_version_cas(
    client: "weaviate.WeaviateClient",
    base_url: str,
    *,
    rf: int,
    results: Results,
) -> None:
    """
    Phase-2: version-CAS / If-Match checks.
    Not yet implemented; will be added in a later phase.
    """
    results.record(
        "version-CAS mode (Phase 2)",
        passed=False,
        detail="Not yet implemented. Use --mode insert_if_not_exists for Phase-1 checks.",
    )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _build_client(args: argparse.Namespace) -> "weaviate.WeaviateClient":
    """
    Build a WeaviateClient from --url and --grpc arguments.

    --url  is the full REST base URL e.g. http://localhost:8080
    --grpc is  host[:port]            e.g. localhost:50051
    """
    parsed = urlparse(args.url)
    http_host = parsed.hostname or "localhost"
    http_port = parsed.port or (443 if parsed.scheme == "https" else 8080)
    http_secure = parsed.scheme == "https"

    grpc_parts = args.grpc.split(":")
    grpc_host = grpc_parts[0]
    grpc_port = int(grpc_parts[1]) if len(grpc_parts) > 1 else 50051
    grpc_secure = False

    return weaviate.connect_to_custom(
        http_host=http_host,
        http_port=http_port,
        http_secure=http_secure,
        grpc_host=grpc_host,
        grpc_port=grpc_port,
        grpc_secure=grpc_secure,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Client-side verification of Weaviate conditional writes. "
            "Uses the official Weaviate Python client (v4) for connect, schema, "
            "and count; uses httpx for the conditional write itself (no native "
            "?condition= support in the Python client yet; tracked follow-on)."
        )
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8080",
        help="REST base URL of the Weaviate node (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--grpc",
        default="localhost:50051",
        help="gRPC address host[:port] (default: localhost:50051)",
    )
    parser.add_argument(
        "--rf",
        type=int,
        default=1,
        help=(
            "Replication factor for the test collection (default: 1). "
            "Use 3 when pointing at a 3-node RF3 cluster."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["insert_if_not_exists", "version"],
        default="insert_if_not_exists",
        help=(
            "Which check suite to run (default: insert_if_not_exists). "
            "'version' is reserved for Phase-2 version-CAS checks."
        ),
    )
    parser.add_argument(
        "--burst-threads",
        type=int,
        default=10,
        help="Number of concurrent threads in the burst test (default: 10)",
    )
    parser.add_argument(
        "--burst-uuids",
        type=int,
        default=5,
        help=(
            "Number of distinct UUIDs each burst thread races to insert "
            "(default: 5)"
        ),
    )
    args = parser.parse_args()

    # -- Client availability check -------------------------------------------
    if not _CLIENT_AVAILABLE:
        print(
            f"{_YELLOW}SKIP{_RESET}  weaviate-client is not installed. "
            "Run: uv sync  (or: pip install weaviate-client)"
        )
        return 2

    print(f"\nWeaviate conditional-write client-side verifier")
    print(f"  REST URL : {args.url}")
    print(f"  gRPC addr: {args.grpc}")
    print(f"  RF       : {args.rf}")
    print(f"  Mode     : {args.mode}")
    print(f"  Write-path: httpx -> POST /v1/objects?condition=<condition>")
    print(f"  Python-client: connect / schema / aggregate count")

    results = Results()

    # -- Connect -------------------------------------------------------------
    try:
        client = _build_client(args)
    except Exception as exc:
        print(f"{_RED}FATAL{_RESET}  Could not build Weaviate client: {exc}")
        traceback.print_exc()
        return 1

    try:
        # -- Readiness check -------------------------------------------------
        try:
            ready = client.is_ready()
            results.record(
                "Cluster is ready (client.is_ready())",
                passed=ready,
                detail=f"is_ready()={ready}",
            )
            if not ready:
                print(f"{_RED}FATAL{_RESET}  Cluster not ready; aborting.")
                return 1
        except Exception as exc:
            results.record(
                "Cluster is ready (client.is_ready())",
                passed=False,
                detail=f"{exc}",
            )
            return 1

        # -- Mode dispatch ---------------------------------------------------
        if args.mode == "insert_if_not_exists":
            run_insert_if_not_exists(
                client,
                args.url,
                rf=args.rf,
                burst_threads=args.burst_threads,
                burst_uuids=args.burst_uuids,
                results=results,
            )
        elif args.mode == "version":
            run_version_cas(client, args.url, rf=args.rf, results=results)

    finally:
        try:
            client.close()
        except Exception:
            pass

    # -- Summary -------------------------------------------------------------
    passed, failed = results.summary()
    print(f"\n{'='*60}")
    print(f"  Result: {passed} PASS  /  {failed} FAIL")
    print(f"{'='*60}\n")

    return results.exit_code()


if __name__ == "__main__":
    sys.exit(main())
