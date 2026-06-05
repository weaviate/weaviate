"""Acceptance tests for automatic shard readonly recovery.

These tests verify that when disk usage drops back below the configured
DISK_USE_READONLY_PERCENTAGE threshold, shards automatically transition
from READONLY back to READY.

Prerequisites:
    Start the dedicated Weaviate instance before running these tests:

        docker compose -f docker-compose-readonly-recovery-test.yml up -d

    The instance runs on ports 8180 (HTTP) and 50151 (gRPC), backed by a
    50 MB tmpfs with DISK_USE_READONLY_PERCENTAGE=70.
"""

import time

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.config import AdditionalConfig

# Dedicated ports for the readonly recovery test instance.
HTTP_PORT = 8180
GRPC_PORT = 50151

# Serialize all tests in this file since they share a single Weaviate instance
# with limited disk.
pytestmark = pytest.mark.xdist_group(name="readonly_recovery")

# Large payload used to fill the 50 MB tmpfs.  Each object carries ~4 KB of
# text.  We write in small batches and stop as soon as READONLY is detected
# to avoid filling the disk to 100 % (which would prevent Raft from writing
# the log entries needed to delete collections during recovery).
_FILLER_TEXT = "x" * 4000


def _connect() -> weaviate.WeaviateClient:
    return weaviate.connect_to_local(
        port=HTTP_PORT,
        grpc_port=GRPC_PORT,
        additional_config=AdditionalConfig(timeout=(30, 60)),
    )


def _get_shard_statuses(client: weaviate.WeaviateClient, collection_name: str) -> list[str]:
    """Return the status string of every shard in the collection."""
    shards = client.collections.get(collection_name).config.get_shards()
    return [s.status if isinstance(s.status, str) else s.status.value for s in shards]


def _wait_for_shard_status(
    client: weaviate.WeaviateClient,
    collection_name: str,
    expected_status: str,
    timeout_s: float = 30,
    poll_interval_s: float = 0.5,
) -> None:
    """Poll until *all* shards of the collection reach the expected status."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        statuses = _get_shard_statuses(client, collection_name)
        if statuses and all(s == expected_status for s in statuses):
            return
        time.sleep(poll_interval_s)
    statuses = _get_shard_statuses(client, collection_name)
    raise AssertionError(
        f"Timed out waiting for shards to reach '{expected_status}'. "
        f"Current statuses: {statuses}"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    """A client connected to the readonly-recovery Weaviate instance."""
    c = _connect()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

CANARY_COLLECTION = "ReadonlyRecoveryCanary"
FILLER_COLLECTION = "ReadonlyRecoveryFiller"


def test_readonly_triggered_by_disk_usage(client: weaviate.WeaviateClient):
    """Fill the tmpfs beyond 70 % to trigger READONLY on all shards."""
    # Clean slate
    client.collections.delete(CANARY_COLLECTION)
    client.collections.delete(FILLER_COLLECTION)

    # Create a small canary collection that we will use later to verify recovery.
    client.collections.create(
        CANARY_COLLECTION,
        properties=[wvc.config.Property(name="note", data_type=wvc.config.DataType.TEXT)],
    )
    canary = client.collections.get(CANARY_COLLECTION)
    canary.data.insert({"note": "created before readonly"})

    # Create a filler collection and write enough data to push disk usage
    # above the 70 % threshold on the 50 MB tmpfs.
    client.collections.create(
        FILLER_COLLECTION,
        properties=[wvc.config.Property(name="payload", data_type=wvc.config.DataType.TEXT)],
    )
    filler = client.collections.get(FILLER_COLLECTION)

    # Write in small batches and check for READONLY after each one.
    # This avoids overshooting to 100% disk usage, which would prevent Raft
    # from writing log entries (making collection deletes impossible).
    batch_size = 50
    max_objects = 10000
    for batch_start in range(0, max_objects, batch_size):
        # Check if shards are already READONLY before writing more.
        statuses = _get_shard_statuses(client, FILLER_COLLECTION)
        if statuses and all(s == "READONLY" for s in statuses):
            break

        try:
            filler.data.insert_many(
                [
                    wvc.data.DataObject(properties={"payload": _FILLER_TEXT})
                    for _ in range(batch_size)
                ]
            )
        except Exception:
            # Once READONLY kicks in, inserts will fail – that is expected.
            break

    # Wait for the resource scanner (500 ms tick) to set shards to READONLY.
    _wait_for_shard_status(client, CANARY_COLLECTION, "READONLY", timeout_s=10)
    statuses = _get_shard_statuses(client, CANARY_COLLECTION)
    assert all(s == "READONLY" for s in statuses), f"Expected READONLY, got {statuses}"


def test_recovery_after_freeing_disk(client: weaviate.WeaviateClient):
    """Delete the filler collection to free disk, then verify automatic recovery."""
    # Deleting the filler collection should free most of the tmpfs, bringing
    # disk usage well below the 70 % threshold.
    client.collections.delete(FILLER_COLLECTION)

    # The resource scanner ticks every 500 ms.  Give it a few seconds to
    # detect the drop in disk usage and set shards back to READY.
    _wait_for_shard_status(client, CANARY_COLLECTION, "READY", timeout_s=30)
    statuses = _get_shard_statuses(client, CANARY_COLLECTION)
    assert all(s == "READY" for s in statuses), f"Expected READY, got {statuses}"


def test_writes_succeed_after_recovery(client: weaviate.WeaviateClient):
    """After recovery, verify that write operations succeed again."""
    canary = client.collections.get(CANARY_COLLECTION)
    # This insert must not raise – writes should be accepted after recovery.
    canary.data.insert({"note": "created after recovery"})
    count = canary.aggregate.over_all(total_count=True).total_count
    assert count >= 2, f"Expected at least 2 objects after recovery, got {count}"

    # Cleanup
    client.collections.delete(CANARY_COLLECTION)
