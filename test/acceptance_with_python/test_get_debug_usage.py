from typing import Callable, List, Optional

import pytest

from . import get_debug_usage as debug_usage
from .get_debug_usage import CollectionUsage, ShardUsage
from .test_usage import vectors_rewritten

COLLECTION = "Test_settle"


def report(vector_storage_bytes: int) -> CollectionUsage:
    return CollectionUsage(
        name=COLLECTION,
        replication_factor=1,
        unique_shard_count=1,
        shards=[
            ShardUsage(
                objects_count=1000,
                objects_storage_bytes=272692,
                vector_storage_bytes=vector_storage_bytes,
                index_storage_bytes=143218,
                full_shard_storage_bytes=272692 + vector_storage_bytes + 143218,
                name="tenant",
                status="active",
            )
        ],
    )


# What a shard reports between its tenant being activated and the commit log the import wrote
# being rewritten. The rewrite is buffered under a temporary name, so the report holds still
# for as long as it runs and then drops in a single step.
ON_ACTIVATION = report(35_167_388)
REWRITTEN = report(5_721_885)

AFTER_REWRITE = vectors_rewritten(ON_ACTIVATION)


def replay(reports: List[CollectionUsage]) -> Callable[..., CollectionUsage]:
    """Serve the given reports in order, then keep serving the last one."""
    remaining = list(reports)

    def read(collection: str, *args, **kwargs) -> CollectionUsage:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return read


@pytest.mark.parametrize(
    "name,reports,accept,expected",
    [
        (
            "the shard holds still for the whole rewrite without being settled",
            [ON_ACTIVATION] * 8 + [REWRITTEN],
            AFTER_REWRITE,
            REWRITTEN,
        ),
        (
            "without a gate the same reads settle on the pre-rewrite state",
            [ON_ACTIVATION] * 8 + [REWRITTEN],
            None,
            ON_ACTIVATION,
        ),
        (
            "an accepted but still moving value is not settled either",
            [report(9_000_000), report(6_000_000), REWRITTEN],
            AFTER_REWRITE,
            REWRITTEN,
        ),
    ],
)
def test_settle_waits_for_accepted_reads(
    monkeypatch,
    name: str,
    reports: List[CollectionUsage],
    accept: Optional[Callable[[CollectionUsage], bool]],
    expected: CollectionUsage,
):
    monkeypatch.setattr(debug_usage, "get_debug_usage_for_collection", replay(reports))

    settled = debug_usage.get_settled_debug_usage_for_collection(COLLECTION, accept=accept, delay=0)

    assert settled == expected, name


def test_settle_times_out_when_never_accepted(monkeypatch):
    monkeypatch.setattr(debug_usage, "get_debug_usage_for_collection", replay([ON_ACTIVATION]))

    with pytest.raises(TimeoutError):
        debug_usage.get_settled_debug_usage_for_collection(
            COLLECTION, accept=AFTER_REWRITE, delay=0, timeout=0
        )


@pytest.mark.parametrize(
    "name,vector_storage_bytes,accepted",
    [
        ("the state the shard was activated in is rejected", 35_167_388, False),
        ("so is a value only marginally below it", 34_939_050, False),
        ("half of it is still not enough", 17_583_694, False),
        ("below half is the rewritten shard", 17_583_693, True),
        ("as is the size the rewrite was measured to leave", 5_721_885, True),
    ],
)
def test_vectors_rewritten(name: str, vector_storage_bytes: int, accepted: bool):
    assert AFTER_REWRITE(report(vector_storage_bytes)) == accepted, name
