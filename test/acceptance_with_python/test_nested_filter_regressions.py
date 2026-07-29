"""End-to-end regression tests for nested object property filtering.

This file is the home for *property-oriented* regression tests — narrow
discriminators for bug fixes, focused on the wire path. Filter-shape
coverage (Equal, AND, OR, IsNull, Contains, etc.) lives in
test_nested_props_array_intermediates.py and
test_nested_props_object_intermediates.py.

Add a new test here whenever a correctness fix lands that's worth a
wire-level lock.
"""

from __future__ import annotations

from weaviate.classes.config import Configure, DataType, Property
from weaviate.collections.classes.filters import Filter

from .conftest import CollectionFactory


def test_limit_respected_on_nested_range_scan(collection_factory: CollectionFactory) -> None:
    """Limit must be honored on nested range scans even when each matching
    doc contributes many positions to the underlying bitmap.

    Before the fix, the bucket cursor for nested range / LIKE /
    Equal-via-RoaringSet scans short-circuited when the raw position
    bitmap's cardinality reached the user limit. Positions are
    (root|leaf|docID) tuples — they don't map 1:1 to docs — so a single
    doc with multiple matching nested elements could exhaust the position
    quota before later matching docs were even read, and the query
    returned far fewer docs than requested.

    Fixture: 10 docs, each with 5 cars all at the same year (year unique
    per doc, in [2011..2020]). The range scan for `cars.year > 2010`
    reads keys in ascending order; with the bug, reading key=2011 alone
    produces 5 positions (doc1's 5 cars), filling limit=5 and stopping
    the cursor — leaving docs 2..10 unread.
    """
    collection = collection_factory(
        properties=[
            Property(
                name="cars",
                data_type=DataType.OBJECT_ARRAY,
                nested_properties=[
                    Property(name="year", data_type=DataType.INT),
                ],
            ),
        ],
        vector_config=Configure.Vectors.self_provided(),
    )

    total_docs = 10
    cars_per_doc = 5
    inserted_ids: set = set()
    for i in range(1, total_docs + 1):
        year = 2010 + i  # 2011..2020, unique per doc
        cars = [{"year": year} for _ in range(cars_per_doc)]
        doc_id = collection.data.insert({"cars": cars})
        inserted_ids.add(doc_id)

    limit = 5
    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars.year").greater_than(2010),
        limit=limit,
    ).objects
    ids = {o.uuid for o in result}

    assert len(result) == limit, (
        f"limit must be honored exactly (got {len(result)}, expected {limit})"
    )
    assert ids.issubset(inserted_ids), "unexpected ids in result"
