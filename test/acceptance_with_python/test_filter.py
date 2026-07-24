from typing import Callable, List

import pytest
from weaviate.collections.classes.config import Configure, Property, DataType
from weaviate.collections.classes.filters import Filter, _FilterValue

from .conftest import CollectionFactory


# Filtering by an empty list is unsupported, and the client now rejects it at
# construction time — so the filter is built lazily via a factory, after the
# skip, to keep the skipped empty-list cases from tripping that validation while
# pytest collects the parametrize list.
@pytest.mark.parametrize(
    "make_filter,results,skip",
    [
        (lambda: Filter.by_property("textArray").equal([]), [1], True),
        (lambda: Filter.by_property("textArray", length=True).equal(0), [1], False),
        (lambda: Filter.by_property("textArray").not_equal([]), [0], True),
        (lambda: Filter.by_property("textArray", length=True).not_equal(0), [0], False),
    ],
    # Explicit ids: the default would be "<lambda>", whose angle brackets make the
    # collection_factory-derived class name invalid.
    ids=["equal_empty", "equal_len0", "not_equal_empty", "not_equal_len0"],
)
def test_empty_list_filter(
    collection_factory: CollectionFactory,
    make_filter: Callable[[], _FilterValue],
    results: List[int],
    skip: bool,
) -> None:
    if skip:
        pytest.skip("Not supported in this version")
    weaviate_filter = make_filter()
    collection = collection_factory(
        vectorizer_config=Configure.Vectorizer.none(),
        properties=[Property(name="textArray", data_type=DataType.TEXT_ARRAY)],
        inverted_index_config=Configure.inverted_index(index_property_length=True),
    )

    uuids = [
        collection.data.insert({"textArray": ["one", "two"]}),
        collection.data.insert({"textArray": []}),
    ]

    objects = collection.query.fetch_objects(filters=weaviate_filter).objects
    assert len(objects) == len(results)

    uuids = [uuids[result] for result in results]
    assert all(obj.uuid in uuids for obj in objects)
