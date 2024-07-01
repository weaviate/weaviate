from typing import List

import pytest
from weaviate.collections.classes.config import Configure, Property, DataType
from weaviate.collections.classes.filters import Filter, _FilterValue

from .conftest import CollectionFactory


# bug in client + not supported in weaviate for filter by empty list
@pytest.mark.parametrize(
    "weaviate_filter,results,skip",
    [
        (
            Filter.by_property("textArray").equal([]),
            [1],
            True,
        ),
        (Filter.by_property("textArray", length=True).equal(0), [1], False),
        (
            Filter.by_property("textArray").not_equal([]),
            [0],
            True,
        ),
        (Filter.by_property("textArray", length=True).not_equal(0), [0], False),
    ],
)
def test_empty_list_filter(
    collection_factory: CollectionFactory,
    weaviate_filter: _FilterValue,
    results: List[int],
    skip: bool,
) -> None:
    if skip:
        pytest.skip("Not supported in this version")
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
