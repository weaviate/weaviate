import uuid
from typing import List

import pytest

from weaviate.classes.config import Configure, Property, DataType
from weaviate.classes.query import TargetVectors
from weaviate.collections.classes.grpc import PROPERTIES, TargetVectorJoinType
from weaviate.types import UUID

from .conftest import CollectionFactory

UUID1 = uuid.uuid4()
UUID2 = uuid.uuid4()
UUID3 = uuid.uuid4()
UUID4 = uuid.uuid4()


@pytest.mark.parametrize(
    "query", ["banana", "car"]
)  # does not matter if a result is found for bm25
@pytest.mark.parametrize(
    "vector,expected,distance",
    [
        ([1, 0, 0], [UUID1, UUID2, UUID4], 1.5),
        ({"first": [1, 0, 0], "second": [0, 1, 0]}, [UUID1], 0.5),
    ],
)
def test_multi_target_near_vector(
    collection_factory: CollectionFactory,
    vector: List[int],
    expected: List[uuid.UUID],
    distance: float,
    query: str,
) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=[
            Configure.NamedVectors.none("first"),
            Configure.NamedVectors.none("second"),
        ],
    )

    collection.data.insert(
        {"name": "banana one"}, vector={"first": [1, 0, 0], "second": [0, 1, 0]}, uuid=UUID1
    )
    collection.data.insert(
        {"name": "banana two"}, vector={"first": [0, 1, 0], "second": [1, 0, 0]}, uuid=UUID2
    )
    collection.data.insert(
        {"name": "banana three"}, vector={"first": [0, 1, 0], "second": [0, 0, 1]}, uuid=UUID3
    )
    collection.data.insert(
        {"name": "banana four"}, vector={"first": [1, 0, 0], "second": [0, 0, 1]}, uuid=UUID4
    )

    objs = collection.query.hybrid(
        "banana",
        vector=vector,
        target_vector=TargetVectors.sum(["first", "second"]),
        distance=distance,
    ).objects
    assert sorted([obj.uuid for obj in objs]) == sorted(expected)  # order is not guaranteed
