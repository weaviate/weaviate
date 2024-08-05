import pytest

from weaviate.classes.config import Configure
from weaviate.classes.query import TargetVectors
from weaviate.collections.classes.grpc import PROPERTIES, TargetVectorJoinType
from weaviate.types import UUID

from .conftest import CollectionFactory

UUID1 = "00000000-0000-0000-0000-000000000001"


@pytest.mark.parametrize(
    "target_vector",
    [
        ["first", "second"],
        TargetVectors.sum(["first", "second"]),
        TargetVectors.minimum(["first", "second"]),
        TargetVectors.average(["first", "second"]),
        TargetVectors.manual_weights({"first": 1.2, "second": 0.7}),
        TargetVectors.relative_score({"first": 1.2, "second": 0.7}),
    ],
)
def test_multi_target_near_vector(
    collection_factory: CollectionFactory, target_vector: TargetVectorJoinType
) -> None:
    collection = collection_factory(
        properties=[],
        vectorizer_config=[
            Configure.NamedVectors.none("first"),
            Configure.NamedVectors.none("second"),
        ],
    )

    uuid1 = collection.data.insert({}, vector={"first": [1, 0, 0], "second": [0, 1, 0]})
    uuid2 = collection.data.insert({}, vector={"first": [0, 1, 0], "second": [1, 0, 0]})

    objs = collection.query.near_vector([1.0, 0.0, 0.0], target_vector=target_vector).objects
    assert sorted([obj.uuid for obj in objs]) == sorted([uuid1, uuid2])  # order is not guaranteed


@pytest.mark.parametrize(
    "target_vector",
    [
        ["first", "second"],
        TargetVectors.sum(["first", "second"]),
        TargetVectors.minimum(["first", "second"]),
        TargetVectors.average(["first", "second"]),
        TargetVectors.manual_weights({"first": 1.2, "second": 0.7}),
        TargetVectors.relative_score({"first": 1.2, "second": 0.7}),
    ],
)
def test_multi_target_near_object(
    collection_factory: CollectionFactory, target_vector: TargetVectorJoinType
) -> None:
    collection = collection_factory(
        properties=[],
        vectorizer_config=[
            Configure.NamedVectors.none("first"),
            Configure.NamedVectors.none("second"),
        ],
    )

    uuid1 = collection.data.insert({}, vector={"first": [1, 0], "second": [0, 1, 0]})
    uuid2 = collection.data.insert({}, vector={"first": [0, 1], "second": [1, 0, 0]})

    objs = collection.query.near_object(uuid1, target_vector=target_vector).objects
    assert sorted([obj.uuid for obj in objs]) == sorted([uuid1, uuid2])  # order is not guaranteed
