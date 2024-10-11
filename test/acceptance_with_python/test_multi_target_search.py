import math
from typing import List

import pytest

from weaviate.classes.config import Configure
from weaviate.classes.query import TargetVectors
from weaviate.collections.classes.grpc import TargetVectorJoinType
import weaviate.classes as wvc

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


@pytest.mark.parametrize(
    "target_vector,distances",
    [
        (["first", "first", "second", "second", "third"], (0, 0)),
        (TargetVectors.sum(["first", "second", "third"]), [3, 4]),
        (TargetVectors.minimum(["first", "second", "third"]), [0, 0]),
        (TargetVectors.average(["first", "second", "third"]), [0.6, 0.8]),
        (
            TargetVectors.manual_weights({"first": [3, 2], "second": [1.5, 1], "third": 0.5}),
            [3, 6.5],
        ),
        (
            # same as above because the scores are already between 0 and 1 =>normalization does not change anything
            TargetVectors.relative_score({"first": [3, 2], "second": [1.5, 1], "third": 0.5}),
            [3, 6.5],
        ),
    ],
)
def test_multi_target_near_vector_multiple_inputs(
    collection_factory: CollectionFactory,
    target_vector: TargetVectorJoinType,
    distances: List[float],
) -> None:
    collection = collection_factory(
        properties=[],
        vectorizer_config=[
            Configure.NamedVectors.none("first"),
            Configure.NamedVectors.none("second"),
            Configure.NamedVectors.none("third"),
        ],
    )

    uuid1 = collection.data.insert(
        {}, vector={"first": [1, 0], "second": [0, 1, 0], "third": [0, 0, 0, 1]}
    )
    uuid2 = collection.data.insert(
        {}, vector={"first": [0, 1], "second": [1, 0, 0], "third": [1, 0, 0, 0]}
    )

    objs = collection.query.near_vector(
        {"first": [[1, 0], [1, 0]], "second": [[1, 0, 0], [0, 0, 1]], "third": [0, 1, 0, 0]},
        target_vector=target_vector,
        return_metadata=wvc.query.MetadataQuery.full(),
    ).objects
    if distances[0] == distances[1]:
        assert sorted(obj.uuid for obj in objs) == sorted([uuid1, uuid2])
    else:
        assert [obj.uuid for obj in objs] == [uuid1, uuid2]

    obj1 = [obj for obj in objs if obj.uuid == uuid1][0]
    assert math.isclose(obj1.metadata.distance, distances[0], rel_tol=1e-5)
    obj2 = [obj for obj in objs if obj.uuid == uuid2][0]
    assert math.isclose(obj2.metadata.distance, distances[1], rel_tol=1e-5)
