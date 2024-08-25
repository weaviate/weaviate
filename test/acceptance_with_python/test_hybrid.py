import uuid
from typing import List

import pytest

from weaviate.classes.config import Configure, Property, DataType
from weaviate.classes.query import TargetVectors
import weaviate.classes as wvc
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
        ([1, 0, 0], [UUID1, UUID2, UUID3, UUID4], 2.5),
        ([0.5, 0.5, 0.5], [], 0.0001),
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


def test_hybrid_search_vector_distance_more_objects(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
    )

    ret = collection.data.insert_many(
        [
            {"name": entry}
            for entry in [
                "mountain hike",
                "banana apple",
                "road trip",
                "coconut smoothie",
                "beach vacation",
                "apple pie",
                "banana split",
                "mountain biking",
                "apple cider",
                "beach volleyball",
                "sailing",
            ]
        ]
    )
    assert ret.has_errors is False

    for query in ["apple", "banana", "beach", "mountain", "summer dress"]:
        objs = collection.query.near_text(
            query, return_metadata=wvc.query.MetadataQuery.full(), limit=100
        ).objects
        middle_distance = objs[len(objs) // 2].metadata.distance

        # with the cutoff distance, the results should be the same for hybrid and near
        objs_nt_cutoff = collection.query.near_text(
            query,
            distance=middle_distance,
            return_metadata=wvc.query.MetadataQuery.full(),
            limit=100,
        ).objects
        objs_hy_cutoff = collection.query.hybrid(
            query,
            distance=middle_distance,
            alpha=1,
            return_metadata=wvc.query.MetadataQuery.full(),
            limit=100,
        ).objects

        assert len(objs_nt_cutoff) == len(objs_hy_cutoff)
        assert all(
            objs_nt_cutoff[i].uuid == objs_hy_cutoff[i].uuid for i, _ in enumerate(objs_nt_cutoff)
        )
