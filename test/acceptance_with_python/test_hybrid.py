import uuid
from typing import List, Optional

import pytest

from weaviate.classes.config import Configure, Property, DataType
from weaviate.classes.query import TargetVectors
import weaviate.classes as wvc
from .conftest import CollectionFactory

from weaviate.collections.classes.grpc import HybridVectorType


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
        max_vector_distance=distance,
    ).objects
    assert sorted([obj.uuid for obj in objs]) == sorted(expected)  # order is not guaranteed


@pytest.mark.parametrize(
    "query", ["banana", "car"]
)  # does not matter if a result is found for bm25
@pytest.mark.parametrize(
    "vector,expected,distance",
    [
        ([1, 0, 0, 0], [UUID1], 0.5),
        ([1, 0, 0, 0], [UUID1, UUID2, UUID3, UUID4], 2.5),
        ([0.5, 0.5, 0.5, 0.5], [], 0.0001),
        ([0.5, 0.5, 0.5, 0.5], [UUID1, UUID2, UUID3, UUID4], None),  # everything is found
    ],
)
def test_aggregate_max_vector_distance(
    collection_factory: CollectionFactory,
    vector: List[int],
    expected: List[uuid.UUID],
    distance: float,
    query: str,
) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.none(),
    )

    collection.data.insert({"name": "banana one"}, vector=[1, 0, 0, 0], uuid=UUID1)
    collection.data.insert({"name": "banana two"}, vector=[0, 1, 0, 0], uuid=UUID2)
    collection.data.insert({"name": "banana three"}, vector=[0, 0, 1, 0], uuid=UUID3)
    collection.data.insert({"name": "banana four"}, vector=[0, 0, 0, 1], uuid=UUID4)

    # get abd aggregate should match the same objects
    objs = collection.query.hybrid("banana", vector=vector, max_vector_distance=distance).objects
    assert sorted([obj.uuid for obj in objs]) == sorted(expected)  # order is not guaranteed

    res = collection.aggregate.hybrid(
        "banana",
        vector=vector,
        max_vector_distance=distance,
        return_metrics=[wvc.aggregate.Metrics("name").text(count=True)],
    )
    assert res.total_count == len(expected)


@pytest.mark.parametrize("query", ["apple", "banana", "beach", "mountain", "summer dress"])
@pytest.mark.parametrize(
    "distance",
    [
        wvc.config.VectorDistances.DOT,
        wvc.config.VectorDistances.COSINE,
        wvc.config.VectorDistances.L2_SQUARED,
    ],
)
@pytest.mark.parametrize("offset", [0, 2])
def test_hybrid_search_vector_distance_more_objects(
    collection_factory: CollectionFactory,
    distance: wvc.config.VectorDistances,
    query: str,
    offset: Optional[int],
) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
        vector_index_config=Configure.VectorIndex.hnsw(distance_metric=distance),
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
        offset=offset,
    ).objects
    objs_hy_cutoff = collection.query.hybrid(
        query,
        max_vector_distance=middle_distance,
        alpha=1,
        return_metadata=wvc.query.MetadataQuery.full(),
        limit=100,
        offset=offset,
    ).objects

    assert len(objs_nt_cutoff) == len(objs_hy_cutoff)
    assert all(
        objs_nt_cutoff[i].uuid == objs_hy_cutoff[i].uuid for i, _ in enumerate(objs_nt_cutoff)
    )

    res = collection.aggregate.hybrid(
        query,
        max_vector_distance=middle_distance,
        return_metrics=[wvc.aggregate.Metrics("name").text(count=True)],
    )
    assert res.total_count == len(objs_nt_cutoff) + offset


def test_hybrid_search_with_bm25_only_objects(
    collection_factory: CollectionFactory,
) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.none(),
    )

    collection.data.insert({"name": "banana"}, vector=[1, 0, 0, 0], uuid=UUID1)
    collection.data.insert({"name": "apple"}, uuid=UUID2)  # not in vector search results

    # both objects are found without limit as second object is found via BM25 search
    objs = collection.query.hybrid("apple", vector=[1, 0, 0, 0]).objects
    assert len(objs) == 2
    res = collection.aggregate.hybrid(
        "apple",
        vector=[1, 0, 0, 0],
        object_limit=50,
        return_metrics=[wvc.aggregate.Metrics("name").text(count=True)],
    )
    assert res.total_count == 2

    # only first object with vector is found with a max vector distance
    objs = collection.query.hybrid("apple", vector=[1, 0, 0, 0], max_vector_distance=0.5).objects
    assert len(objs) == 1
    assert objs[0].uuid == UUID1

    res = collection.aggregate.hybrid(
        "apple",
        vector=[1, 0, 0, 0],
        object_limit=50,
        max_vector_distance=0.5,
        return_metrics=[wvc.aggregate.Metrics("name").text(count=True)],
    )
    assert res.total_count == 1

    # no results found
    objs = collection.query.hybrid("apple", vector=[0, 1, 0, 0], max_vector_distance=0.5).objects
    assert len(objs) == 0

    res = collection.aggregate.hybrid(
        "apple",
        vector=[0, 1, 0, 0],
        object_limit=50,
        max_vector_distance=0.5,
        return_metrics=[wvc.aggregate.Metrics("name").text(count=True)],
    )
    assert res.total_count == 0


@pytest.mark.parametrize("vector", [None, wvc.query.HybridVector.near_text("summer dress")])
def test_hybrid_with_offset(
    collection_factory: CollectionFactory, vector: Optional[HybridVectorType]
) -> None:
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

    hy = collection.query.hybrid("summer dress")
    assert len(hy.objects) > 0

    hy_offset = collection.query.hybrid("summer dress", offset=2, vector=vector)
    assert len(hy_offset.objects) + 2 == len(hy.objects)


def test_flipping(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.none(),
    )

    collection.data.insert({"name": "banana fruit"}, vector=[1, 0, 0], uuid=UUID1)
    collection.data.insert({"name": "apple fruit first"}, vector=[1, 0, 0], uuid=UUID2)
    collection.data.insert({"name": "apple fruit second"}, vector=[1, 0, 0], uuid=UUID3)

    hy = collection.query.hybrid("fruit", vector=[1, 0, 0]).objects

    # repeat search to make sure order is always the same
    for i in range(10):
        hy2 = collection.query.hybrid("fruit", vector=[1, 0, 0]).objects
        assert all(hy[i].uuid == hy2[i].uuid for i in range(len(hy)))
