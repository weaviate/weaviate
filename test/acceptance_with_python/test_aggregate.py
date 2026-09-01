import pytest
from weaviate.collections.classes.config import Configure, Property, DataType
import weaviate.classes as wvc

from .conftest import CollectionFactory


def test_aggregate_max_vector_distance(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.none(),
    )

    if collection._connection._weaviate_version.is_lower_than(1, 26, 3):
        pytest.skip("Hybrid max vector distance is only supported in versions higher than 1.26.3")

    collection.data.insert({"name": "banana one"}, vector=[1, 0, 0, 0])
    collection.data.insert({"name": "banana two"}, vector=[0, 1, 0, 0])
    collection.data.insert({"name": "banana three"}, vector=[0, 1, 0, 0])
    collection.data.insert({"name": "banana four"}, vector=[1, 0, 0, 0])

    res = collection.aggregate.hybrid(
        "banana",
        vector=[1, 0, 0, 0],
        max_vector_distance=0.5,
        return_metrics=[wvc.aggregate.Metrics("name").text(count=True)],
    )
    assert res.total_count == 2


def test_aggregate_max_vector_distance_named(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vector_config=[Configure.Vectors.self_provided(name="default")],
    )

    if collection._connection._weaviate_version.is_lower_than(1, 26, 3):
        pytest.skip("Hybrid max vector distance is only supported in versions higher than 1.26.3")

    collection.data.insert({"name": "banana one"}, vector={"default": [1, 0, 0, 0]})
    collection.data.insert({"name": "banana two"}, vector={"default": [0, 1, 0, 0]})
    collection.data.insert({"name": "banana three"}, vector={"default": [0, 1, 0, 0]})
    collection.data.insert({"name": "banana four"}, vector={"default": [1, 0, 0, 0]})

    res = collection.aggregate.hybrid(
        "banana",
        vector=[1, 0, 0, 0],
        max_vector_distance=0.5,
        return_metrics=[wvc.aggregate.Metrics("name").text(count=True)],
        target_vector="default",
    )
    assert res.total_count == 2


def _fruit_and_veg_collection(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[
            Property(name="name", data_type=DataType.TEXT),
            Property(name="category", data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.none(),
    )
    collection.data.insert({"name": "banana one", "category": "fruit"}, vector=[1, 0, 0, 0])
    collection.data.insert({"name": "banana two", "category": "fruit"}, vector=[0, 1, 0, 0])
    collection.data.insert({"name": "banana three", "category": "veg"}, vector=[0, 1, 0, 0])
    collection.data.insert({"name": "banana four", "category": "veg"}, vector=[1, 0, 0, 0])
    return collection


# "banana" matches all four objects, so a leg that ignores the filter doubles
# every count below. alpha 0 runs the keyword leg alone, which is what a client
# gets by leaving alpha unset over gRPC.
@pytest.mark.parametrize(
    "alpha,object_limit,expected_count",
    [
        (0, 10, 2),
        (0, None, 2),
        (0.5, 10, 2),
        (1, None, 2),
    ],
)
def test_aggregate_hybrid_applies_filter(
    collection_factory: CollectionFactory,
    alpha: float,
    object_limit: int,
    expected_count: int,
) -> None:
    collection = _fruit_and_veg_collection(collection_factory)

    res = collection.aggregate.hybrid(
        "banana",
        alpha=alpha,
        vector=[1, 0, 0, 0],
        object_limit=object_limit,
        filters=wvc.query.Filter.by_property("category").equal("fruit"),
        return_metrics=[wvc.aggregate.Metrics("name").text(top_occurrences_value=True)],
    )
    assert res.total_count == expected_count
    assert sorted(occ.value for occ in res.properties["name"].top_occurrences) == [
        "banana one",
        "banana two",
    ]


def test_aggregate_hybrid_without_filter(collection_factory: CollectionFactory) -> None:
    collection = _fruit_and_veg_collection(collection_factory)

    res = collection.aggregate.hybrid("banana", alpha=0, object_limit=10)
    assert res.total_count == 4


def test_aggregate_hybrid_group_by_applies_filter(collection_factory: CollectionFactory) -> None:
    collection = _fruit_and_veg_collection(collection_factory)

    res = collection.aggregate.hybrid(
        "banana",
        alpha=0,
        object_limit=10,
        filters=wvc.query.Filter.by_property("category").equal("fruit"),
        group_by=wvc.aggregate.GroupByAggregate(prop="category"),
    )
    assert len(res.groups) == 1
    assert res.groups[0].grouped_by.value == "fruit"
    assert res.groups[0].total_count == 2
