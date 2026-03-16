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
