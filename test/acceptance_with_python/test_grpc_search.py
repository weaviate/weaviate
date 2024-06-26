from weaviate.classes.config import Configure, DataType, Property
from weaviate.classes.query import MetadataQuery

from .conftest import CollectionFactory


def test_near_object_search(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[Property(name="Name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
    )
    uuid_banana = collection.data.insert({"Name": "Banana"})
    collection.data.insert({"Name": "Fruit"})
    collection.data.insert({"Name": "car"})
    collection.data.insert({"Name": "Mountain"})

    full_objects = collection.query.near_object(
        uuid_banana, return_metadata=MetadataQuery(distance=True, certainty=True)
    ).objects
    assert len(full_objects) == 4

    objects_distance = collection.query.near_object(
        uuid_banana, distance=full_objects[2].metadata.distance
    ).objects
    assert len(objects_distance) == 3

    objects_certainty = collection.query.near_object(
        uuid_banana, certainty=full_objects[2].metadata.certainty
    ).objects
    assert len(objects_certainty) == 3


def test_near_vector_search(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[Property(name="Name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
    )
    uuid_banana = collection.data.insert({"Name": "Banana"})
    collection.data.insert({"Name": "Fruit"})
    collection.data.insert({"Name": "car"})
    collection.data.insert({"Name": "Mountain"})

    banana = collection.query.fetch_object_by_id(uuid_banana, include_vector=True)

    full_objects = collection.query.near_vector(
        banana.vector["default"], return_metadata=MetadataQuery(distance=True, certainty=True)
    ).objects
    assert len(full_objects) == 4

    objects_distance = collection.query.near_vector(
        banana.vector["default"], distance=full_objects[2].metadata.distance
    ).objects
    assert len(objects_distance) == 3

    objects_distance = collection.query.near_vector(
        banana.vector["default"], certainty=full_objects[2].metadata.certainty
    ).objects
    assert len(objects_distance) == 3
