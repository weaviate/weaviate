import uuid
from typing import List, Optional, Union

import pytest
from weaviate.classes.config import Configure, DataType, Property
from weaviate.classes.data import DataObject
from weaviate.classes.query import HybridVector, MetadataQuery, Move
from weaviate.collections.classes.grpc import PROPERTIES
from weaviate.types import UUID

from .conftest import CollectionFactory

UUID1 = "00000000-0000-0000-0000-000000000001"

def test_fetch_objects_search(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[Property(name="Name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.none(),
    )
    for i in range(5):
        collection.data.insert({"Name": str(i)})

    assert len(collection.query.fetch_objects().objects) == 5


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


@pytest.mark.parametrize("query", ["cake", ["cake"]])
@pytest.mark.parametrize("objects", [UUID1, str(UUID1), [UUID1], [str(UUID1)]])
@pytest.mark.parametrize("concepts", ["hiking", ["hiking"]])
@pytest.mark.parametrize(
    "return_properties", [["value"], None]
)  # Passing none here causes a server-side bug with <=1.22.2
def test_near_text_search(
    collection_factory: CollectionFactory,
    query: Union[str, List[str]],
    objects: Union[UUID, List[UUID]],
    concepts: Union[str, List[str]],
    return_properties: Optional[PROPERTIES],
) -> None:
    collection = collection_factory(
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
        properties=[Property(name="value", data_type=DataType.TEXT)],
    )

    batch_return = collection.data.insert_many(
        [
            DataObject(properties={"value": "Apple"}, uuid=UUID1),
            DataObject(properties={"value": "Mountain climbing"}),
            DataObject(properties={"value": "apple cake"}),
            DataObject(properties={"value": "cake"}),
        ]
    )

    objs = collection.query.near_text(
        query=query,
        move_to=Move(force=1.0, objects=objects),
        move_away=Move(force=0.5, concepts=concepts),
        include_vector=True,
        return_properties=return_properties,
    ).objects

    assert len(objs) == 4

    assert objs[0].uuid == batch_return.uuids[2]
    assert "default" in objs[0].vector
    if return_properties is not None:
        assert objs[0].properties["value"] == "apple cake"

def test_hybrid_near_vector_search(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[
            Property(name="text", data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
    )
    uuid_banana = collection.data.insert({"text": "banana"})
    obj = collection.query.fetch_object_by_id(uuid_banana, include_vector=True)

    collection.data.insert({"text": "dog"})
    collection.data.insert({"text": "different concept"})

    hybrid_objs = collection.query.hybrid(
        query=None,
        vector=HybridVector.near_vector(vector=obj.vector["default"]),
    ).objects

    assert hybrid_objs[0].uuid == uuid_banana
    assert len(hybrid_objs) == 3

    # make a near vector search to get the distance
    near_vec = collection.query.near_vector(
        near_vector=obj.vector["default"], return_metadata=["distance"]
    ).objects
    assert near_vec[0].metadata.distance is not None

    hybrid_objs2 = collection.query.hybrid(
        query=None,
        vector=HybridVector.near_vector(
            vector=obj.vector["default"], distance=near_vec[0].metadata.distance + 0.001
        ),
        return_metadata=MetadataQuery.full(),
    ).objects

    assert hybrid_objs2[0].uuid == uuid_banana
    assert len(hybrid_objs2) == 1