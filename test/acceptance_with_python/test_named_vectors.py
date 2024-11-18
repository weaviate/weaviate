import pytest
import weaviate.classes as wvc
from weaviate.exceptions import UnexpectedStatusCodeError, WeaviateInsertManyAllFailedError

from .conftest import CollectionFactory


def test_create_named_vectors_with_and_without_vectorizer(
    collection_factory: CollectionFactory,
) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="content", data_type=wvc.config.DataType.TEXT),
        ],
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="AllExplicit",
                source_properties=["title", "content"],
                vectorize_collection_name=False,
            ),
            wvc.config.Configure.NamedVectors.none(name="bringYourOwn"),
        ],
    )

    uuid = collection.data.insert(
        properties={"title": "Hello", "content": "World"},
        vector={"bringYourOwn": [0.5, 0.25, 0.75]},
    )

    obj = collection.query.fetch_object_by_id(uuid, include_vector=True)
    assert obj.vector["AllExplicit"] is not None
    assert obj.vector["bringYourOwn"] is not None


def test_single_named_vectors_without_names(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=[wvc.config.Configure.NamedVectors.none("title")],
    )

    # insert object with single non-named vector.
    uuid1 = collection.data.insert(properties={"title": "Hello"}, vector=[1, 2, 3])
    obj = collection.query.fetch_object_by_id(uuid1, include_vector=True)
    assert "title" in obj.vector

    ret = collection.data.insert_many(
        [wvc.data.DataObject(properties={"title": "Hello"}, vector=[1, 2, 3])]
    )
    obj_batch = collection.query.fetch_object_by_id(ret.uuids[0], include_vector=True)
    assert "title" in obj_batch.vector


def test_named_vectors_without_names(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none("first"),
            wvc.config.Configure.NamedVectors.none("second"),
        ],
    )
    with pytest.raises(UnexpectedStatusCodeError):
        collection.data.insert(properties={"title": "Hello"}, vector=[1, 2, 3])

    with pytest.raises(WeaviateInsertManyAllFailedError):
        collection.data.insert_many(
            [wvc.data.DataObject(properties={"title": "Hello"}, vector=[1, 2, 3])]
        )


def test_single_vectorizer_with_named_vectors(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    with pytest.raises(UnexpectedStatusCodeError):
        collection.data.insert(properties={"title": "Hello"}, vector={"something": [1, 2, 3]})

    with pytest.raises(WeaviateInsertManyAllFailedError):
        collection.data.insert_many(
            [wvc.data.DataObject(properties={"title": "Hello"}, vector={"something": [1, 2, 3]})]
        )
