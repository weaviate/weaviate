import weaviate
import weaviate.classes as wvc

from .conftest import CollectionFactory


def test_batch_update_empty_list(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="array", data_type=wvc.config.DataType.TEXT_ARRAY),
        ],
        vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
    )

    uuid1 = collection.data.insert({"array": []})
    collection.data.insert_many(
        [wvc.data.DataObject(properties={"array": ["one", "two"]}, uuid=uuid1)]
    )


def test_batch_without_properties(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(vectorizer_config=wvc.config.Configure.Vectorizer.none())

    ret = collection.data.insert_many([wvc.data.DataObject(vector=[0.0, 1.0])])
    assert not ret.has_errors

    res = collection.query.fetch_objects()
    assert len(res.objects) == 1
