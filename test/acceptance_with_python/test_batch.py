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
