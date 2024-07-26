import weaviate.classes as wvc

from .conftest import CollectionFactory


def test_batch_update_empty_list2(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="tags", data_type=wvc.config.DataType.TEXT_ARRAY),
            wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="authorized", data_type=wvc.config.DataType.BOOL),
        ],
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="title_vector", vectorize_collection_name=False
            ),
        ],
    )

    uuid1 = collection.data.insert({"tags": [], "authorized": False})

    # update without the empty array
    collection.data.update(properties={"authorized": True}, uuid=uuid1)
