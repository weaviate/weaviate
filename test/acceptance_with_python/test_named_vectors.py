import weaviate.classes as wvc

from conftest import CollectionFactory


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


def test_hybrid_search_with_multiple_target_vectors(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="title1", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="title2", data_type=wvc.config.DataType.TEXT),
        ],
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="All",
                vectorize_collection_name=False,
            ),
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="title1",
                source_properties=["title1"],
                vectorize_collection_name=False,
            ),
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="title2",
                source_properties=["title2"],
                vectorize_collection_name=False,
            ),
        ],
    )

    uuid1 = collection.data.insert(
        properties={"title1": "apple", "title2": "cocoa"},
    )
    uuid2 = collection.data.insert(
        properties={"title1": "cocoa", "title2": "apple"},
    )
    uuid3 = collection.data.insert(
        properties={"title1": "mountain", "title2": "ridge line"},
    )

    direct = collection.query.hybrid(
        "apple",
        target_vector=["title1", "title2"],
        return_metadata=wvc.query.MetadataQuery.full(),
        alpha=1,  # to make sure that the vector part works
    )

    assert len(direct.objects) == 3

    # first two objects are a perfect fit, but their order is not guaranteed
    assert sorted([obj.uuid for obj in direct.objects[:2]]) == sorted([uuid1, uuid2])
    assert direct.objects[2].uuid == uuid3

    assert direct.objects[0].metadata.score == 1
    assert direct.objects[1].metadata.score == 1
    assert direct.objects[2].metadata.score == 0

    near_text_sub_search = collection.query.hybrid(
        "something else",
        vector=wvc.query.HybridVector.near_text("apple sandwich"),
        target_vector=["title1", "title2"],
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_text_sub_search.objects) == 3

    # first two objects are a perfect fit for vector search, but their order is not guaranteed
    assert sorted([obj.uuid for obj in near_text_sub_search.objects[:2]]) == sorted([uuid1, uuid2])
    assert (
        near_text_sub_search.objects[0].metadata.score > 0.5
    )  # only vector search part has result
    assert near_text_sub_search.objects[1].metadata.score > 0.5
    assert near_text_sub_search.objects[2].metadata.score == 0

    obj1 = collection.query.fetch_object_by_id(uuid1, include_vector=True)
    near_vector_sub_search = collection.query.hybrid(
        "something else",
        vector=wvc.query.HybridVector.near_vector(obj1.vector["title1"]),
        target_vector=["title1", "title2"],
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_vector_sub_search.objects) == 3

    # first two objects are a perfect fit for vector search, but their order is not guaranteed
    assert sorted([obj.uuid for obj in near_vector_sub_search.objects[:2]]) == sorted(
        [uuid1, uuid2]
    )
    assert (
        near_vector_sub_search.objects[0].metadata.score > 0.5
    )  # only vector search part has result
    assert near_vector_sub_search.objects[1].metadata.score > 0.5
    assert near_vector_sub_search.objects[2].metadata.score == 0


def test_near_object(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="title1", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="title2", data_type=wvc.config.DataType.TEXT),
        ],
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="title1",
                source_properties=["title1"],
                vectorize_collection_name=False,
            ),
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="title2",
                source_properties=["title2"],
                vectorize_collection_name=False,
            ),
        ],
    )

    uuid1 = collection.data.insert(
        properties={"title1": "apple", "title2": "cocoa"},
    )
    uuid2 = collection.data.insert(
        properties={"title1": "cocoa", "title2": "apple"},
    )
    uuid3 = collection.data.insert(
        properties={"title1": "mountain", "title2": "ridge line"},
    )

    # only finds first object with minimal distance
    near_obj1 = collection.query.near_object(
        uuid1,
        target_vector="title1",
        distance=0.1,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_obj1.objects) == 1
    assert near_obj1.objects[0].uuid == uuid1

    # finds both objects, but the second target vector has a larger distance
    near_obj2 = collection.query.near_object(
        uuid1,
        target_vector=["title1", "title2"],
        distance=0.9,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_obj2.objects) == 2
    # order is not guaranteed
    assert sorted([obj.uuid for obj in near_obj2.objects]) == sorted([uuid1, uuid2])


def test_near_text(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="title1", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="title2", data_type=wvc.config.DataType.TEXT),
        ],
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="title1",
                source_properties=["title1"],
                vectorize_collection_name=False,
            ),
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name="title2",
                source_properties=["title2"],
                vectorize_collection_name=False,
            ),
        ],
    )

    uuid1 = collection.data.insert(
        properties={"title1": "apple", "title2": "cocoa"},
    )
    uuid2 = collection.data.insert(
        properties={"title1": "cocoa", "title2": "apple"},
    )
    uuid3 = collection.data.insert(
        properties={"title1": "mountain", "title2": "ridge line"},
    )

    near_text1 = collection.query.near_text(
        "apple",
        target_vector="title1",
        distance=0.1,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_text1.objects) == 1
    assert near_text1.objects[0].uuid == uuid1

    # finds both objects, but the second target vector has a larger distance
    near_text2 = collection.query.near_text(
        "apple",
        target_vector=["title1", "title2"],
        distance=0.9,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_text2.objects) == 2
    # order is not guaranteed
    assert sorted([obj.uuid for obj in near_text2.objects]) == sorted([uuid1, uuid2])
