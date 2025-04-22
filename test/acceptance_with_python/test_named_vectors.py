import pytest
import weaviate.classes as wvc
import math
from weaviate.collections.classes.grpc import (
    _MultiTargetVectorJoin,
    TargetVectors,
    _MultiTargetVectorJoinEnum,
    HybridVectorType,
)
from weaviate.exceptions import UnexpectedStatusCodeError, WeaviateInsertManyAllFailedError

from .conftest import CollectionFactory, NamedCollection


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


def test_hybrid_search_with_multiple_target_vectors(named_collection: NamedCollection) -> None:
    collection = named_collection()

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


def test_near_object(named_collection: NamedCollection) -> None:
    collection = named_collection()

    uuid1 = collection.data.insert(
        properties={"title1": "apple", "title2": "cocoa"},
    )
    uuid2 = collection.data.insert(
        properties={"title1": "banana", "title2": "cocoa"},
    )
    collection.data.insert(
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
        target_vector=TargetVectors.sum(["title1", "title2"]),
        distance=0.9,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_obj2.objects) == 2
    # order is not guaranteed
    assert sorted([obj.uuid for obj in near_obj2.objects]) == sorted([uuid1, uuid2])


def test_near_text(named_collection: NamedCollection) -> None:
    collection = named_collection()

    uuid1 = collection.data.insert(
        properties={"title1": "apple", "title2": "cocoa"},
    )
    uuid2 = collection.data.insert(
        properties={"title1": "cocoa", "title2": "apple"},
    )
    collection.data.insert(
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
        target_vector=TargetVectors.sum(["title1", "title2"]),
        distance=0.9,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_text2.objects) == 2
    # order is not guaranteed
    assert sorted([obj.uuid for obj in near_text2.objects]) == sorted([uuid1, uuid2])


def test_near_vector(named_collection: NamedCollection) -> None:
    collection = named_collection()

    uuid1 = collection.data.insert(
        properties={"title1": "apple", "title2": "cocoa"},
    )
    uuid2 = collection.data.insert(
        properties={"title1": "cocoa", "title2": "apple"},
    )
    collection.data.insert(
        properties={"title1": "mountain", "title2": "ridge line"},
    )

    obj1 = collection.query.fetch_object_by_id(uuid1, include_vector=True)
    near_vector1 = collection.query.near_vector(
        obj1.vector["title1"],
        target_vector="title1",
        distance=0.1,
        return_metadata=wvc.query.MetadataQuery.full(),
    )

    assert len(near_vector1.objects) == 1
    assert near_vector1.objects[0].uuid == uuid1

    # finds both objects, but the second target vector has a larger distance
    near_vector2 = collection.query.near_vector(
        obj1.vector["title1"],
        target_vector=TargetVectors.sum(["title1", "title2"]),
        distance=0.9,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(near_vector2.objects) == 2
    # order is not guaranteed
    assert sorted([obj.uuid for obj in near_vector2.objects]) == sorted([uuid1, uuid2])


@pytest.mark.parametrize("target_vector", [None, "title"])
def test_near_vector_with_single_named_vector(
    named_collection: NamedCollection, target_vector: str | None
) -> None:
    collection = named_collection(props=["title"])

    uuid1 = collection.data.insert(
        properties={"title": "alpha"},
    )
    uuid2 = collection.data.insert(
        properties={"title": "beta"},
    )
    collection.data.insert(
        properties={"title": "gamma"},
    )

    obj1 = collection.query.fetch_object_by_id(uuid1, include_vector=True)
    near_vector1 = collection.query.near_vector(
        obj1.vector["title"],
        target_vector=target_vector,
        distance=0.1,
        return_metadata=wvc.query.MetadataQuery.full(),
    )

    assert len(near_vector1.objects) == 1
    assert near_vector1.objects[0].uuid == uuid1


CAR_DISTANCE = 0.7892138957977295
APPLE_DISTANCE = 0.5168729424476624
KALE_DISTANCE = 0.5732871294021606


@pytest.mark.parametrize(
    "multi_target_fusion_method,distance",
    [
        (
            TargetVectors.sum(["title1", "title2", "title3"]),
            CAR_DISTANCE + APPLE_DISTANCE + KALE_DISTANCE,
        ),
        (
            TargetVectors.average(["title1", "title2", "title3"]),
            (CAR_DISTANCE + APPLE_DISTANCE + KALE_DISTANCE) / 3,
        ),
        (TargetVectors.minimum(["title1", "title2", "title3"]), APPLE_DISTANCE),
        (
            TargetVectors.manual_weights({"title1": 0.4, "title2": 1.2, "title3": 0.752}),
            APPLE_DISTANCE * 0.4 + CAR_DISTANCE * 1.2 + KALE_DISTANCE * 0.752,
        ),
    ],
)
def test_different_target_fusion_methods(
    named_collection: NamedCollection,
    multi_target_fusion_method: _MultiTargetVectorJoin,
    distance: float,
) -> None:
    collection = named_collection()

    collection.data.insert(properties={"title1": "apple", "title2": "car", "title3": "kale"})

    nt = collection.query.near_text(
        "fruit",
        target_vector=multi_target_fusion_method,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(nt.objects) == 1
    assert math.isclose(nt.objects[0].metadata.distance, distance, rel_tol=1e-5)


def test_score_fusion(named_collection: NamedCollection) -> None:
    collection = named_collection()

    uuid0 = collection.data.insert(
        properties={"title1": "first"},
        vector={
            "title1": [1, 0, 0],
            "title2": [0, 0, 1],
            "title3": [1, 0, 0],
        },
    )
    uuid1 = collection.data.insert(
        properties={"title1": "second"},
        vector={
            "title1": [0, 1, 0],
            "title2": [1, 0, 0],
            "title3": [0, 0, 1],
        },
    )
    uuid2 = collection.data.insert(
        properties={"title1": "third"},
        vector={
            "title1": [0, 1, 0],
            "title2": [0, 0, 1],
            "title3": [0, 0, 1],
        },
    )

    nt = collection.query.near_vector(
        [1.0, 0.0, 0.0],
        target_vector=TargetVectors.relative_score({"title1": 1, "title2": 1, "title3": 1}),
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(nt.objects) == 3

    assert math.isclose(nt.objects[0].metadata.distance, 1, rel_tol=1e-5)
    assert nt.objects[0].uuid == uuid0
    assert math.isclose(nt.objects[1].metadata.distance, 2, rel_tol=1e-5)
    assert nt.objects[1].uuid == uuid1
    assert math.isclose(nt.objects[2].metadata.distance, 3, rel_tol=1e-5)
    assert nt.objects[2].uuid == uuid2


@pytest.mark.parametrize(
    "multi_target_fusion_method",
    [
        TargetVectors.sum(["colour", "weather", "material"]),
        TargetVectors.average(["colour", "weather", "material"]),
        TargetVectors.manual_weights({"colour": 0.4, "weather": 1.2, "material": 0.752}),
        TargetVectors.relative_score({"colour": 1, "weather": 1.0, "material": 1.0}),
    ],
)
def test_more_results_than_limit(
    named_collection: NamedCollection,
    multi_target_fusion_method: _MultiTargetVectorJoin,
) -> None:
    collection = named_collection(props=["colour", "weather", "material"])

    uuid1 = collection.data.insert(
        properties={"colour": "bright", "weather": "summer", "material": "cotton"},
    )
    uuid2 = collection.data.insert(
        properties={"colour": "snow", "weather": "warm", "material": "breezy"},
    )
    uuid3 = collection.data.insert(
        properties={"colour": "white", "weather": "cold", "material": "heavy fur"},
    )
    uuid4 = collection.data.insert(
        properties={"colour": "red", "weather": "summer", "material": "thick"},
    )
    uuid5 = collection.data.insert(
        properties={"colour": "black", "weather": "arctic", "material": "lite"},
    )

    # uuid3 is the best match for colour but bad for the others targets => make sure that the extra distances are
    # computed correctly
    nt = collection.query.near_text(
        "white summer clothing with breezy material",
        target_vector=multi_target_fusion_method,
        return_metadata=wvc.query.MetadataQuery.full(),
        limit=2,
    )

    assert len(nt.objects) == 2
    assert nt.objects[0].uuid == uuid1
    assert nt.objects[1].uuid == uuid2

    # get all results to check if the distances are correct
    nt3 = collection.query.near_text(
        "white summer clothing with breezy material",
        target_vector=multi_target_fusion_method,
        return_metadata=wvc.query.MetadataQuery.full(),
        limit=5,
    )

    assert nt3.objects[0].uuid == uuid1
    assert nt3.objects[1].uuid == uuid2
    # fusion score depend on all the input scores and are expected to be different with more objects that are found
    if (
        multi_target_fusion_method.combination.value
        != _MultiTargetVectorJoinEnum.RELATIVE_SCORE.value
    ):
        assert math.isclose(
            nt3.objects[0].metadata.distance, nt.objects[0].metadata.distance, rel_tol=0.001
        )
        assert math.isclose(
            nt3.objects[1].metadata.distance, nt.objects[1].metadata.distance, rel_tol=0.001
        )


@pytest.mark.parametrize(
    "multi_target_fusion_method,number_objects",
    [
        (TargetVectors.sum(["first", "second", "third"]), 1),
        (TargetVectors.average(["first", "second", "third"]), 1),
        (TargetVectors.minimum(["first", "second", "third"]), 2),
        (
            TargetVectors.manual_weights({"first": 0.4, "second": 1.2, "third": 0.752}),
            1,
        ),
        (TargetVectors.relative_score({"first": 1, "second": 1, "third": 1}), 1),
    ],
)
def test_named_vectors_missing_entries(
    collection_factory: CollectionFactory,
    multi_target_fusion_method: _MultiTargetVectorJoin,
    number_objects: int,
) -> None:
    collection = collection_factory(
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none(
                name=entry,
            )
            for entry in ["first", "second", "third"]
        ]
    )

    # first object has all entries, second object is missing the third entry is missing.
    uuid1 = collection.data.insert(
        properties={}, vector={"first": [1, 0, 0], "second": [1, 0, 0], "third": [1, 0, 0]}
    )
    uuid2 = collection.data.insert(
        properties={},
        vector={"first": [0, 1, 0], "second": [0, math.sqrt(3), 0]},
    )

    nt = collection.query.near_vector(
        [1, 0, 0],
        target_vector=multi_target_fusion_method,
        return_metadata=wvc.query.MetadataQuery.full(),
    )

    # first object is perfect fit, second object has a distance of 1
    assert len(nt.objects) == number_objects
    assert nt.objects[0].uuid == uuid1
    if len(nt.objects) == 2:
        assert nt.objects[1].uuid == uuid2


def test_multi_target_near_vector(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none(
                name=entry,
            )
            for entry in ["first", "second", "third"]
        ]
    )

    collection.data.insert(
        properties={}, vector={"first": [1, 0], "second": [0, 0, 1], "third": [0, 0, 0, 1]}
    )
    uuid2 = collection.data.insert(
        properties={}, vector={"first": [0, 1], "second": [0, 1, 0], "third": [0, 0, 1, 0]}
    )

    nt = collection.query.near_vector(
        {"first": [0, 1], "second": [0, 1, 0], "third": [0, 0, 1, 0]},
        return_metadata=wvc.query.MetadataQuery.full(),
        target_vector=TargetVectors.sum(["first", "second", "third"]),
        distance=0.1,
    )
    assert len(nt.objects) == 1
    assert nt.objects[0].uuid == uuid2
    assert nt.objects[0].metadata.distance == 0


def test_multi_target_with_filter(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="first", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="second", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="int", data_type=wvc.config.DataType.INT),
        ],
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name=entry, source_properties=[entry], vectorize_collection_name=False
            )
            for entry in ["first", "second"]
        ],
    )

    uuid1 = collection.data.insert(
        properties={"first": "apple", "second": "mountain", "int": 3},
    )
    collection.data.insert(
        properties={"first": "banana", "second": "blueberry", "int": 1},
    )
    uuid3 = collection.data.insert(
        properties={"first": "backpack", "second": "orange", "int": 2},
    )

    objs = collection.query.near_text(
        "fruit",
        return_metadata=wvc.query.MetadataQuery.full(),
        target_vector=wvc.query.TargetVectors.sum(["first", "second"]),
        limit=5,
        filters=wvc.query.Filter.by_property("int").greater_or_equal(2),
    ).objects

    # second object should not be part of results
    assert len(objs) == 2
    assert sorted(obj.uuid for obj in objs) == sorted(
        [uuid1, uuid3]
    )  # order is not guaranteed and does not matter for this test


@pytest.mark.parametrize(
    "combination",
    [
        wvc.query.TargetVectors.sum(["title1", "title2"]),
        wvc.query.TargetVectors.average(["title1", "title2"]),
    ],
)
@pytest.mark.parametrize(
    "vector",
    [
        wvc.query.HybridVector.near_vector({"title1": [1, 0, 0], "title2": [0, 0, 1]}),
        {"title1": [1, 0, 0], "title2": [0, 0, 1]},
    ],
)
def test_hybrid_combinations(
    collection_factory: CollectionFactory,
    vector: HybridVectorType,
    combination: _MultiTargetVectorJoin,
) -> None:
    collection = collection_factory(
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none(
                name=entry,
            )
            for entry in ["title1", "title2"]
        ]
    )
    uuid0 = collection.data.insert(
        properties={"title1": "first"},
        vector={"title1": [1, 0, 0], "title2": [0, 0, 1]},
    )
    uuid1 = collection.data.insert(
        properties={"title1": "second"},
        vector={"title1": [0, 1, 0], "title2": [1, 0, 0]},
    )
    uuid2 = collection.data.insert(
        properties={"title1": "third"},
        vector={"title1": [0, 1, 0], "title2": [0, 0, 1]},
    )

    res = collection.query.hybrid(
        "something else",
        vector=vector,
        target_vector=wvc.query.TargetVectors.sum(["title1", "title2"]),
        alpha=1,
        return_metadata=wvc.query.MetadataQuery.full(),
    )
    assert len(res.objects) == 3
    assert res.objects[0].uuid == uuid0
    assert res.objects[0].metadata.score == 1
    assert res.objects[1].uuid == uuid2
    assert res.objects[1].metadata.score == 0.5
    assert res.objects[2].uuid == uuid1
    assert res.objects[2].metadata.score == 0.0


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
