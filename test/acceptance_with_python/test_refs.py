import pytest

import weaviate.classes as wvc
from weaviate.collections.classes.internal import ReferenceToMulti

from .conftest import CollectionFactory


def test_ref_with_cycle(collection_factory: CollectionFactory) -> None:
    col = collection_factory(
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    col.config.add_reference(wvc.config.ReferenceProperty(name="ref", target_collection=col.name))

    a = col.data.insert(properties={"name": "A"})
    b = col.data.insert(properties={"name": "B"}, references={"ref": a})
    col.data.reference_add(from_uuid=a, from_property="ref", to=b)

    ret = col.query.fetch_objects(
        return_references=[
            wvc.query.QueryReference(
                link_on="ref",
                return_properties="name",
                return_references=[
                    wvc.query.QueryReference(
                        link_on="ref",
                        return_properties="name",
                        return_metadata=wvc.query.MetadataQuery.full(),
                    )
                ],
                return_metadata=wvc.query.MetadataQuery.full(),
            ),
        ],
    ).objects

    ret = sorted(ret, key=lambda x: x.properties["name"])
    assert ret[0].properties["name"] == "A"
    assert ret[1].properties["name"] == "B"
    assert ret[0].references["ref"].objects[0].properties["name"] == "B"
    assert ret[1].references["ref"].objects[0].properties["name"] == "A"


@pytest.mark.skip(reason="DB-18")
def test_ref_with_multiple_cycle(collection_factory: CollectionFactory) -> None:
    col = collection_factory(
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    col.config.add_reference(wvc.config.ReferenceProperty(name="ref", target_collection=col.name))

    # Add objects with two cyclic paths
    # c => b => a => c
    # c => a => c
    a = col.data.insert(properties={"name": "A"})
    b = col.data.insert(properties={"name": "B"}, references={"ref": a})
    c = col.data.insert(properties={"name": "C"}, references={"ref": [b, a]})  # has two refs
    col.data.reference_add(from_uuid=a, from_property="ref", to=c)

    ret = col.query.fetch_objects(
        return_references=[
            wvc.query.QueryReference(
                link_on="ref",
                return_properties=["name"],
                return_references=[
                    wvc.query.QueryReference(
                        link_on="ref",
                        return_properties="name",
                        return_metadata=wvc.query.MetadataQuery.full(),
                        return_references=[
                            wvc.query.QueryReference(
                                link_on="ref",
                                return_properties="name",
                                return_metadata=wvc.query.MetadataQuery.full(),
                            )
                        ],
                    )
                ],
                return_metadata=wvc.query.MetadataQuery.full(),
            ),
        ],
    ).objects

    # both paths are resolved correctly
    ret = sorted(ret, key=lambda x: x.properties["name"])
    assert ret[0].properties["name"] == "A"
    assert ret[1].properties["name"] == "B"
    assert ret[2].properties["name"] == "C"

    assert ret[0].references["ref"].objects[0].properties["name"] == "C"
    assert ret[1].references["ref"].objects[0].properties["name"] == "A"

    ret2_objects = sorted(ret[2].references["ref"].objects, key=lambda x: x.properties["name"])
    assert ret2_objects[0].properties["name"] == "A"
    assert ret2_objects[1].properties["name"] == "B"


def test_return_metadata_ref(collection_factory: CollectionFactory) -> None:
    target = collection_factory(
        name="target",
        vectorizer_config=[
            wvc.config.Configure.NamedVectors.none(name="bringYourOwn1"),
            wvc.config.Configure.NamedVectors.none(name="bringYourOwn2"),
        ],
    )

    source = collection_factory(
        name="source",
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    uuid_target = target.data.insert(
        properties={}, vector={"bringYourOwn1": [1, 2, 3], "bringYourOwn2": [4, 5, 6]}
    )
    source.data.insert(properties={}, references={"ref": uuid_target})

    res = source.query.fetch_objects(
        return_references=wvc.query.QueryReference(link_on="ref", include_vector=True)
    )

    assert res.objects[0].references["ref"].objects[0].vector["bringYourOwn1"] == [1, 2, 3]


def test_multi_target_ref_request(collection_factory: CollectionFactory) -> None:
    none_vectorizer = wvc.config.Configure.Vectorizer.none()
    carbs = collection_factory(name="Carbs", vectorizer_config=none_vectorizer)
    fats = collection_factory(name="Fats", vectorizer_config=none_vectorizer)
    proteins = collection_factory(name="Proteins", vectorizer_config=none_vectorizer)

    foods = collection_factory(
        name="Foods",
        references=[
            wvc.config.ReferenceProperty.MultiTarget(
                name="ingredients", target_collections=[carbs.name, fats.name, proteins.name]
            )
        ],
        vectorizer_config=none_vectorizer,
    )

    carb_1 = carbs.data.insert({})
    fat_1 = fats.data.insert({})
    protein_1 = proteins.data.insert({})

    # Create a food object with 3 ingredients
    food_1 = foods.data.insert({}, references={"ingredients": [carb_1, fat_1, protein_1]})

    # Request all ingredients this food has
    result = foods.query.fetch_object_by_id(
        uuid=food_1,
        return_references=[
            wvc.query.QueryReference.MultiTarget(
                link_on="ingredients", target_collection=carbs.name
            ),
            wvc.query.QueryReference.MultiTarget(
                link_on="ingredients", target_collection=fats.name
            ),
            wvc.query.QueryReference.MultiTarget(
                link_on="ingredients", target_collection=proteins.name
            ),
        ],
    )

    ingredients = result.references.get("ingredients")
    assert ingredients is not None

    # Since we requested references for all 3 ingredient kinds above
    # we expect that all 3 will appear in the result set.
    kinds = {i.collection for i in ingredients.objects}
    want = {carbs.name, fats.name, proteins.name}
    assert kinds == want, f"wrong ingredients\nwant:\n\t{want}\ngot:\n\t{kinds}"
