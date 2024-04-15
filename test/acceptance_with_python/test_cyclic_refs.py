import weaviate.classes as wvc

from conftest import CollectionFactory


def test_ref_with_cycle(collection_factory: CollectionFactory) -> None:
    col = collection_factory(
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)]
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


def test_ref_with_multiple_cycle(collection_factory: CollectionFactory) -> None:
    col = collection_factory(
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)]
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
