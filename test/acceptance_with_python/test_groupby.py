from typing import Optional

import pytest
import weaviate
import weaviate.classes as wvc
from .conftest import CollectionFactory


@pytest.mark.parametrize("return_refs", [None, [wvc.query.QueryReference(link_on="ref")]])
def test_groupby_with_refs(
    collection_factory: CollectionFactory, return_refs: Optional[wvc.query.QueryReference]
) -> None:
    col = collection_factory(
        properties=[wvc.config.Property(name="text", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    col.config.add_reference(wvc.config.ReferenceProperty(name="ref", target_collection=col.name))

    uuids = [
        str(uid)
        for uid in col.data.insert_many(
            [
                wvc.data.DataObject(properties={"text": "a1"}, vector=[1, 0, 0]),
                wvc.data.DataObject(properties={"text": "a2"}, vector=[0, 1, 0]),
                wvc.data.DataObject(properties={"text": "a2"}, vector=[0, 0, 1]),
            ]
        ).uuids.values()
    ]

    for uid in uuids:
        col.data.reference_add_many(
            [wvc.data.DataReference(from_property="ref", from_uuid=uid, to_uuid=uid)]
        )

    return_props = None
    if return_refs is None:
        return_props = ["text"]

    res = col.query.near_object(
        uuids[0],
        group_by=wvc.query.GroupBy(prop="ref", objects_per_group=2, number_of_groups=3),
        return_properties=return_props,
        return_references=return_refs,
    )
    assert len(res.groups) == 3
    for _, grp in res.groups.items():
        for obj in grp.objects:
            if return_refs is not None:
                assert obj.references is not None
            else:
                assert len(obj.properties.get("text")) == 2

    # repeat with GQL - slightly different code path in
    client = weaviate.connect_to_local()
    ref = "_additional{id distance}"
    if return_refs is None:
        ref = f"text {ref}"
    if return_refs is not None:
        ref = "ref{... on " + col.name + "{_additional{id}}} _additional{id distance}"
    hits = "hits{" + ref + "}"
    group = f"group{{ id groupedBy {{ value path }} count maxDistance minDistance {hits} }}"
    _additional = f"_additional{{ {group} }}"
    res = client.graphql_raw_query(
        f"""{{
                Get {{
                    {col.name}(nearObject: {{id: "{uuids[0]}"}} groupBy:{{path: [\"ref\"] groups: 3 objectsPerGroup: 10}}) {{
                        {_additional}
                    }}
                }}
            }}"""
    )

    assert res.errors is None
    for group in res.get[col.name]:
        assert len(group["_additional"]["group"]["hits"]) == 1
        if return_refs is not None:
            assert group["_additional"]["group"]["hits"][0]["ref"] is not None
        else:
            assert len(group["_additional"]["group"]["hits"][0]["text"]) == 2
