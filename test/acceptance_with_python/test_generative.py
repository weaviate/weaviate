from typing import List

import pytest
import weaviate
import weaviate.classes as wvc


# the dummy generative module is not supported in the python client => create collection from dict
@pytest.mark.parametrize(
    "single,grouped,grouped_properties",
    [
        ("show me {prop}", None, None),
        (None, "combine these", ["prop"]),
        (None, "combine these", None),
        ("show me {prop}", "combine these", ["prop"]),
    ],
)
def test_generative(single: str, grouped: str, grouped_properties: List[str]) -> None:
    client = weaviate.connect_to_local()
    collection_name = "TestGenerativeDummy"
    client.collections.delete(name=collection_name)
    collection = client.collections.create_from_dict(
        {
            "class": collection_name,
            "vectorizer": "none",
            "moduleConfig": {"generative-dummy": {}},
            "properties": [
                {"name": "prop", "dataType": ["text"]},
                {"name": "prop2", "dataType": ["text"]},
            ],
        }
    )

    collection.data.insert({"prop": "hello", "prop2": "banana"}, vector=[1, 0])
    collection.data.insert({"prop": "world", "prop2": "banana"}, vector=[1, 0])

    ret = collection.generate.near_vector(
        [1, 0],
        single_prompt=single,
        grouped_task=grouped,
        grouped_properties=grouped_properties,
        return_properties=[],
    )
    assert len(ret.objects) == 2

    if single is not None:
        for obj in ret.objects:
            assert "show me" in obj.generated
            assert "hello" in obj.generated or "world" in obj.generated
            assert (
                "You want me to generate something based on the following prompt" in obj.generated
            )
    if grouped is not None:
        assert "hello" in ret.generated
        assert "world" in ret.generated
    if grouped_properties is None and grouped is not None:
        assert "banana" in ret.generated

    client.collections.delete(name=collection_name)
