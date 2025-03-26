from typing import List

import pytest
from _pytest.fixtures import SubRequest
from weaviate.collections.classes.config import Configure, Property, DataType

from .conftest import CollectionFactory


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
def test_generative(
    collection_factory: CollectionFactory,
    single: str,
    grouped: str,
    grouped_properties: List[str],
) -> None:
    collection = collection_factory(
        vectorizer_config=Configure.Vectorizer.none(),
        generative_config=Configure.Generative.custom("generative-dummy"),
        properties=[
            Property(name="prop", data_type=DataType.TEXT),
            Property(name="prop2", data_type=DataType.TEXT),
        ],
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


def test_generative_array(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        vectorizer_config=Configure.Vectorizer.none(),
        generative_config=Configure.Generative.custom("generative-dummy"),
        properties=[Property(name="array", data_type=DataType.TEXT_ARRAY)],
    )

    collection.data.insert({"array": ["hello", "apple"]}, vector=[1, 0])
    collection.data.insert({"array": ["world", "wide"]}, vector=[1, 0])

    ret = collection.generate.near_vector(
        [1, 0],
        single_prompt="show me {array}",
        grouped_task="combine these",
        grouped_properties=["array"],
        return_properties=[],
    )
    assert len(ret.objects) == 2

    for obj in ret.objects:
        assert "show me" in obj.generated
        assert ("hello" in obj.generated and "apple" in obj.generated) or (
            "world" in obj.generated and "wide" in obj.generated
        )
        assert "You want me to generate something based on the following prompt" in obj.generated

        assert "hello" in ret.generated
        assert "world" in ret.generated
        assert "apple" in ret.generated
        assert "wide" in ret.generated
