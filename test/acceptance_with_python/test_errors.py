import pytest
from weaviate.exceptions import WeaviateQueryError

from .conftest import CollectionFactory
import weaviate.classes as wvc


def test_error_no_module_for_vectorizer(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        properties=[wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=[wvc.config.Configure.NamedVectors.none(name="custom")],
    )

    collection.data.insert({"title": "Hello"})

    with pytest.raises(WeaviateQueryError) as exc:
        collection.query.near_text("hello")
    assert "could not vectorize input" in str(exc.value)
