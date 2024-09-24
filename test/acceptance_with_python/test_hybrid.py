import uuid
from typing import Optional

import pytest

from weaviate.classes.config import Configure, Property, DataType
import weaviate.classes as wvc
from weaviate.collections.classes.grpc import HybridVectorType

from .conftest import CollectionFactory


UUID1 = uuid.uuid4()
UUID2 = uuid.uuid4()
UUID3 = uuid.uuid4()
UUID4 = uuid.uuid4()


@pytest.mark.parametrize("vector", [None, wvc.query.HybridVector.near_text("summer dress")])
def test_hybrid_with_offset(
    collection_factory: CollectionFactory, vector: Optional[HybridVectorType]
) -> None:
    collection = collection_factory(
        properties=[Property(name="name", data_type=DataType.TEXT)],
        vectorizer_config=Configure.Vectorizer.text2vec_contextionary(
            vectorize_collection_name=False
        ),
    )

    ret = collection.data.insert_many(
        [
            {"name": entry}
            for entry in [
                "mountain hike",
                "banana apple",
                "road trip",
                "coconut smoothie",
                "beach vacation",
                "apple pie",
                "banana split",
                "mountain biking",
                "apple cider",
                "beach volleyball",
                "sailing",
            ]
        ]
    )
    assert ret.has_errors is False

    hy = collection.query.hybrid("summer dress")
    assert len(hy.objects) > 0

    hy_offset = collection.query.hybrid("summer dress", offset=2, vector=vector)
    assert len(hy_offset.objects) + 2 == len(hy.objects)
