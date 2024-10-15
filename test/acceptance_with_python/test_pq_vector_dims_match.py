import pytest
import time
import numpy as np

from .conftest import CollectionFactory
from weaviate.classes.config import Configure, Reconfigure
from weaviate.classes.query import MetadataQuery
from weaviate.exceptions import WeaviateQueryError


VEC_DIMS = 256
OBJ_NUM = 1024


def test_pq_dims_match(collection_factory: CollectionFactory):
    col = collection_factory(
        name='CompressedVector',
        vectorizer_config=Configure.Vectorizer.none()
    )

    with col.batch.dynamic() as batch:
        for i in range(OBJ_NUM):
            batch.add_object(
                properties={
                    'someText': f'object-{i}'
                },
                vector=generate_vec(VEC_DIMS)
            )

    col.config.update(
        vector_index_config=Reconfigure.VectorIndex.hnsw(
            quantizer=Reconfigure.VectorIndex.Quantizer.pq()
        )
    )

    # time to quantize
    print('sleeping for 5 seconds to compress vectors...')
    time.sleep(5)

    with pytest.raises(WeaviateQueryError) as exc:
        col.query.near_vector(
            near_vector=generate_vec(128),
            limit=2,
            return_metadata=MetadataQuery(distance=True)
        )
    assert "ProductQuantizer.DistanceBetweenCompressedAndUncompressedVectors: mismatched dimensions:" in str(exc.value)


def generate_vec(dims):
    return np.random.random(dims).tolist()
