import pytest
import time
import numpy as np

from .conftest import CollectionFactory
from weaviate.classes.config import Configure, Reconfigure
from weaviate.classes.query import MetadataQuery
from weaviate.exceptions import WeaviateQueryError


VEC_DIMS = 256
OBJ_NUM = 1024


@pytest.mark.skip(
    reason="fix PQ to return error when searching with wrong vector dimensions. This is a regression that was introduced in 1.26"
)
def test_pq_dims_match(collection_factory: CollectionFactory):
    col = collection_factory(name="CompressedVector", vectorizer_config=Configure.Vectorizer.none())

    with col.batch.dynamic() as batch:
        for i in range(OBJ_NUM):
            batch.add_object(properties={"someText": f"object-{i}"}, vector=generate_vec(VEC_DIMS))

    col.config.update(
        vector_index_config=Reconfigure.VectorIndex.hnsw(
            quantizer=Reconfigure.VectorIndex.Quantizer.pq()
        )
    )

    # time to quantize
    time2sleep = 3
    print(f"sleeping for {time2sleep} seconds to compress vectors...")
    time.sleep(time2sleep)

    with pytest.raises(WeaviateQueryError) as exc:
        col.query.near_vector(
            near_vector=generate_vec(128), limit=2, return_metadata=MetadataQuery(distance=True)
        )
    assert (
        "ProductQuantizer.DistanceBetweenCompressedAndUncompressedVectors: mismatched dimensions:"
        in str(exc.value)
    )


# Regression tests for https://github.com/weaviate/weaviate/issues/12207:
# a nearVector query with the wrong dimensionality against an RQ-compressed
# (any bits) or BQ-compressed (under-dimensioned) HNSW index used to silently
# return an empty result set (HTTP 200, no errors) instead of raising the
# same "vector lengths don't match" error that uncompressed HNSW, flat, and
# SQ-compressed indexes correctly raise. Unlike the PQ test above, RQ and BQ
# are enabled from collection creation (no separate compress/quantize step is
# needed), matching how the issue was reproduced.
@pytest.mark.parametrize("bits", [1, 8])
def test_rq_dims_match(collection_factory: CollectionFactory, bits: int):
    col = collection_factory(
        name="CompressedVectorRQ",
        vectorizer_config=Configure.Vectorizer.none(),
        vector_index_config=Configure.VectorIndex.hnsw(
            quantizer=Configure.VectorIndex.Quantizer.rq(bits=bits),
        ),
    )

    with col.batch.dynamic() as batch:
        for i in range(OBJ_NUM):
            batch.add_object(properties={"someText": f"object-{i}"}, vector=generate_vec(VEC_DIMS))

    for wrong_dims in (VEC_DIMS + 32, VEC_DIMS - 32):
        with pytest.raises(WeaviateQueryError) as exc:
            col.query.near_vector(
                near_vector=generate_vec(wrong_dims),
                limit=2,
                return_metadata=MetadataQuery(distance=True),
            )
        assert "vector lengths don't match" in str(exc.value)


def test_bq_dims_match(collection_factory: CollectionFactory):
    col = collection_factory(
        name="CompressedVectorBQ",
        vectorizer_config=Configure.Vectorizer.none(),
        vector_index_config=Configure.VectorIndex.hnsw(
            quantizer=Configure.VectorIndex.Quantizer.bq(),
        ),
    )

    with col.batch.dynamic() as batch:
        for i in range(OBJ_NUM):
            batch.add_object(properties={"someText": f"object-{i}"}, vector=generate_vec(VEC_DIMS))

    # Over-dimensioned queries already error before this fix (they cross a
    # 64-bit block boundary); under-dimensioned queries within the same
    # block did not. Cover both to guard against regressions in either
    # direction.
    for wrong_dims in (VEC_DIMS + 32, VEC_DIMS - 32):
        with pytest.raises(WeaviateQueryError) as exc:
            col.query.near_vector(
                near_vector=generate_vec(wrong_dims),
                limit=2,
                return_metadata=MetadataQuery(distance=True),
            )
        assert "vector lengths don't match" in str(exc.value)


def generate_vec(dims):
    return np.random.random(dims).tolist()
