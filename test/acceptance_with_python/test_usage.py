from typing import Union, List, Optional

import pytest
import weaviate.classes as wvc
from weaviate.collections.classes.config_vector_index import (
    _BQConfigCreate,
    _SQConfigCreate,
    _RQConfigCreate,
    _PQConfigCreate,
    _VectorIndexConfigCreate,
)
from weaviate.collections.classes.config_vectorizers import _VectorizerConfigCreate
from weaviate.collections.classes.config_vectors import _VectorConfigCreate

from . import get_debug_usage as debug_usage
from .conftest import CollectionFactory

vectors = wvc.config.Configure.Vectors
vectorizers = wvc.config.Configure.Vectorizer
quantizer = wvc.config.Configure.VectorIndex.Quantizer


def tenant_objects_count(tenant_id: int) -> int:
    return 10 + tenant_id


vector_names = ["first", "second", "third"]


@pytest.mark.parametrize(
    "vector_config, vectorizer_config, vector_index_config",
    [
        (vectors.self_provided(name=vector_names[0]), None, None),
        ([vectors.self_provided(name="first"), vectors.self_provided(name="second")], None, None),
        ([vectors.self_provided(name="first", quantizer=quantizer.bq())], None, None),
        (
            [
                vectors.self_provided(name="first", quantizer=quantizer.rq()),
                vectors.self_provided(name="second", quantizer=quantizer.sq()),
                vectors.self_provided(name="third", quantizer=quantizer.pq()),
            ],
            None,
            None,
        ),
        (None, vectorizers.none(), None),
        (None, vectorizers.none(), wvc.config.Configure.VectorIndex.hnsw(quantizer=quantizer.rq())),
        # (None, vectorizers.none(), wvc.config.Configure.VectorIndex.flat(quantizer=quantizer.bq())), - flat with compresseion is not supported in the usage module
    ],
)
def test_usage_mt(
    collection_factory: CollectionFactory,
    vector_config: Union[_VectorConfigCreate, List[_VectorConfigCreate]],
    vectorizer_config: Optional[_VectorizerConfigCreate],
    vector_index_config: Optional[_VectorIndexConfigCreate],
) -> None:
    collection = collection_factory(
        vector_config=vector_config,  # either vector_config or vectorizer_config
        vectorizer_config=vectorizer_config,
        vector_index_config=vector_index_config,
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=True),
    )

    if isinstance(vector_config, _VectorConfigCreate):
        vector_config = [vector_config]

    num_vector_indices = len(vector_config) if vector_config is not None else 1

    num_tenants = 10
    # Create objects with vectors of different dimensionalities into each tenant and its vector indices. We always skip
    # the first object to have no vectors, so we can check that case as well.
    collection.tenants.create(["tenant" + str(i) for i in range(num_tenants)])
    for i in range(num_tenants):
        obj_count = tenant_objects_count(i)
        for j in range(obj_count):
            if vector_config is not None:
                vector = {}
                for k in range(num_vector_indices):
                    vector_length = i + k + 50
                    if j > 0:
                        vector[vector_names[k]] = [j * 0.01] * vector_length
            else:
                vector_length = i + 50
                if j > 0:
                    vector = [j * 0.01] * vector_length
                else:
                    vector = None
            collection.with_tenant("tenant" + str(i)).data.insert({}, vector=vector)

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection is not None
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == num_tenants

    for shard in usage_collection.shards:
        tenant_id = int(shard.name.removeprefix("tenant"))
        assert shard.objects_count == tenant_objects_count(tenant_id)
        assert len(shard.named_vectors) == num_vector_indices
        shard.status = "active"
        for named_vector in shard.named_vectors:
            if vector_config is not None:
                vector_index = vector_names.index(named_vector.name)
                vector_index_config = vector_config[vector_index].vectorIndexConfig
            else:
                vector_index_config = vector_index_config
                vector_index = 0

            assert (
                named_vector.dimensionalities[0].count == tenant_objects_count(tenant_id) - 1
            )  # first object has no vector
            assert named_vector.dimensionalities[0].dimensions == tenant_id + vector_index + 50

            if vector_index_config is not None:
                assert named_vector.vector_index_type == vector_index_config.vector_index_type()
                if isinstance(vector_index_config.quantizer, _BQConfigCreate):
                    assert named_vector.compression == _BQConfigCreate.quantizer_name()
                    assert named_vector.vector_compression_ratio == 32
                elif isinstance(vector_index_config.quantizer, _SQConfigCreate):
                    assert named_vector.compression == _SQConfigCreate.quantizer_name()
                    # SQ compression is only enabled for async indexing
                elif isinstance(vector_index_config.quantizer, _RQConfigCreate):
                    assert named_vector.compression == _RQConfigCreate.quantizer_name()
                    assert named_vector.vector_compression_ratio != 1  # not constant
                elif isinstance(vector_index_config.quantizer, _PQConfigCreate):
                    assert named_vector.compression == _PQConfigCreate.quantizer_name()
                    # PQ compression is only enabled after training
