import random
from typing import Union, List, Optional

import pytest
import weaviate.classes as wvc
from faker import Faker
from weaviate.collections.classes.config_vector_index import (
    _BQConfigCreate,
    _SQConfigCreate,
    _RQConfigCreate,
    _PQConfigCreate,
    _VectorIndexConfigCreate,
    _QuantizerConfigUpdate,
)
from weaviate.collections.classes.config_vectorizers import _VectorizerConfigCreate
from weaviate.collections.classes.config_vectors import _VectorConfigCreate

from . import get_debug_usage as debug_usage
from .conftest import CollectionFactory
from .get_debug_usage import ShardUsage, VectorUsage

vectors = wvc.config.Configure.Vectors
vectorizers = wvc.config.Configure.Vectorizer
quantizer = wvc.config.Configure.VectorIndex.Quantizer


def tenant_objects_count(tenant_id: int) -> int:
    return 50 + tenant_id


vector_names = ["first", "second", "third"]


def test_usage_adding_named_vector(collection_factory: CollectionFactory):
    vec_config = vectors.self_provided(
        name="first",
        quantizer=quantizer.bq(),
        vector_index_config=wvc.config.Configure.VectorIndex.flat(),
    )
    collection = collection_factory(
        vector_config=vec_config,  # either vector_config or vectorizer_config
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=True),
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
    )

    collection.tenants.create(["tenant1"])
    uuid1 = collection.with_tenant("tenant1").data.insert(
        {"name": "some text"}, vector={"first": [0.1, 0.2, 0.3]}
    )
    collection.with_tenant("tenant1").data.insert({"name": "some text"})

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection is not None
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == 1
    tenant1_usage = usage_collection.shards[0]
    assert tenant1_usage.name == "tenant1"
    assert tenant1_usage.objects_count == 2
    assert len(tenant1_usage.named_vectors) == 1

    def verify_named_vector1(named_vector: VectorUsage) -> None:
        assert named_vector.name == "first"
        assert named_vector.compression == "bq"
        assert named_vector.vector_index_type == "flat"
        assert len(named_vector.dimensionalities) == 1
        dim = named_vector.dimensionalities[0]
        assert dim.dimensions == 3
        assert dim.count == 1

    verify_named_vector1(tenant1_usage.named_vectors[0])

    # verify adding a new named vector
    collection.config.add_vector(
        vector_config=wvc.config.Configure.Vectors.self_provided(
            name="second", quantizer=quantizer.rq()
        ),
    )

    collection.with_tenant("tenant1").data.update(uuid1, vector={"second": [0.4, 0.5]})

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection is not None
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == 1

    # verify that second vector is added
    tenant1_usage = usage_collection.shards[0]
    assert tenant1_usage.name == "tenant1"
    assert tenant1_usage.objects_count == 2
    assert len(tenant1_usage.named_vectors) == 2
    named_vector_first = next(nv for nv in tenant1_usage.named_vectors if nv.name == "first")
    named_vector_second = next(nv for nv in tenant1_usage.named_vectors if nv.name == "second")

    verify_named_vector1(named_vector_first)  # verify first vector is unchanged

    assert named_vector_second.name == "second"
    assert named_vector_second.compression == "rq"
    assert named_vector_second.vector_index_type == "hnsw"
    assert len(named_vector_second.dimensionalities) == 1
    dimensionality = named_vector_second.dimensionalities[0]
    assert dimensionality.dimensions == 2
    assert dimensionality.count == 1


@pytest.mark.parametrize(
    "vector_config, vectorizer_config, vector_index_config",
    [
        (vectors.self_provided(name=vector_names[0]), None, None),
        ([vectors.self_provided(name="first"), vectors.self_provided(name="second")], None, None),
        ([vectors.self_provided(name="first", quantizer=quantizer.rq())], None, None),
        (
            [
                vectors.self_provided(
                    name="first",
                    quantizer=quantizer.bq(),
                    vector_index_config=wvc.config.Configure.VectorIndex.flat(),
                ),
                vectors.self_provided(name="second", quantizer=quantizer.sq()),
                vectors.self_provided(name="third", quantizer=quantizer.pq()),
            ],
            None,
            None,
        ),
        (None, vectorizers.none(), None),
        (None, vectorizers.none(), wvc.config.Configure.VectorIndex.hnsw(quantizer=quantizer.rq())),
        (None, vectorizers.none(), wvc.config.Configure.VectorIndex.flat(quantizer=quantizer.bq())),
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
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
    )

    fake = Faker()
    Faker.seed(4321)

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
            collection.with_tenant("tenant" + str(i)).data.insert(
                {"name": fake.text()}, vector=vector
            )

    # test usage for HOT tenants
    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection is not None
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == num_tenants

    for shard in usage_collection.shards:
        analyse_tenant(shard, True, num_vector_indices, vector_config, vector_index_config)

    # now deactivate some tenats and check usage again
    collection.tenants.deactivate(["tenant" + str(i) for i in range(0, num_tenants, 2)])
    usage_collection_col = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection is not None
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == num_tenants

    for shard in usage_collection_col.shards:
        tenant_id = int(shard.name.removeprefix("tenant"))
        assert len(shard.named_vectors) == num_vector_indices
        analyse_tenant(
            shard, tenant_id % 2 != 0, num_vector_indices, vector_config, vector_index_config
        )


@pytest.mark.parametrize(
    "quantizer_config",
    [
        wvc.config.Reconfigure.VectorIndex.Quantizer.bq(enabled=True),
        wvc.config.Reconfigure.VectorIndex.Quantizer.rq(enabled=True),
        wvc.config.Reconfigure.VectorIndex.Quantizer.sq(enabled=True, training_limit=50),
    ],
)
def test_usage_enabling_compression(
    collection_factory: CollectionFactory, quantizer_config: _QuantizerConfigUpdate
) -> None:
    collection = collection_factory(vector_config=vectors.self_provided(name=vector_names[0]))

    # add 1000 objects
    for i in range(10):
        collection.data.insert_many(
            [
                wvc.data.DataObject(
                    properties={}, vector={vector_names[0]: [random.random() for _ in range(150)]}
                )
                for _ in range(100)
            ]
        )

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection is not None
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == 1
    shard = usage_collection.shards[0]
    assert shard.objects_count == 1000
    assert len(shard.named_vectors) == 1
    named_vector = shard.named_vectors[0]
    assert named_vector.name == vector_names[0]
    assert len(named_vector.dimensionalities) == 1
    dimensionality = named_vector.dimensionalities[0]
    assert dimensionality.dimensions == 150
    assert dimensionality.count == 1000
    assert named_vector.vector_index_type == "hnsw"
    assert named_vector.compression == "standard"

    # enable compression
    collection.config.update(
        vector_config=wvc.config.Reconfigure.Vectors.update(
            name=vector_names[0],
            vector_index_config=wvc.config.Reconfigure.VectorIndex.hnsw(quantizer=quantizer_config),
        )
    )

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection is not None
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == 1
    shard = usage_collection.shards[0]
    assert shard.objects_count == 1000
    assert len(shard.named_vectors) == 1
    named_vector = shard.named_vectors[0]
    assert named_vector.name == vector_names[0]
    assert len(named_vector.dimensionalities) == 1
    dimensionality = named_vector.dimensionalities[0]
    assert dimensionality.dimensions == 150
    assert dimensionality.count == 1000
    assert named_vector.vector_index_type == "hnsw"
    if quantizer_config.quantizer_name() == "bq":
        assert named_vector.compression == "bq"
        assert named_vector.vector_compression_ratio == 32
    elif quantizer_config.quantizer_name() == "rq":
        assert named_vector.compression == "rq"
        assert named_vector.vector_compression_ratio != 1  # not constant
    elif quantizer_config.quantizer_name() == "sq":
        assert named_vector.compression == "sq"
        assert named_vector.vector_compression_ratio != 1  # not constant


def analyse_tenant(
    shard: ShardUsage,
    is_active: bool,
    num_vector_indices: int,
    vector_config: Optional[_VectorConfigCreate],
    vector_index_config: Optional[_VectorIndexConfigCreate],
) -> None:
    tenant_id = int(shard.name.removeprefix("tenant"))
    assert len(shard.named_vectors) == num_vector_indices
    if is_active:
        shard.status = "active"
        assert shard.objects_count == tenant_objects_count(
            tenant_id
        )  # inactive count is too unreliable
    else:
        assert shard.status == "inactive"

    for named_vector in shard.named_vectors:
        if vector_config is not None:
            vector_index = vector_names.index(named_vector.name)
            vec_index_config = vector_config[vector_index].vectorIndexConfig
        else:
            vec_index_config = vector_index_config
            vector_index = 0

        assert (
            named_vector.dimensionalities[0].count == tenant_objects_count(tenant_id) - 1
        )  # first object has no vector
        assert named_vector.dimensionalities[0].dimensions == tenant_id + vector_index + 50

        if vec_index_config is not None:
            assert named_vector.vector_index_type == vec_index_config.vector_index_type()
            if isinstance(vec_index_config.quantizer, _BQConfigCreate):
                assert named_vector.compression == _BQConfigCreate.quantizer_name()
                if is_active:
                    if vec_index_config.vector_index_type() == "flat":
                        assert named_vector.vector_compression_ratio == 1  # not set for flat
                    else:
                        assert named_vector.vector_compression_ratio == 32
            elif isinstance(vec_index_config.quantizer, _SQConfigCreate):
                assert named_vector.compression == _SQConfigCreate.quantizer_name()
                # SQ compression is only enabled for async indexing after training
            elif isinstance(vec_index_config.quantizer, _RQConfigCreate):
                assert named_vector.compression == _RQConfigCreate.quantizer_name()
                if is_active:
                    assert named_vector.vector_compression_ratio != 1  # not constant
            elif isinstance(vec_index_config.quantizer, _PQConfigCreate):
                assert named_vector.compression == _PQConfigCreate.quantizer_name()
                # PQ compression is only enabled after training
