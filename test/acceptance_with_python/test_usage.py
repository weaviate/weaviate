import random
import time
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
    _PQConfigUpdate,
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


vector_names = ["first", "second", "third", "fourth"]


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
        (
            None,
            vectorizers.none(),
            wvc.config.Configure.VectorIndex.hnsw(
                quantizer=quantizer.rq(bits=8)
            ),  # change to bits=1 in 1.33 to also test
        ),
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
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == num_tenants

    for shard in usage_collection.shards:
        analyse_tenant(shard, True, num_vector_indices, vector_config, vector_index_config)

    # now deactivate some tenats and check usage again
    collection.tenants.deactivate(["tenant" + str(i) for i in range(0, num_tenants, 2)])
    usage_collection_col = debug_usage.get_debug_usage_for_collection(collection.name)
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
        wvc.config.Reconfigure.VectorIndex.Quantizer.pq(enabled=True, training_limit=150),
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

    time.sleep(0.5)  # wait for async training to finish

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
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
    assert not named_vector.multi_vector_config.enabled
    if quantizer_config.quantizer_name() == "bq":
        assert named_vector.compression == "bq"
        assert named_vector.vector_compression_ratio == 32
    elif quantizer_config.quantizer_name() == "rq":
        assert named_vector.compression == "rq"
        assert named_vector.vector_compression_ratio != 1  # not constant
    elif quantizer_config.quantizer_name() == "sq":
        assert named_vector.compression == "sq"
        assert named_vector.vector_compression_ratio != 1  # not constant


def test_multi_vector(collection_factory: CollectionFactory):
    collection = collection_factory(
        vector_config=[
            wvc.config.Configure.MultiVectors.self_provided(name=vector_names[0]),
            wvc.config.Configure.MultiVectors.self_provided(
                name=vector_names[1],
                encoding=wvc.config.Configure.VectorIndex.MultiVector.Encoding.muvera(),
            ),
            wvc.config.Configure.MultiVectors.self_provided(
                name=vector_names[2],
                encoding=wvc.config.Configure.VectorIndex.MultiVector.Encoding.muvera(),
                quantizer=quantizer.bq(),
            ),
            wvc.config.Configure.MultiVectors.self_provided(
                name=vector_names[3], quantizer=quantizer.bq()
            ),
        ]
    )

    collection.data.insert(
        {},
        vector={
            vector_names[0]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
            vector_names[1]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
            vector_names[2]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
            vector_names[3]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
        },
    )
    collection.data.insert(
        {},
        vector={
            vector_names[0]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
            vector_names[1]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
            vector_names[2]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
            vector_names[3]: [[0.1, 0.2], [0.4, 0.5], [0.7, 0.8]],
        },
    )
    collection.data.insert({})

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == 1
    shard = usage_collection.shards[0]
    assert shard.objects_count == 3
    assert len(shard.named_vectors) == 4
    named_vector_pure = next(nv for nv in shard.named_vectors if nv.name == vector_names[0])
    assert named_vector_pure.name == vector_names[0]
    assert len(named_vector_pure.dimensionalities) == 1
    dimensionality = named_vector_pure.dimensionalities[0]
    assert dimensionality.dimensions == 6  # 3 vectors of 2 dimensions each
    assert dimensionality.count == 2
    assert named_vector_pure.compression == "standard"
    assert named_vector_pure.vector_compression_ratio == 1
    assert named_vector_pure.multi_vector_config.enabled
    assert not named_vector_pure.multi_vector_config.muvera_config.enabled

    named_vector_muvera = next(nv for nv in shard.named_vectors if nv.name == vector_names[1])
    assert named_vector_muvera.name == vector_names[1]
    assert len(named_vector_muvera.dimensionalities) == 1
    dimensionality = named_vector_muvera.dimensionalities[0]
    assert dimensionality.dimensions == 6  # 3 vectors of 2 dimensions each
    assert dimensionality.count == 2
    assert named_vector_muvera.compression == "standard"
    assert named_vector_muvera.vector_compression_ratio == 1
    assert named_vector_muvera.multi_vector_config.enabled
    assert named_vector_muvera.multi_vector_config.muvera_config.enabled

    named_vector_muvera_bq = next(nv for nv in shard.named_vectors if nv.name == vector_names[2])
    assert named_vector_muvera_bq.name == vector_names[2]
    assert len(named_vector_muvera_bq.dimensionalities) == 1
    dimensionality = named_vector_muvera_bq.dimensionalities[0]
    assert dimensionality.dimensions == 6  # 3 vectors of 2 dimensions each
    assert dimensionality.count == 2
    assert named_vector_muvera_bq.compression == "bq"
    assert named_vector_muvera_bq.vector_compression_ratio == 32
    assert named_vector_muvera_bq.multi_vector_config.enabled
    assert named_vector_muvera_bq.multi_vector_config.muvera_config.enabled

    named_vector_bq = next(nv for nv in shard.named_vectors if nv.name == vector_names[3])
    assert named_vector_bq.name == vector_names[3]
    assert len(named_vector_bq.dimensionalities) == 1
    dimensionality = named_vector_bq.dimensionalities[0]
    assert dimensionality.dimensions == 6  # 3 vectors of 2 dimensions each
    assert dimensionality.count == 2
    assert named_vector_bq.compression == "bq"
    assert named_vector_bq.vector_compression_ratio == 32
    assert named_vector_muvera_bq.multi_vector_config.enabled


def test_object_storage(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[
            wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="description", data_type=wvc.config.DataType.TEXT),
        ],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=True),
    )

    collection.tenants.create("tenant")
    collection = collection.with_tenant("tenant")

    # Adding 100 objects with increasing size of text properties
    # each object will have
    #  - names of the properties "name" and "description" (11 bytes, 1100 bytes in total)
    #  - content of the two properties with i bytes each => 9900 bytes in total
    #  - collection name and tenant name overhead (let's assume 50 bytes, 5000 bytes in total)
    # So total storage should be around 16000 bytes
    #
    # However, there is a lot of overhead in the storage engine as well, especially because we have the primary and two
    # secondary indices, which are roughly the size of the data itself
    guesstimate = 16000 * 2

    collection.data.insert_many(
        [
            wvc.data.DataObject(properties={"name": "a" * i, "description": "b" * i})
            for i in range(100)
        ]
    )

    # deactivate and reactivate, to make sure that everything is written to disk
    collection.tenants.deactivate("tenant")
    collection.tenants.activate("tenant")

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == 1
    shard = usage_collection.shards[0]
    assert shard.objects_count == 100

    collection.tenants.deactivate("tenant")

    usage_collection_cold = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection_cold.name == collection.name
    assert len(usage_collection_cold.shards) == 1
    shard_cold = usage_collection_cold.shards[0]

    assert (
        shard_cold.objects_storage_bytes == shard.objects_storage_bytes
    )  # hot and cold computation should result in the same value
    assert shard_cold.objects_storage_bytes > guesstimate
    assert shard_cold.objects_storage_bytes < 3 * guesstimate

    assert shard_cold.index_storage_bytes == shard.index_storage_bytes
    assert shard_cold.full_shard_storage_bytes == shard.full_shard_storage_bytes
    # shard storage must be larger than sum of components
    assert (
        shard.full_shard_storage_bytes
        > shard.vector_storage_bytes + shard.index_storage_bytes + shard.objects_storage_bytes
    )


def test_storage_vectors(collection_factory: CollectionFactory):
    collection = collection_factory(
        vector_config=[
            vectors.self_provided(
                name="first",
                quantizer=quantizer.bq(),
                vector_index_config=wvc.config.Configure.VectorIndex.flat(),
            ),
            vectors.self_provided(name="second", quantizer=quantizer.bq()),
        ],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=True),
    )

    collection.tenants.create("tenant")
    collection = collection.with_tenant("tenant")

    collection.data.insert_many(
        [
            wvc.data.DataObject(
                properties={},
                vector={
                    vector_names[0]: [random.random() for _ in range(100)],
                    vector_names[1]: [random.random() for _ in range(125)],
                },
            )
            for _ in range(1000)
        ]
    )

    # deactivate and reactivate, to make sure that all vector data is written to disk
    collection.tenants.deactivate("tenant")
    collection.tenants.activate("tenant")

    # we have 1000 objects now with 2 vectors each, with 100 and 125 dimensions respectively
    # uncompressed storage is 4 bytes per dimension
    # so total uncompressed storage is 1000 * (100 + 125) * 4 = 900000 bytes
    # with bq compression we have 32x compression, so storage should be around 28125 bytes in total
    # resulting in 928125 bytes in total.
    # Moreover, the flat index does use an additional bucket to store the uncompressed vectors, resulting in an
    # additional 400000 bytes for the first vector, so total storage should be around 1328125 bytes

    usage_collection = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection.name == collection.name
    assert len(usage_collection.shards) == 1
    shard = usage_collection.shards[0]
    assert shard.objects_count == 1000
    assert len(shard.named_vectors) == 2

    collection.tenants.deactivate("tenant")

    usage_collection_cold = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection_cold.name == collection.name
    assert len(usage_collection_cold.shards) == 1
    shard_cold = usage_collection_cold.shards[0]
    assert len(shard_cold.named_vectors) == 2

    assert (
        shard_cold.vector_storage_bytes == shard.vector_storage_bytes
    )  # hot and cold computation should result in the same value
    # we want AT LEAST the calculated value, but it can be higher due to overhead
    assert shard_cold.vector_storage_bytes > 1328125
    assert shard_cold.vector_storage_bytes < 1328125 * 1.25  # allow 25% overhead

    assert shard_cold.index_storage_bytes == shard.index_storage_bytes
    assert shard_cold.full_shard_storage_bytes == shard.full_shard_storage_bytes
    # shard storage must be larger than sum of components
    assert (
        shard.full_shard_storage_bytes
        > shard.vector_storage_bytes + shard.index_storage_bytes + shard.objects_storage_bytes
    )


def test_usage_with_caching(collection_factory: CollectionFactory):
    collection = collection_factory(
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vector_config=[
            vectors.self_provided(
                name="first",
                quantizer=quantizer.bq(),
                vector_index_config=wvc.config.Configure.VectorIndex.flat(),
            ),
            vectors.self_provided(name="second", quantizer=quantizer.bq()),
        ],
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=True),
    )

    collection.tenants.create("tenant")
    collection = collection.with_tenant("tenant")

    collection.data.insert_many(
        [
            wvc.data.DataObject(
                properties={"name": "object" + str(i)},
                vector={
                    vector_names[0]: [random.random() for _ in range(100)],
                    vector_names[1]: [random.random() for _ in range(125)],
                },
            )
            for i in range(100)
        ]
    )

    # necessary that all cna files are written to disk
    collection.tenants.deactivate("tenant")
    collection.tenants.activate("tenant")
    collection.tenants.deactivate("tenant")

    usage_collection_cold1 = debug_usage.get_debug_usage_for_collection(collection.name)
    usage_collection_cold2 = debug_usage.get_debug_usage_for_collection(collection.name)
    assert usage_collection_cold1.name == collection.name
    shard = usage_collection_cold2.shards[0]
    assert shard.objects_count == 100
    assert usage_collection_cold1 == usage_collection_cold2

    # activate again and add more
    collection.tenants.activate("tenant")
    collection.data.insert_many(
        [
            wvc.data.DataObject(
                properties={"name": "object" + str(i + 100)},
                vector={
                    vector_names[0]: [random.random() for _ in range(100)],
                    vector_names[1]: [random.random() for _ in range(125)],
                },
            )
            for i in range(10)
        ]
    )

    # change has been picked up
    collection.tenants.deactivate("tenant")
    collection.tenants.activate("tenant")
    collection.tenants.deactivate("tenant")

    usage_collection_cold3 = debug_usage.get_debug_usage_for_collection(collection.name)
    shard = usage_collection_cold3.shards[0]
    assert shard.objects_count == 110


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
                # assert named_vector.bits == vec_index_config.quantizer.bits - uncomment in 1.33 to also test bits=1
                if is_active:
                    assert named_vector.vector_compression_ratio != 1  # not constant
            elif isinstance(vec_index_config.quantizer, _PQConfigCreate):
                assert named_vector.compression == _PQConfigCreate.quantizer_name()
                # PQ compression is only enabled after training
