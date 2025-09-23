import os
import time
import weaviate
import numpy as np
from weaviate.classes.config import Configure
from weaviate.classes.config import Property, DataType
from weaviate.classes.config import Reconfigure
from weaviate.util import generate_uuid5


def test_index_compression() -> None:
    with weaviate.connect_to_local() as client:
        COLLECTION_NAME = "Compressed"

        target_vector_dimensions = {
            "uncompressed": 64,
            "flat_bq": 128,
            "hnsw_bq": 256,
            "hnsw_pq": 512,
            "hnsw_sq": 1024,
        }

        def generate_random_vector(dimensionality):
            return np.random.rand(dimensionality)

        def query_all_target_vectors():
            for target_vector, dim in target_vector_dimensions.items():
                res = collection.query.near_vector(
                    near_vector=generate_random_vector(dim),
                    target_vector=target_vector,
                )
                assert len(res.objects) > 0

        client.collections.delete(COLLECTION_NAME)

        collection = client.collections.create(
            name=COLLECTION_NAME,
            vector_config=[
                Configure.Vectors.self_provided(
                    name="uncompressed",
                    vector_index_config=Configure.VectorIndex.hnsw(),
                ),
                Configure.Vectors.self_provided(
                    name="flat_bq",
                    vector_index_config=Configure.VectorIndex.flat(),
                    quantizer=Configure.VectorIndex.Quantizer.bq(),
                ),
                Configure.Vectors.self_provided(
                    name="hnsw_bq",
                    vector_index_config=Configure.VectorIndex.hnsw(),
                    quantizer=Configure.VectorIndex.Quantizer.bq(),
                ),
                Configure.Vectors.self_provided(
                    name="hnsw_pq",
                    vector_index_config=Configure.VectorIndex.hnsw(),
                ),
                Configure.Vectors.self_provided(
                    name="hnsw_sq",
                    vector_index_config=Configure.VectorIndex.hnsw(),
                ),
            ],
            properties=[
                Property(name="name", data_type=DataType.TEXT),
                Property(name="description", data_type=DataType.TEXT),
            ]
        )

        with collection.batch.dynamic() as batch:
            for i in range(1000): 
                batch.add_object(
                    properties={
                        "name": f"name {i}",
                        "description": f"some description {i}",
                    },
                    uuid=generate_uuid5(f"name {i}"),
                    vector={
                        "uncompressed": generate_random_vector(target_vector_dimensions["uncompressed"]),
                        "flat_bq": generate_random_vector(target_vector_dimensions["flat_bq"]),
                        "hnsw_bq": generate_random_vector(target_vector_dimensions["hnsw_bq"]),
                        "hnsw_pq": generate_random_vector(target_vector_dimensions["hnsw_pq"]),
                        "hnsw_sq": generate_random_vector(target_vector_dimensions["hnsw_sq"]),
                }
        )

        count = collection.aggregate.over_all()
        assert count.total_count == 1000

        query_all_target_vectors()

        collection.config.update(
            vector_config=Reconfigure.Vectors.update(
                name="hnsw_pq",
                vector_index_config=Reconfigure.VectorIndex.hnsw(
                    quantizer=Reconfigure.VectorIndex.Quantizer.pq(
                        enabled=True,
                        training_limit=100,
                    ),
                )
            )
        )
        # we need to wait a little bit before we can enable next compression
        time.sleep(3)
        collection.config.update(
            vector_config=Reconfigure.Vectors.update(
                name="hnsw_sq",
                vector_index_config=Reconfigure.VectorIndex.hnsw(
                    quantizer=Reconfigure.VectorIndex.Quantizer.sq(
                        enabled=True,
                        training_limit=100,
                    ),
                )
            )
        )

        query_all_target_vectors()

        res = client.cluster.nodes(COLLECTION_NAME, output="verbose")
        assert len(res) > 0
        assert len(res[0].shards) > 0
        shard_name = res[0].shards[0].name

        for target_vector in target_vector_dimensions.keys():
            if target_vector != "uncompressed":
                directory = f"./data/{COLLECTION_NAME.lower()}/{shard_name}/lsm/vectors_compressed_{target_vector}"
                assert os.path.isdir(directory) == True
