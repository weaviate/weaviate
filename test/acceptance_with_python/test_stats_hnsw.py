import json
import httpx
from weaviate.classes.config import Configure, VectorDistances
import weaviate


def test_stats_hnsw() -> None:
    short_url = "http://localhost:6060/debug/stats/collection/collection_name/shards"
    response = httpx.post(short_url)
    assert response.status_code == 404
    assert "invalid path" in response.text
    long_url = "http://localhost:6060/debug/stats/collection/collection_name/shards/shard_name/arg4/arg5/arg6"
    response = httpx.post(long_url)
    assert response.status_code == 404
    assert "invalid path" in response.text
    wrong_url = "http://localhost:6060/debug/stats/collection/collection_name/wrong/shard_name"
    response = httpx.post(wrong_url)
    assert response.status_code == 404
    assert "invalid path" in response.text
    # HNSW index
    client = weaviate.connect_to_local()
    client.collections.delete(name="vector")
    collection = client.collections.create_from_dict(
        {
            "class": "vector",
            "vectorizer": "none",
            "moduleConfig": {"reranker-dummy": {}},
            "properties": [{"name": "prop", "dataType": ["text"]}],
        }
    )

    collection.data.insert({"prop": "hello"}, vector=[1, 0])
    collection.data.insert({"prop": "hellohellohello"}, vector=[1, 0])
    collection.data.insert({"prop": "hellohello"}, vector=[1, 0])
    shards = collection.config.get_shards()

    wrong_collection = (
        "http://localhost:6060/debug/stats/collection/wrong_collection/shards/" + shards[0].name
    )
    response = httpx.post(wrong_collection)
    assert response.status_code == 404
    assert "collection not found" in response.text
    wrong_shard = "http://localhost:6060/debug/stats/collection/vector/shards/wrong_shard"
    response = httpx.post(wrong_shard)
    assert response.status_code == 404
    assert "shard not found" in response.text

    url = "http://localhost:6060/debug/stats/collection/vector/shards/" + shards[0].name
    response = httpx.post(url)
    keywords = list(json.loads(response.text).keys())
    assert response.status_code == 200
    assert [
        "dimensions",
        "entryPointID",
        "distributionLayers",
        "unreachablePoints",
        "numTombstones",
        "cacheSize",
        "compressed",
        "compressionStats",
        "compressionType",
    ] == keywords

    # Flat index
    flat_index = client.collections.create(
        name="flatIndex",
        vector_index_config=Configure.VectorIndex.flat(
            distance_metric=VectorDistances.COSINE,
            quantizer=None,
            vector_cache_max_objects=1000000,
        ),
    )
    flat_index.data.insert({"prop": "hello"}, vector=[1, 0])
    flat_index.data.insert({"prop": "hellohellohello"}, vector=[1, 0])
    flat_index.data.insert({"prop": "hellohello"}, vector=[1, 0])
    flat_shards = flat_index.config.get_shards()
    flat_url = (
        "http://localhost:6060/debug/stats/collection/flatIndex/shards/" + flat_shards[0].name
    )
    response = httpx.post(flat_url)
    assert response.status_code == 400
    assert "Stats() is not implemented for flat index" in response.text
