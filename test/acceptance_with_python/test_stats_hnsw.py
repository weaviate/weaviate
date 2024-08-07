import uuid
from typing import List, Optional, Union
import httpx
import pytest
from weaviate.classes.config import Configure, DataType, Property
from weaviate.classes.data import DataObject
from weaviate.classes.query import HybridVector, MetadataQuery, Move
from weaviate.collections.classes.grpc import PROPERTIES
from weaviate.types import UUID
import weaviate
from .conftest import CollectionFactory


def test_stats_hnsw(weaviate_client) -> None:

    client = weaviate_client(8080, 50051)
    client.collections.delete_all()

    collection = client.collections.create("Vector")

    collections = client.collections.list_all(simple=False)

    print("Collections: ",collections)

    NUM_OBJ = 5
    data_rows = [{"title": f"Object {i+1}"} for i in range(int(NUM_OBJ))]
    vectors = [[0.1] * 1 for i in range(int(NUM_OBJ))]
    with collection.batch.dynamic() as batch:
        for i, data_row in enumerate(data_rows):
            batch.add_object(
                properties=data_row,
                vector=vectors[i]
            )

    shards = collection.config.get_shards()
    print("Shards: ",shards[0].name)
    

    url = "http://localhost:6060/debug/stats/collection/Vector/shard/"+shards[0].name
    response = httpx.get(url)
    print("url: ", url)
    print("Status code: ",response.status_code)
    print("Response: ",response)
    assert response.status_code == 200


