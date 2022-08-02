import json
import random
from time import time
import torch
import weaviate
import math
from uuid import uuid4

client = weaviate.Client("http://localhost:8080",
        timeout_config = (5, 120)

        )  # or another location where your Weaviate instance is running
schema = {
    "classes": [{
        "class": "SemanticUnit",
        "description": "A written text, for example a news article or blog post",
        "vectorIndexType": "hnsw",
         "vectorIndexConfig": {
               "efConstruction": 128,
               "maxConnections": 64,
         },
        # "shardingConfig": {
        #     "desiredCount":4,
        # },
        "vectorizer": "none",
        "properties": [
            {
                "dataType": [
                    "string"
                ],
                "description": "ID",
                "name": "reference"
            },
            {
                "dataType": [
                    "text"
                ],
                "description": "titles of the unit",
                "name": "title",
            },
            {
                "dataType": [
                    "text"
                ],
                "description": "semantic unit flat text",
                "name": "text"
            },
            {
                "dataType": [
                    "string"
                ],
                "description": "document type",
                "name": "docType"
            },
            {
                "dataType": [
                    "int"
                ],
                "description": "so we can do some int queries",
                "name": "itemId"
            },
            {
                "dataType": [
                    "int"
                ],
                "description": "so we can do some int queries",
                "name": "itemIdHundred"
            },
            {
                "dataType": [
                    "int"
                ],
                "description": "so we can do some int queries",
                "name": "itemIdTen"
            },
            {
                "dataType": [
                    "int"
                ],
                "description": "so we can do some int queries",
                "name": "dummy"
            }
        ]
    }]
}
# cleanup from previous runs
client.schema.delete_all()
client.schema.create(schema)
batch = weaviate.ObjectsBatchRequest()
batchSize = 256

data=[]
with open("data.json", "r") as f:
    data = json.load(f)

update_ratio = 0.0

# ids=[]

# if update_ratio != 0:
#     id_ratio = 1-update_ratio
#     id_count = len(data) * id_ratio
#     for i in range(int(id_count)):
#         ids+=[str(uuid4())]

# def get_uuid():
#     if update_ratio == 0:
#         return None

#     return random.choice(ids)

def normalize(v):
    norm=0
    for x in v:
        norm+= x*x
    norm=math.sqrt(norm)
    for i, x in enumerate(v):
        v[i] = x/norm
    return v

start = time()
for i, doc in enumerate(data):
    props = {
        "title": doc['properties']['title'],
        "text": doc['properties']['text'],
        "docType": doc['properties']['token'],
        "itemId": doc['properties']['itemId'],
        "itemIdHundred": doc['properties']['itemIdHundred'],
        "itemIdTen": doc['properties']['itemIdTen'],
        "dummy": 7,
    }
    batch.add(props, "SemanticUnit", vector=normalize(doc['vector']), uuid=doc['id'])
    # when either batch size is reached or we are at the last object
    if (i != 0 and i % batchSize == 0) or i == len(data) - 1:
        print(f'send! {i/len(data)*100:.2f}% - index time: {(time() -start)/60:.2f}mn')
        # send off the batch
        res = client.batch.create(batch)
        for single in res:
            if "errors" in single["result"]:
                print(single["result"]["errors"])
        # and reset for the next batch
        batch = weaviate.ObjectsBatchRequest()

