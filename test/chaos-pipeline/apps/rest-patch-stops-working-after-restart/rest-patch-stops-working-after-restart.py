"""
https://semi-technology.atlassian.net/browse/WEAVIATE-151
"""

import weaviate
from loguru import logger
import sys, traceback

def create_weaviate_schema():
    schema = {
        "classes": [
            {
                "class": "PatchStopsWorkingAfterRestart",
                "vectorizer": "none",
                "vectorIndexType": "hnsw",
                "invertedIndexConfig": {
                  "bm25": {
                    "b": 0.75,
                    "k1": 1.2
                  },
                  "cleanupIntervalSeconds": 60,
                  "stopwords": {
                    "preset": "en"
                  }
                },
                "properties": [
                    {
                        "dataType": [
                            "string"
                        ],
                        "name": "description",
                        "tokenization": "word",
                        "indexInverted": True
                    },
                ]
            },
        ]
    }
    # add schema
    if not client.schema.contains(schema):
        client.schema.create(schema)

def get_body(index: str, req_type: str):
    return { "description": f"this is an update number: {index} with {req_type}" }

def create_object_if_it_doesnt_exist(class_name: str, object_id: str):
  try:
    exists = client.data_object.exists(object_id)
    if not exists:
      client.data_object.create(get_body(0, "create"), class_name, object_id)
  except Exception as e:
      logger.error(f"Error adding {class_name} object - id: {object_id}")
      logger.error(e)
      logger.error(''.join(traceback.format_tb(e.__traceback__)))

def constant_updates(class_name: str, object_id: str):
  loops = 1000
  for i in range(loops):
    try:
      client.data_object.replace(get_body(i, "put"), class_name, object_id)
      client.data_object.update(get_body(i, "patch"), class_name, object_id, [0.1, 0.2, 0.1, 0.3])
    except Exception as e:
      logger.error(f"Error updating {class_name} object - id: {object_id}")
      logger.error(e)
      logger.error(''.join(traceback.format_tb(e.__traceback__)))
      raise Exception('Error occured during update')

if __name__ == "__main__":
    client = weaviate.Client("http://localhost:8080")
    try:
      create_weaviate_schema()
      class_name = "PatchStopsWorkingAfterRestart"
      object_id = "07e9828d-ff0a-5e47-8101-b52312345678"
      create_object_if_it_doesnt_exist(class_name, object_id)
      constant_updates(class_name, object_id)
    except:
      logger.error(sys.exc_info()[1])
      logger.error(''.join(traceback.format_tb(sys.exc_info()[2])))
      exit(1)
