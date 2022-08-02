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
                "class": "ObjectsAreNotDeleted",
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

def delete_uuids(uuids, name):
    for uuid in uuids:
        client.data_object.delete(uuid)
    logger.info(f"deleted {name} objects: {len(uuids)}")

def search_object(object_id: str, should_exist: bool):
    headExists = client.data_object.exists(object_id)
    objectGetById = client.data_object.get_by_id(object_id)
    equalId = { "path": ["id"], "operator": "Equal", "valueString": object_id }
    objectGraphQLGet = (
      client.query
      .get("ObjectsAreNotDeleted", ['_additional{id}'])
      .with_where(equalId)
      .do()
    )

    objectRestExists = objectGetById is not None and len(objectGetById) > 0
    objectGraphQLExists = len(objectGraphQLGet['data']['Get']['ObjectsAreNotDeleted']) > 0
    # logger.info(f"graphQL: {objectGraphQLGet['data']['Get']['ObjectsAreNotDeleted']}")
    # logger.info(f"search {id}: should_exist: {should_exist} head: {headExists} rest: {objectRestExists} graphql: {objectGraphQLExists}")
    if not (should_exist == headExists and should_exist == objectRestExists and should_exist == objectGraphQLExists):
        logger.info(f"search {object_id}: should_exist: {should_exist} head: {headExists} rest: {objectRestExists} graphql: {objectGraphQLExists}")
        raise Exception(f"Discrepancy between REST and GraphQL: {object_id}")

def delete_object(object_id: str):
  try:
    client.data_object.delete(object_id)
  except Exception as e:
    logger.error(f"Error deleting {class_name} object - id: {object_id}")
    logger.error(e)
    logger.error(''.join(traceback.format_tb(e.__traceback__)))
    raise Exception('Error occured during delete')

if __name__ == "__main__":
    client = weaviate.Client("http://localhost:8080")
    try:
      create_weaviate_schema()
      class_name = "ObjectsAreNotDeleted"
      # object_id = "07e9828d-ff0a-5e47-8101-b52312345678"
      uuids = [
        "07e9828d-ff0a-5e47-8101-b52312345670",
        "07e9828d-ff0a-5e47-8101-b52312345671",
        "07e9828d-ff0a-5e47-8101-b52312345672",
        "07e9828d-ff0a-5e47-8101-b52312345673",
        "07e9828d-ff0a-5e47-8101-b52312345674",
        "07e9828d-ff0a-5e47-8101-b52312345675",
        "07e9828d-ff0a-5e47-8101-b52312345678"
      ]
      loops = 10000
      for i in range(loops):
        logger.info(f"RUN number: {i}")
        for id in uuids:
          create_object_if_it_doesnt_exist(class_name, id)
          search_object(id, True)
          delete_object(id)
          search_object(id, False)
    except:
      logger.error(sys.exc_info()[1])
      logger.error(''.join(traceback.format_tb(sys.exc_info()[2])))
      exit(1)
