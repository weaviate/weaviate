"""
Imports the complete Wiki dataset into Weaviate
"""

import json
from re import U
import weaviate
from uuid import uuid3, NAMESPACE_DNS
from loguru import logger
import sys, shutil, os
import traceback

def create_weaviate_schema():
    schema = {
        "classes": [
            {
                "class": "Article",
                "description": "A wikipedia article with a title and crefs",
                "vectorizer": "text2vec-contextionary",
                "vectorIndexConfig": {
                    "skip": False
                },
                "properties": [
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Title of the article",
                        "name": "title",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": False,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "Paragraph"
                        ],
                        "description": "List of paragraphs this article has",
                        "name": "hasParagraphs",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "Article"
                        ],
                        "description": "Articles this page links to",
                        "name": "linksToArticles",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    }
                ]
            },
            {
                "class": "Paragraph",
                "description": "A wiki paragraph",
                "vectorizer": "text2vec-contextionary",
                "vectorIndexConfig": {
                    "skip": False
                },
                "properties": [
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Title of the paragraph",
                        "name": "title",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "text"
                        ],
                        "description": "The content of the paragraph",
                        "name": "content",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": False,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "int"
                        ],
                        "description": "Order of the paragraph",
                        "name": "order",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "int"
                        ],
                        "description": "Number of characters in paragraph",
                        "name": "word_count",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "Article"
                        ],
                        "description": "Article this paragraph is in",
                        "name": "inArticle",
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    }
                ]
            }
        ]
    }
    # add schema
    if not client.schema.contains(schema):
        client.schema.create(schema)


def add_article_to_batch(parsed_line):
    return [
        {
            "title": parsed_line["title"]
        },
        "Article",
        str(uuid3(NAMESPACE_DNS, parsed_line["title"].replace(" ", "_")))
    ]


def add_paragraph_to_batch(parsed_line):
    return_array = []
    for paragraph in parsed_line["paragraphs"]:
        add_object = {    
            "content": paragraph["content"],
            "order": paragraph["count"],
            "word_count": len(paragraph["content"]),
            "inArticle": [{
                "beacon": "weaviate://localhost/" + str(uuid3(NAMESPACE_DNS, parsed_line["title"].replace(" ", "_")))
            }]
        }
        if "title" in paragraph:
            # Skip if wiki paragraph
            if ":" in paragraph["title"]:
                continue
            add_object["title"] = paragraph["title"]
        # add to batch
        return_array.append([
            add_object,
            "Paragraph",
            str(uuid3(NAMESPACE_DNS, parsed_line["title"].replace(" ", "_") + "___paragraph___" + str(paragraph["count"])))
        ])
    return return_array


def handle_results(results):
    if results is not None:
        for result in results:
            if 'result' in result and 'errors' in result['result'] and  'error' in result['result']['errors']:
                for message in result['result']['errors']['error']:
                    logger.debug(message['message'])


def import_data_without_crefs(wiki_data_file):
    counter = 1
    counter_article = 0
    counter_article_successful = 0
    counter_article_failed = 0
    uuids_a = []
    uuids_p = []
    uuids_ex = []
    with open(wiki_data_file) as f:
        for line in f:
            parsed_line = json.loads(line)
            if len(parsed_line["paragraphs"]) > 0:
                try:
                    article_obj = add_article_to_batch(parsed_line)
                    counter_article += 1
                    # skip if it is a standard wiki category
                    if ":" in article_obj[2]:
                        continue
                    else:
                        # add the article obj
                        client.data_object.create(article_obj[0], article_obj[1], article_obj[2])
                        counter_article_successful += 1
                        counter += 1
                        uuids_a.append(article_obj[2])    
                except Exception as e:
                    uuids_ex.append(article_obj[2])
                    counter_article_failed += 1
                    logger.error(f"issue adding article {article_obj[2]}")
                    logger.error(e)
                    logger.error(''.join(traceback.format_tb(e.__traceback__)))
    logger.debug(f"articles added {counter_article} / {counter_article_successful} / {counter_article_failed}")            
    client.batch.create_objects()
    client.batch.flush()
    return uuids_a, uuids_p, uuids_ex

def delete_uuids(uuids, name):
    for uuid in uuids:
        client.data_object.delete(uuid)
    logger.info(f"deleted {name} objects: {len(uuids)}")

def checkUUIDExists(_id: str):
    exists = client.data_object.exists(_id)
    if not exists:
        logger.error(f"ERROR!!! Object with ID: {_id} doesn't exist!!! exists: {exists}")
        raise

def checkIfObjectsExist(uuids):
    for _id in uuids:
        checkUUIDExists(_id)

def checkForDuplicates(uuids):
    if len(set(uuids)) != len(uuids):
        logger.info('uuids contain duplicates')
    return set(uuids)

def unzip():
    shutil.unpack_archive("wikipedia1k.json.zip", ".")

def cleanup():
    os.remove("wikipedia1k.json")

def performImport(skipDelete):
    logger.info("Start import")
    wiki_data_file = "wikipedia1k.json"
    logger.info("Importing data")
    uuids_a, uuids_p, uuids_ex = import_data_without_crefs(wiki_data_file)
    logger.info("Checking if object's exist articles")
    uuids_a = checkForDuplicates(uuids_a)
    checkIfObjectsExist(uuids_a)
    logger.info("Checking if object's exist paragraphs")
    uuids_p = checkForDuplicates(uuids_p)
    checkIfObjectsExist(uuids_p)
    if not skipDelete:
        logger.info("Deleting data")
        delete_uuids(uuids_a, "Articles")
        delete_uuids(uuids_p, "Paragraphs")
        if len(uuids_ex) > 0:
            delete_uuids(uuids_ex, "existing Articles")
    logger.info("Done")


if __name__ == "__main__":
    client = weaviate.Client("http://localhost:8080")
    try:
        unzip()
        create_weaviate_schema()
        loops = 100
        for i in range(loops):
            logger.info(f"RUN number {i}")
            try:
                performImport(False)
            except Exception as e:
                logger.error(f"RUN number {i}. Exception occurred.")
                logger.error("Exception occurred:", e)
                exit(1)
            except:
                logger.error(f"RUN number {i}. Exception occurred.")
                logger.error("Exception occurred:", sys.exc_info()[0])
                exit(1)
    finally:
        cleanup()
