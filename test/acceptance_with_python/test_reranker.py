import weaviate
import weaviate.classes as wvc


# the dummy reranker is not supported in the python client => create collection from dict
def test_reranker() -> None:
    client = weaviate.connect_to_local()
    collection_name = "TestRerankerDummy"
    client.collections.delete(name=collection_name)
    collection = client.collections.create_from_dict(
        {
            "class": collection_name,
            "vectorizer": "none",
            "moduleConfig": {"reranker-dummy": {}},
            "properties": [{"name": "prop", "dataType": ["text"]}],
        }
    )

    uuid1 = collection.data.insert({"prop": "hello"}, vector=[1, 0])
    uuid2 = collection.data.insert({"prop": "hellohellohello"}, vector=[1, 0])
    uuid3 = collection.data.insert({"prop": "hellohello"}, vector=[1, 0])

    objs = collection.query.near_vector(
        [1, 0], rerank=wvc.query.Rerank(prop="prop"), return_properties=[]
    ).objects
    assert len(objs) == 3

    # sorted by length by dummy reranker
    assert objs[0].uuid == uuid2
    assert objs[1].uuid == uuid3
    assert objs[2].uuid == uuid1

    client.collections.delete(name=collection_name)
