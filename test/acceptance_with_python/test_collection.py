import weaviate
import weaviate.classes as wvc

def test_collection_casing() -> None:
    with weaviate.connect_to_local() as client:
        # Goal: Collection create/get/delete should automatically transform class name to GQL (first letter caps).

        # create collection with all "lower" case -> GQL ("Testcollectioncase")
        assert client.collections.create_from_dict({
            "class": "testcollectioncase",
            "vectorizer": "none",
            }
        ) is not None

        # GET should also tranform to GQL
        assert client.collections.get("testcollectioncase") is not None

        # DELETE should also transform to GQL
        client.collections.delete("testcollectioncase")
        assert client.collections.exists("testcollectioncase") is False

        # same with mult-word with "_"
        assert client.collections.create_from_dict({
            "class": "test_collection_case",
            "vectorizer": "none",
            }
        ) is not None


        # GET should also tranform to GQL
        assert client.collections.get("test_collection_case") is not None

        # DELETE should also transform to GQL
        client.collections.delete("test_collection_case")
        assert client.collections.exists("testcollectioncase") is False
