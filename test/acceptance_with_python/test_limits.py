import weaviate.classes as wvc

from .conftest import CollectionFactory


def test_requesting_more_than_the_default_limit(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    # This bug is somewhat hard to trigger and needs two things:
    # 1. A higher than default QUERY_MAXIMUM_RESULTS in the weaviate config
    # 2. Adding more objects than the default limit to the collection
    # 3. Querying, so the limit is lower than the limit, but that limit+offset are higher than the default limit
    collection.data.insert_many([{} for _ in range(10001)])
    collection.query.fetch_objects(limit=9999, offset=2)
