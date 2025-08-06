import time
import weaviate

COLLECTION_NAME = 'MyCollection'

with weaviate.connect_to_local() as client:
    if client.collections.exists(COLLECTION_NAME):
        client.collections.delete(COLLECTION_NAME)
        time.sleep(0.1)

    my_collection = client.collections.create(
        name=COLLECTION_NAME,
        properties=[
            weaviate.classes.config.Property(name='my_property', data_type=weaviate.classes.config.DataType.TEXT)
        ],
        vectorizer_config=weaviate.classes.config.Configure.Vectorizer.none(),
    )
    o = my_collection.data.insert(
        properties={
            'my_property': 'blue elephant',
        },
        vector=[0.1] * 1536,
    )
    r = my_collection.query.fetch_objects()
    for o in r.objects:
        print(o)