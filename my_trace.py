import weaviate

COLLECTION_NAME = 'MyCollection'

with weaviate.connect_to_local() as client:
    if client.collections.exists(COLLECTION_NAME):
        client.collections.delete(COLLECTION_NAME)

    my_collection = client.collections.create(
        name=COLLECTION_NAME,
        properties=[
            weaviate.classes.config.Property(name='my_property', data_type=weaviate.classes.config.DataType.TEXT)
        ],
        vectorizer_config=weaviate.classes.config.Configure.Vectorizer.none(),
        replication_config=weaviate.classes.config.Configure.replication(
            factor=2,
        ),
        sharding_config=weaviate.classes.config.Configure.sharding(
            desired_count=3,
        ),
    )
    my_col = my_collection.with_consistency_level(weaviate.classes.config.ConsistencyLevel.ALL)
    for i in range(1):
        o = my_col.data.insert(
            properties={
                'my_property': f'blue elephant {i}',
            },
            vector=[0.1] * 1536,
        )
    r = my_col.query.fetch_objects()
    for o in r.objects:
        print(o)