import weaviate

COLLECTION_NAME = 'MyCollection'

with weaviate.connect_to_local() as client:
    if client.collections.exists(COLLECTION_NAME):
        client.collections.delete(COLLECTION_NAME)

    my_collection = client.collections.create_from_dict({
        'invertedIndexConfig': {'bm25': {'b': 0.75, 'k1': 1.2}, 'cleanupIntervalSeconds': 60, 'indexNullState': False, 'indexPropertyLength': False, 'indexTimestamps': False, 'stopwords': {'preset': 'en'}}, 
        'multiTenancyConfig': {'enabled': False, 'autoTenantCreation': False, 'autoTenantActivation': False}, 
        'properties': [{'name': 'my_property', 'dataType': ['text'], 'indexFilterable': True, 'indexSearchable': True, 'indexRangeFilters': False, 'tokenization': 'word'}], 
        'replicationConfig': {'factor': 1, 'asyncEnabled': False, 'deletionStrategy': 'NoAutomatedResolution'}, 
        'shardingConfig': {'virtualPerPhysical': 128, 'desiredCount': 1, 'actualCount': 1, 'desiredVirtualCount': 128, 'actualVirtualCount': 128, 'key': '_id', 'strategy': 'hash', 'function': 'murmur3'}, 
        'vectorConfig': {
            'default': {
                'vectorizer': {'none': {}}, 
                'vectorIndexConfig': {
                    'distanceMetric': 'cosine', 
                    'vectorCacheMaxObjects': 1000000000000,
                },
                'vectorIndexType': 'spfresh',
            },
        },
        'class': 'MyCollection', 
        'moduleConfig': {},
    })
    my_col = my_collection.with_consistency_level(weaviate.classes.config.ConsistencyLevel.ALL)
    for i in range(20):
        o = my_col.data.insert(
            properties={
                'my_property': f'blue elephant {i}',
            },
            vector=[0.1] * 768,
        )
    v = my_col.query.near_vector(
        near_vector=[0.1] * 768,
        distance=2,
        limit=30,
    )
    for o in v.objects:
        print(o)
