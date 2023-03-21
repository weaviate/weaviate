import weaviate
client = weaviate.Client('http://localhost:8080')
print(client.query.aggregate('Fashion').with_meta_count().do())
print(client.query.aggregate('FashionBatchless').with_meta_count().do())