import weaviate
client = weaviate.Client('http://localhost:8080')

print(client.query.aggregate('News').with_meta_count().do())