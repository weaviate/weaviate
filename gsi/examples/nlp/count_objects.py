import weaviate
client = weaviate.Client('http://localhost:8081')

try:
    count = client.query.aggregate('News').with_meta_count().do()
except Exception:
    raise Exception