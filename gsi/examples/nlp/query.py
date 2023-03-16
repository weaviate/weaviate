import weaviate
client = weaviate.Client('http://localhost:8080')
query_res = client.query\
    .get("News", ["text"])\
    .with_near_text({"concepts": ["nuclear propulsion"]})\
    .with_limit(5)\
    .do()

print(query_res['data']['Get']['News'])