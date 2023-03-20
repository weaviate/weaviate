import weaviate
client = weaviate.Client('http://localhost:8080')

tmp = input('Please write a sentence or two for which you wish to find the "closest" documents')

query_res = client.query\
    .get("News", ["text"])\
    .with_near_text({"concepts": [tmp]})\
    .with_limit(5)\
    .do()

print(query_res['data']['Get']['News'])