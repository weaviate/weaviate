
import weaviate
client = weaviate.Client('http://localhost:8081')

tmp = input('Please write a sentence or two for which you wish to find the "closest" documents: ')
try:
    limit = int(input('Please enter the number of characters you wish to see for each doc found (default=300): '))
except:
    limit = 300

resp = client.query\
    .get("News", ["newsType","text"])\
    .with_near_text({"concepts": [tmp]})\
    .with_limit(5)\
    .do()
resp = resp['data']['Get']['News']
for i in range(len(resp)):
    print(f'QUERY RESPONSE DOCUMENT {i}')
    print(f'------- doc type -------\n\t{resp[i]["newsType"]}')
    print(f'------- doc text -------\n{resp[i]["text"][:limit]}')