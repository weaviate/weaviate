import weaviate
import json
import sys
import traceback

client = weaviate.Client("http://localhost:8081")  # Replace with your endpoint

try:
    # delete class "YourClassName" - THIS WILL DELETE ALL DATA IN THIS CLASS
    client.schema.delete_class("Question")  # Replace with your class name
except:
    traceback.print_exc()

schema = client.schema.get()
print(json.dumps(schema, indent=4))

#class_obj = {
#    "class": "Question",
#    "vectorizer": "text2vec-openai"  # Or "text2vec-cohere" or "text2vec-huggingface"
#}


# we will create the class "Question"
class_obj = {
    "class": "Question",
    "description": "Information from a Jeopardy! question",  # description of the class
    "properties": [
        {
            "dataType": ["text"],
            "description": "The question",
            "name": "question",
        },
        {
            "dataType": ["text"],
            "description": "The answer",
            "name": "answer",
        },
        {
            "dataType": ["string"],
            "description": "The category",
            "name": "category",
        },
    ]
}

client.schema.create_class(class_obj)

schema = client.schema.get()
print(json.dumps(schema, indent=4))


# ===== import data =====
# Load data
import requests
url = 'https://raw.githubusercontent.com/weaviate-tutorials/quickstart/main/data/jeopardy_tiny.json'
resp = requests.get(url)
data = json.loads(resp.text)
print( "data size", len(data) )

# Prepare a batch process
with client.batch as batch:
    batch.batch_size=100
    # Batch import all Questions
    for i, d in enumerate(data):
        print(f"importing question: {i+1}")

        properties = {
            "answer": d["Answer"],
            "question": d["Question"],
            "category": d["Category"],
        }

        resp = client.batch.add_data_object(properties, "Question")
        print(resp)
        
        # break

print("Done adding...")


nearText = {"concepts": ["biology"]}

result = (
    client.query
    .get("Question", ["question", "answer", "category"])
    .with_near_text(nearText)
    .with_limit(2)
    .do()
)

print("QUERY RESULT")
print(json.dumps(result, indent=4))
