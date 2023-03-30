import weaviate
import time
import pandas as pd

CLASS_NAME = "Fashion"
df = pd.read_csv('/mnt/nas1/fashion/clean.csv')
DATADIR = '/mnt/nas1/fashion/base64_images/'

print("Connecting to Weaviate...")
client = weaviate.Client('http://localhost:8081')
print("Done.")

print("Getting weaviate schema...")
schema = client.schema.get()
print("Done.")

# check if schema contains Fashion class
if CLASS_NAME in [cls["class"] for cls in schema["classes"]]:
    print(f"Warning: found class={CLASS_NAME}. Deleting class...")
    client.schema.delete_class(CLASS_NAME)
    print("Done. verifying schema")
    schema = client.schema.get()
    if CLASS_NAME in [cls["class"] for cls in schema["classes"]]:
        raise Exception(f"did not expect to find class={CLASS_NAME}")
    print("Done.")

class_obj = {
    "class": "Fashion",
    "description": "images of different fashion items",
    "moduleConfig": {
        "img2vec-neural": {
            "imageFields": ["image"]
        }
    },
    "properties": [
        {
            "dataType": ["blob"],
            "description": "colored jpg image of fashion article",
            "name": "image"
        }, {
            "dataType": ["string"],
            "description": "attributed gender for clothing",
            "name": "gender"
        }, {
            "dataType": ["string"], 
            "description": "the primary category for clothing",
            "name": "masterCategory"
        }, {
            "dataType": ["string"],
            "description": "more in-depth category",
            "name": "subCategory"
        }, {
            "dataType": ["string"],
            "description": "type of clothing article",
            "name": "articleType"
        }, {
            "dataType": ["string"],
            "description": "the main color in image",
            "name": "baseColour"
        }, {
            "dataType": ["string"],
            "description": "the season of the style??",
            "name": "season"
        }, {
            "dataType": ["int"], 
            "description": "the year the clothing was released",
            "name": "year"
        }, {
            "dataType": ["string"], 
            "description": "the full name of the article",
            "name": "productDisplayName"
        }
    ],
    "vectorIndexType": "gemini",
    "vectorizer": "img2vec-neural"
}

print(f"Creating '{CLASS_NAME}' with gemini index...")
client.schema.create_class(class_obj)

print("done. verifying schema and gemini index...")
schema = client.schema.get()
if CLASS_NAME not in [cls["class"] for cls in schema["classes"]]:
    raise Exception(f"could not verify class={CLASS_NAME}")
cls_schema = None
for cls in schema["classes"]:
    if cls["class"] == CLASS_NAME: cls_schema = cls
if cls_schema == None:
    raise Exception(f"could not retrieve schema for class={CLASS_NAME}")
if cls_schema["vectorIndexType"] != "gemini":
    raise Exception(f"the schema for class='{CLASS_NAME}' is not a gemini index")
print("verified.")



st_time = time.time()
with client.batch(batch_size=100) as batch:
    for i, row in df.iterrows():
        data_obj = eval(row.loc[~row.keys().isin(['id', 'uri'])].to_json())
        with open(f'{DATADIR}{row.id}.jpg.b64') as file:
            file_lines = file.readlines()
        encoding = ' '.join(file_lines)
        encoding = encoding.replace('\n', '').replace(' ', '')
        data_obj['image'] = encoding
        batch.add_data_object(class_name="Fashion", data_object=data_obj)
print('time elapsed (s):', time.time() - st_time)