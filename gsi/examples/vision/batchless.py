import weaviate, base64
import pandas as pd

df = pd.read_csv('/mnt/nas1/fashion/clean.csv')
DATADIR = '/mnt/nas1/fashion/base64_images/'
client = weaviate.Client('http://localhost:8080')

def idToPath(id):
    return DATADIR+str(id)+".jpg.b64"

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
    "vectorIndexType": "hnsw",
    "vectorizer": "img2vec-neural"
}
try:
    client.schema.create_class(class_obj)
    print('class created')
except weaviate.UnexpectedStatusCodeException:
    print('class exists')
    pass

for i, row in df.iterrows():
    data_obj = eval(row.loc[~row.keys().isin(['id', 'uri'])].to_json())
    path = idToPath(row['id'])
    with open(path, 'rb') as img:
        data_obj['image']=img.read()
        img.close()
    client.data_object.create(class_name="Fashion", data_object=data_obj)
    if i >= 500:
        break