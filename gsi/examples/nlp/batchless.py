import weaviate, os
from datetime import datetime

# get docs from each subdir
DATADIR = "/mnt/nas1/news20/"
def absoluteFilePaths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))
paths = absoluteFilePaths(DATADIR)

# initialize client and class object
client = weaviate.Client('http://localhost:8080') 
class_obj = {
    "class": "NewsBatchless",
    "description": "contains news-related emails",
    "moduleConfig": {
        "text2vec-transformers": {
            "poolingStrategy": "masked_mean", 
            "vectorizeClassName": "false"
        }
    },
    "vectorizer": "text2vec-transformers",
    "properties": [
        {
            "name": "newsType",
            "description": "news category for the document",
            "moduleConfig": {
                "text2vec-transformers": {
                    "skip": "true"
                }
            },
            "dataType": ["text"]
        },
        {
            "name": "text",
            "description": "content to be vectorized",
            "moduleConfig": {
                "text2vec-transformers": {
                    "poolingStrategy": "masked_mean",
                    "skip": "false"
                }
            },
            "dataType": ["text"]
        }
    ]
}
try:
    client.schema.create_class(class_obj) 
except weaviate.UnexpectedStatusCodeException as e:
    print('Class already exists')
    pass

t_start = datetime.now()
print('time start:', t_start)

# add data to weaviate schema
for i, path in enumerate(paths):
    with open(path, errors='ignore') as file:
        data = file.read()
        data_obj = {
            "newsType": path.split('/')[-2], # gets the directory for each doc
            "text": data # doc text
        }
        client.data_object.create(data_object=data_obj, class_name="NewsBatchless")


t_end = datetime.now()
print('time end:', t_end)
print('time elapsed:', t_end - t_start)