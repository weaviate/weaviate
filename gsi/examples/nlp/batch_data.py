import weaviate
import os
import time
import requests

DATADIR = "/mnt/nas1/news20/"
CLASS_NAME = "News"
WEAVIATE_CONN = "http://localhost:8081"
VERBOSE = True
MAX_ADDS = 5000
MAX_SEARCHES = 10
BATCH_SIZE = 10
HEADER = {"Content-Type": "application/json"}




# initialize client and class object
print("Connecting to Weaviate...")
client = weaviate.Client(WEAVIATE_CONN) 
print("Done.")

print("Getting weaviate schema...")
schema = client.schema.get()
print("Done.")

# check if schema contains News class
if CLASS_NAME in [cls["class"] for cls in schema["classes"]]:
    print(f"Warning: found class={CLASS_NAME}. Deleting class...")
    client.schema.delete_class(CLASS_NAME)
    print("Done. verifying schema")
    schema = client.schema.get()
    if CLASS_NAME in [cls["class"] for cls in schema["classes"]]:
        raise Exception(f"did not expect to find class={CLASS_NAME}")
    print("Done.")

class_obj = {
    "class": CLASS_NAME,
    "description": "contains news-related emails",
    "properties": [
        {
            "dataType": ["text"],
            "description": "news category for the email",
            "name": "newsType",
            
        },
        {
            "dataType": ["text"],
            "description": "email content to be vectorized",
            "name": "text",
        },
    ],
    "vectorIndexType": "gemini"
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

print("extracting file paths from datadir=%s" % DATADIR)
# get docs from each subdir
def absoluteFilePaths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))
paths = absoluteFilePaths(DATADIR)

# add data to weaviate schema
with client.batch as batch:
    batch.batch_size = BATCH_SIZE
    for i, path in enumerate(paths):
        with open(path, errors='ignore') as file: # ignoring bad files
            data = file.read()
            properties = {
                "newsType": path.split('/')[-2], # gets the directory for each doc
                "text": data # doc text
                }
            resp = batch.add_data_object(properties, CLASS_NAME)
        if i % 1000 == 0:
            print("batch uploaded %d documents so far" % i)
        if i >= MAX_ADDS:
            break


print("%d total documents import weaviate, preparing for search..." % \
      client.query.aggregate("News").with_meta_count().do()['data']['Aggregate'][CLASS_NAME][0]['meta']['count'])
print("documents per news type:")
gql_json = {"query": "{Aggregate { News ( groupBy: [ \"newsType\" ] ) { meta { count } groupedBy { value path }}}}"}
resp = requests.post(url=WEAVIATE_CONN+"/v1/graphql", headers=HEADER, json=gql_json)
for d in eval(resp.text)['data']['Aggregate']['News']:
    print("type: %s, count: %d" % (d['groupedBy']['value'], d['meta']['count']))

time.sleep(5)

def parse_result(result):
    async_try_again = False
    errors = []
    data = None

    if "errors" in result.keys():
        errs = result["errors"]
        for err in errs:
            if "message" in err.keys():
                mesg = err["message"]
                if mesg.find("vector search: Async index build is in progress.")>=0:
                    async_try_again = True
                elif mesg.find("vector search: Async index build completed.")>=0:
                    async_try_again = True
                else:
                    errors.append(err)
    elif "data" in result.keys():
        data = result["data"]
    return async_try_again, errors, data

# prepare for search
# consec_errs = 0
# successful_searches = 0
# st_time = time.time()

# while successful_searches < MAX_SEARCHES:
#     print("sending a similarity search request now...")
#     nextText = {"concepts": [""]}