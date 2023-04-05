import weaviate
import time
import pandas as pd
import os, sys
import random
import warnings
#TODO: figure out why deprecation warnings are being triggered during upload
warnings.filterwarnings("ignore",category=DeprecationWarning) 

DATADIR = '/mnt/nas1/fashion/base64_images/'
CLASS_NAME = "Fashion"
WEAVIATE_CONN = "http://localhost:8091"
MAX_ADDS = 4000 # up to 44k, limit to 10k for less than 10 minute upload
MAX_SEARCHES = 50
BATCH_SIZE = 50
HEADER = {"Content-Type": "application/json"}

print("Reading data csv to Pandas...")
df = pd.read_csv('/mnt/nas1/fashion/clean.csv')
print("Checking for base64 images...")
if os.path.exists(DATADIR):
    num_imgs = len(os.listdir(DATADIR))
    if num_imgs > 0: # should be 44k 
        print("%s exists with %d files" % (DATADIR, num_imgs))
    else:
        print("%s exists with %d files, please run $ python3 img_to_base64.py" % (DATADIR, num_imgs))
        sys.exit()
else:
    print("%s does not exist, please run $ python3 img_to_base64.py" % DATADIR)
    sys.exit()
print("Connecting to Weaviate...")
client = weaviate.Client(WEAVIATE_CONN)
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

print("Done. verifying schema and gemini index...")
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
with client.batch as batch:
    batch.batch_size = BATCH_SIZE
    deprecations = 0
    for i, row in df.iterrows():
        data_obj = eval(row.loc[~row.keys().isin(['id', 'uri'])].to_json())
        with open(f'{DATADIR}{row.id}.jpg.b64') as file:
            file_lines = file.readlines()
        encoding = ' '.join(file_lines)
        encoding = encoding.replace('\n', '').replace(' ', '')
        data_obj['image'] = encoding
        batch.add_data_object(class_name="Fashion", data_object=data_obj)
        if i % 1000 == 0:
            print("batch uploaded %d documents so far" % i)
        if i >= MAX_ADDS:
            break
print(f'batch upload time elapsed (s):{time.time() - st_time} for {MAX_ADDS} docs' )


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
consec_errs = 0
successful_searches = 0
st_time = time.time()

while successful_searches < MAX_SEARCHES:
    print("sending a similarity search request now...")
    row = df.iloc[random.randint(MAX_ADDS, df.shape[0]), :]
    uri = row.uri
    with open(f"{DATADIR}{row.id}.jpg.b64") as file:
        file_lines = file.readlines()
    encoding = ' '.join(file_lines)
    encoding = encoding.replace('\n', '').replace(' ', '')
    nearImage = {'image': encoding}
    result = client.query.get(CLASS_NAME, ["productDisplayName"]).with_near_image(nearImage, encode=False).with_limit(2).do()
    async_try_again, errors, data = parse_result(result)
    if async_try_again:
        print("Gemini is asynchronously building an index, and has asked us to try the search again a little later...")
        time.sleep(2)
        continue
    elif errors:
        print("We got search errors->", errors)
        consec_errs += 1
        if consec_errs > 5:
            print("Too many errors. Let's stop here.")
            break
    elif data:
        print("Successful search, query->%s: %s\ndata->%s" % (uri,row.productDisplayName,data['Get']['Fashion']))
        consec_errs = 0
        successful_searches += 1
        if successful_searches % 100 == 0:
            print("Total searches so far=%d" % successful_searches)
    else:
        print("Unknown result, let's stop here.")
        break
# finalize
e_time = time.time()
print("Total time performing the %d search(es) with async index building=" %successful_searches, e_time - st_time, "seconds")
print("Done.")