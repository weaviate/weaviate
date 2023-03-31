#                           _       _
# __      _____  __ ___   ___  __ _| |_ ___
# \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
#  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
#   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
#
#  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
#
#  CONTACT: hello@weaviate.io
#

import json
import sys
import traceback
import time
import weaviate
import requests
import argparse

#
# Configuration
#

# Weaviate connection string
WEAVIATE_CONN   = "http://localhost:8081"

# Maximum number of documents to add
MAX_ADDS        = 4000

# Maximum number of (successful) searches to perform
MAX_SEARCHES    = 10

# Weaviate import batch size
BATCH_SIZE      = 10

# Name of the custom class for this test program
CLASS_NAME      = "SanityQuestion"

# Set to True to print more messages
VERBOSE         = True

#
# Sanity check
#

# Check some arguments to override defaults
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-n", type=int)
args = parser.parse_args()
if args.n != None:
    MAX_ADDS = args.n
    print("Overriding MAX_ADDS=%d" % MAX_ADDS)

print("Connecting to Weaviate...")
client = weaviate.Client(WEAVIATE_CONN)
print("Done.")

print("Getting the Weaviate schema...")
schema = client.schema.get()
print(schema)
print("Done.")

# Check if the schema already has our test class.
# If so, try to delete it.
if CLASS_NAME in [ cls["class"] for cls in schema["classes"] ]:
    print("Warning:  Found class='%s'.  Will try to delete..." % CLASS_NAME)
    
    client.schema.delete_class(CLASS_NAME)  
    print("Done. Verifying schema...")

    schema = client.schema.get()
    if CLASS_NAME in [ cls["class"] for cls in schema["classes"] ]:
        raise Exception("Did not expect to find class='%s'" % CLASS_NAME)

    print("Done.")

# The schema class to create.
class_obj = {
    "class": CLASS_NAME,
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
    ],
    "vectorIndexType": "gemini" # Here is where we tell Weaviate to use the gemini plugin
}

# Update the schema with this class
print("Creating '%s' with gemini index..." % CLASS_NAME)
client.schema.create_class(class_obj)

# Retrieve updated schema and check its a gemini index
print("Done.  Verifying schema and gemini index...")
schema = client.schema.get()
if CLASS_NAME not in [ cls["class"] for cls in schema["classes"] ]:
    raise Exception("Could not verify class='%s' was created." % CLASS_NAME)
cls_schema = None
for cls in schema["classes"]:
    if cls["class"] == CLASS_NAME: cls_schema = cls
if cls_schema==None:
    raise Exception("Could not retrieve schema for class='%s'" % CLASS_NAME)
if cls_schema['vectorIndexType'] != "gemini":
    raise Exception("The schema for class='%s' is not a gemini index." % CLASS_NAME)
print("Verified.")

# Retrieve some NLP sample data.
print("Retrieving NLP data...")
url = 'https://raw.githubusercontent.com/weaviate-tutorials/quickstart/main/data/jeopardy_tiny.json'
resp = requests.get(url)
data = json.loads(resp.text)
if len(data)==0:
    raise Exception("There was a problem retrieve NLP test data.")

# Prepare a batch process for importing data to Weaviate.
print("Import documents to Weaviate (max of around %d docs)" % MAX_ADDS)

# Let's loop until we exceed MAX_ADDS configured above.
count = 0
while True: 

    with client.batch as batch:
        batch.batch_size=BATCH_SIZE

        for i, d in enumerate(data):
            properties = {
                "answer": d["Answer"],
                "question": d["Question"],
                "category": d["Category"],
            }

            resp = client.batch.add_data_object(properties, CLASS_NAME)
            # TODO: check there's no error - print(resp)
            count += 1
            
    if count > MAX_ADDS:
        break

    print("Batch uploaded %d documents so far..." % count)

print("%d total documents imported to Weaviate.  Let's prepare to search..." % count )
time.sleep(5)

def parse_result(result):
    '''Parse a query result into something actionable.'''
  
    async_try_again = False
    errors = []
    data = None

    # First loop through errors if any.  
    # We look for "gemini async build" messages 
    # and don't interpret them as errors.
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


# Prepare for search
consec_errs = 0
successful_searches = 0
st_time = time.time()

# Perform several searches
while successful_searches< MAX_SEARCHES:

    print("Sending a similarity search request now...")
    nearText = {"concepts": ["biology"]}
    result = client.query.get(CLASS_NAME, ["question", "answer", "category"] ).with_near_text(nearText).with_limit(2).do()

    # Interpret the results
    async_try_again, errors, data = parse_result(result)
    if async_try_again:
        print("Gemini is asynchronously building an index, and has asked us to try the search again a little later...")
        time.sleep(2)
        continue
    elif errors:
        print("We got search errors->", errors)
        consec_errs += 1
        if consec_errs > 5: 
            print("Too many errors.  Let's stop here.")
            break
    elif data:
        print("Successful search, data->", data)
        consec_errs = 0
        successful_searches += 1
        if successful_searches % 100 == 0:
            print("Total searches so far=%d" % successful_searches)
    else:
        print("Unknown result! Let's stop here.")
        break

# Finalize
e_time = time.time()
print("Total time performing the %d search(es) with async index building=" % successful_searches, e_time-st_time,"seconds")
print("Done.")
