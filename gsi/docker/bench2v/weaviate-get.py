import json
import sys
import traceback
import time
import weaviate

#
# Configuration
#
VERBOSE = True

#
# Sanity check
#

# The weaviate client with connection info
client = weaviate.Client("http://localhost:801")  # Replace with your endpoint


# TODO: Make sure the class exists...

# print the schema
schema = client.schema.get()
if VERBOSE: print("Schema: ", json.dumps(schema, indent=4))

# get the count
result = client.query.aggregate("Benchmark") \
    .with_meta_count().do()
print(result)

# get some items
result = client.query.get("Benchmark", "index") \
  .with_limit(5) \
  .do()
print(result)
sys.exit(0)

# Prepare a batch process for sending data to weaviate
print("Uploading benchmark indices to Weaviate (max of around %d strings)" % MAX_ADDS)
count = 0
while True: # lets loop until we exceed the MAX configured above
    with client.batch as batch:
        batch.batch_size=1
        # Batch import all Questions
        for i, d in enumerate(range(MAX_ADDS)):
            if VERBOSE: print(f"importing index: {i}")

            properties = {
                "index": str(i)
            }

            resp = client.batch.add_data_object(properties, "Benchmark")
            count += 1
            
    if count > MAX_ADDS:
        break
    if VERBOSE: print("Batch uploading %d strings so far..." % count)

print("Done adding %d total strings to Weaviate.  Let's prepare to search..." % count )
time.sleep(5)

def parse_result(result):
    '''Parse a query result into something actionable.'''
  
    async_try_again = False
    errors = []
    data = None

    #print("RESULT->", result)

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


consec_errs = 0
successful_searches = 0
st_time = time.time()

# Now let's perform a bunch of searches
while successful_searches< MAX_SEARCHES:

    print("Sending a search request now...")
    nearText = {"concepts": [ str(successful_searches) ]}
    result = client.query.get("Benchmark", ["index"] ).with_near_text(nearText).with_limit(10).do()
    async_try_again, errors, data = parse_result(result)

    if async_try_again:
        print("Gemini is building an index.  It's asked us to try the search again later...")
        time.sleep(2)
        continue
    elif errors:
        print("We got errors->", errors)
        consec_errs += 1
        if consec_errs > 5: 
            print("Too many errors.  Let's stop...")
            break
    elif data:
        print("Successful search, data->", data)
        consec_errs = 0
        successful_searches += 1
        if successful_searches % 100 == 0:
            print("Total searches so far=%d" % successful_searches)
    else:
        print("Unknown result! Let's stop here...")
        break

e_time = time.time()
print("Total time performing the %d search(es) with async index building=" % successful_searches, e_time-st_time,"seconds")

print("Done.")
