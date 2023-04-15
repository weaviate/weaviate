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

import os
import json
import sys
import traceback
import time
import weaviate
import requests
import argparse
import pandas as pd
import platform

#
# Configuration
#

# Weaviate connection string
WEAVIATE_CONN       = "http://localhost:8091"

# Weaviate import batch size
BATCH_SIZE          = 10

# Name of the custom class for this test program
BENCH_CLASS_NAME    = "BenchmarkDeep1B"

# Set to True to print more messages
VERBOSE             = False

# Timing each import call
BENCHMARK_DETAILED  = False

# Vector index to use, gets overriden via args
VECTOR_INDEX        = False

# The csv output dir to use
RESULTS_DIR         = "results"

# Gemini index config ( nBits gets set via args )
GEMINI_PARAMETERS   = {'skip': False, 'searchType': 'clusters', 'centroidsHammingK': 5000, 'centroidsRerank': 4000, 'hammingK': 3200, 'nBits': -1 }

# Gemini training bits ( gets set via args )
GEMINI_TRAINING_BITS= -1

# Gemini search type ( gets set via args )
GEMINI_SEARCH_TYPE  = None

# Generally, we don't want to ever allow vector cache-ing in benchmarking
ALLOW_CACHEING      = False

#
# Globals
#

# The total number of imports - will be retrieved via args
TOTAL_ADDS          = -1 

# (Possibly) assume an existing dataset at the start
START_AT            = -1

# Store timings for later export to CSV
STATS               = []

# CSV export filename, gets set after arg parse
EXPORT_FNAME        = None

#
# Parse cmd line arguments
#

parser = argparse.ArgumentParser()
parser.add_argument("-n", required=True)
parser.add_argument("--startat", type=int, default=-1)
parser.add_argument("--gemini", action="store_true")
parser.add_argument("--bitsize", type=int, default=-1)
parser.add_argument("--searchtype", choices={'flat','clusters'})
parser.add_argument("--dontexport", action="store_true",  default=False)
args = parser.parse_args()

# Determine if we are assuming an existing database
START_AT = args.startat

# Set number items to import
if args.n == "10K":
    TOTAL_ADDS = 10000
elif args.n == "1M":
    TOTAL_ADDS = 1000000
elif args.n == "2M":
    TOTAL_ADDS =2000000
elif args.n == "5M":
    TOTAL_ADDS =5000000
elif args.n == "10M":
    TOTAL_ADDS =10000000
elif args.n == "20M":
    TOTAL_ADDS = 20000000
else:
    TOTAL_ADDS = int(args.n)

# Set vector index
if args.gemini:
    VECTOR_INDEX = "gemini"
    if args.bitsize < 0:
        raise Exception("valid --bitsize argument is required for gemini")
    GEMINI_TRAINING_BITS = args.bitsize
    GEMINI_PARAMETERS['nBits'] = GEMINI_TRAINING_BITS

    if args.searchtype==None:
        raise Exception("valid --searchtype argument is required for gemini")    
    GEMINI_SEARCH_TYPE = args.searchtype 
    GEMINI_PARAMETERS['searchType'] = GEMINI_SEARCH_TYPE

    print("Gemini index paramters=", GEMINI_PARAMETERS)

else:
    VECTOR_INDEX = "hnsw"


# Check results dir
if not os.path.exists(RESULTS_DIR):
    raise Exception("The output dir %s does not exist." % RESULTS_DIR)

# Get CSV export if any
if not args.dontexport:
    vectorstr = "%s__%s" % (VECTOR_INDEX, ("allowcacheing_%s__" % str(ALLOW_CACHEING)) if VECTOR_INDEX == "hnsw" else ("bt_%d__st_%s" % (GEMINI_TRAINING_BITS, GEMINI_SEARCH_TYPE) ))
    EXPORT_FNAME = "%s/Import-%s__%s__sz_%d__%s__%f.csv" % ( RESULTS_DIR, platform.node(), BENCH_CLASS_NAME, TOTAL_ADDS, vectorstr, time.time() )
    if os.path.exists(EXPORT_FNAME):
        raise Exception("File exists %s" % EXPORT_FNAME)
    print("export fname=", EXPORT_FNAME)

else:
    print("WARNING: not exporting csv results")
    time.sleep(1)

#
# Start schema checks and import
#

print("Connecting to Weaviate...")
client = weaviate.Client(WEAVIATE_CONN)
print("Done.")

print("Getting the Weaviate schema...")
schema = client.schema.get()
print(schema)
print("Done.")

# Schema check...
STATS.append( {"event": "start_schema_check", "ts": time.time()} )
if BENCH_CLASS_NAME in [ cls["class"] for cls in schema["classes"] ]:
    print("Warning: Found class='%s'.  Verifying..." % BENCH_CLASS_NAME)

    if START_AT <0:
        raise Exception("expecting --startat to be set")

    # Get class schema and validate
    cls_schema = None
    for cls in schema["classes"]:
        if cls["class"] == BENCH_CLASS_NAME: cls_schema = cls
    if cls_schema==None:
        raise Exception("Could not retrieve schema for class='%s'" % BENCH_CLASS_NAME)
    if cls_schema['vectorIndexType'] != VECTOR_INDEX:
        raise Exception("The schema for class='%s' is not an %s index." % (BENCH_CLASS_NAME, VECTOR_INDEX ))
    if VECTOR_INDEX=="gemini":
        raise Exception("Gemini is not currently supported in this mode.")
    elif VECTOR_INDEX=="hnsw":
        if not ALLOW_CACHEING and cls['vectorIndexConfig']["vectorCacheMaxObjects"]!=0:
            raise Exception("Expected 'vectorCacheMaxObjects'=0")

    resp = client.query.aggregate(BENCH_CLASS_NAME).with_meta_count().do()
    print(resp)
    # should look something like this - {'data': {'Aggregate': {'Benchmark_Deep1B': [{'meta': {'count': 0}}]}}}
    count = 0
    try:
        count = resp['data']['Aggregate'][BENCH_CLASS_NAME][0]['meta']['count']
    except:
        traceback.print_exc()
        raise Exception("Could not get count for '%s'" % BENCH_CLASS_NAME)
    if START_AT>=0:
        print("Checking existing size is '%d'..." % START_AT )
        if count != START_AT:
            raise Exception("Unexpected object count (%d) for '%s', expected=%d" % ( count, BENCH_CLASS_NAME, START_AT))
    print("Schema verified.")
    time.sleep(5)

else:
    # The schema class to create. Define the class first and then set..."

    class_obj = {
        "class": BENCH_CLASS_NAME,
        "description": "The benchmark dataset class for Deep1B",  
        "properties": [
            {
                "dataType": ["text"],
                "description": "The array index as a string",
                "name": "index",
            }
        ],
        "vectorIndexType": VECTOR_INDEX,
    }
        
    if VECTOR_INDEX == "gemini":
        class_obj["vectorIndexConfig"] =  GEMINI_PARAMETERS
    elif VECTOR_INDEX == "hnsw" and not ALLOW_CACHEING:
        class_obj["vectorIndexConfig"] = {"vectorCacheMaxObjects":0}

    # Update the schema with this class
    print("Creating '%s' with %s index..." % (BENCH_CLASS_NAME, VECTOR_INDEX))
    client.schema.create_class(class_obj)

    # Retrieve updated schema and check it...
    print("Done.  Verifying schema and %s index..." % VECTOR_INDEX )
    schema = client.schema.get()
    if BENCH_CLASS_NAME not in [ cls["class"] for cls in schema["classes"] ]:
        raise Exception("Could not verify class='%s' was created." % BENCH_CLASS_NAME)
    cls_schema = None
    for cls in schema["classes"]:
        if cls["class"] == BENCH_CLASS_NAME: cls_schema = cls
    if cls_schema==None:
        raise Exception("Could not retrieve schema for class='%s'" % BENCH_CLASS_NAME)
    if cls_schema['vectorIndexType'] != VECTOR_INDEX:
        raise Exception("The schema for class='%s' is not a %s index." % (BENCH_CLASS_NAME, VECTOR_INDEX))
    if VECTOR_INDEX=="gemini":
        print("Gemini parameter check: got", cls_schema['vectorIndexConfig'], "expected", GEMINI_PARAMETERS)
        if cls_schema['vectorIndexConfig'] != GEMINI_PARAMETERS:
            raise Exception("gemini parameter check failed")
        print("Gemini config ok.")
        time.sleep(5)
    elif VECTOR_INDEX=="hnsw" and not ALLOW_CACHEING:
        print("Hnsw parameter check: got", cls_schema['vectorIndexConfig'])
        if cls_schema['vectorIndexConfig']["vectorCacheMaxObjects"]!= 0:
            raise Exception("Invalid hnsw index config")
        print("Hnsw config ok.")
        time.sleep(5)

    print("Schema verified.")

STATS.append( {"event": "end_schema_check", "ts": time.time()} )

# Prepare a batch process for importing data to Weaviate.
print("Import documents to Weaviate (max of %d docs)" % TOTAL_ADDS)

# Prepare a batch process for sending data to weaviate
print("Uploading benchmark indices to Weaviate (max of around %d strings)" % TOTAL_ADDS)

if START_AT >= 0:
    count = START_AT
else:
    count = 0
print("Start count=", count)

while True: # lets loop until we exceed the MAX configured above
    with client.batch as batch:
        batch.batch_size=BATCH_SIZE
        # Batch import all Questions
        for i, d in enumerate(range(count, TOTAL_ADDS)):

            #
            # perform an interim validation
            #
            if (d % 1000) ==0:
                print("Getting interim count to match=%d" % i)
                resp = client.query.aggregate(BENCH_CLASS_NAME).with_meta_count().do()
                print(resp)
                interim_count = 0
                try:
                    interim_count = resp['data']['Aggregate'][BENCH_CLASS_NAME][0]['meta']['count']
                except:
                    traceback.print_exc()
                    raise Exception("Could not get count for '%s'" % BENCH_CLASS_NAME)
                if interim_count != d:
                    raise Exception("Unexpected object count (%d) for '%s', expected=%d" % \
                            ( interim_count, BENCH_CLASS_NAME, d))
                print("Interim count verified (%d,%d)" % (interim_count, d))

            #
            # Add the new item
            #
            if VERBOSE: print(f"importing index: {i}")
            properties = {
                "index": str(d)
            }

            if BENCHMARK_DETAILED: STATS.append( {"event": "adding %d/%d" % ((d+1), TOTAL_ADDS), "ts": time.time()} )
            elif (d % 1000) ==0: STATS.append( {"event": "adding %d/%d" % ((d+1), TOTAL_ADDS), "ts": time.time()} )

            resp = client.batch.add_data_object(properties, BENCH_CLASS_NAME)
            if 'error' in resp:
                print("Got error adding object->", resp)
                raise Exception("Add failed at %d" % i)

            if BENCHMARK_DETAILED: STATS.append( {"event": "added %d/%d" % ((d+1),TOTAL_ADDS), "ts": time.time()} )
            elif (d % 1000) ==0: STATS.append( {"event": "added %d/%d" % ((d+1), TOTAL_ADDS), "ts": time.time()} )

            if (d % 1000)==0: print("Imported %d/%d so far..." % (d+1, TOTAL_ADDS) )

            count += 1
            
    if VERBOSE: print("Batch uploaded %d items so far..." % count)
    if count == TOTAL_ADDS:
        break

STATS.append( {"event": "done all %d" % count, "ts": time.time()} )
print("Done adding %d total strings to Weaviate.  Verifying count at Weaviate..." % count)

resp = client.query.aggregate(BENCH_CLASS_NAME).with_meta_count().do()
print("meta count query=", resp)
count = 0
try:
    count = resp['data']['Aggregate'][BENCH_CLASS_NAME][0]['meta']['count']
except:
    traceback.print_exc()
    raise Exception("Could not get count for '%s'" % BENCH_CLASS_NAME)
STATS.append( {"event": "import verified %d" % count, "ts": time.time()} )
if count != TOTAL_ADDS:
    raise Exception("Unexpected object count (%d) for '%s'" % ( count, BENCH_CLASS_NAME ))
print("Verified.")

#
# export the STATS csv
#
if not args.dontexport:
    df = pd.DataFrame(STATS)
    df.to_csv(EXPORT_FNAME)
    print("Wrote results", EXPORT_FNAME)

# 
# Loop until training finishes
#
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

#STATS = []
STATS.append( {"event": "start train", "ts": time.time()} )

# loop here
consec_errs = 0
while True:

    print("Sending a similarity search request now...")
    STATS.append( {"event": "before query", "ts": time.time()} )
    nearText = {"concepts": [ "q-%d" % 0 ]}
    result = client.query.get( BENCH_CLASS_NAME, ["index"] ).with_additional(['lastUpdateTimeUnix']).with_near_text(nearText).with_limit(10).do()

    # Interpret the results
    async_try_again, errors, data = parse_result(result)
    if async_try_again:
        STATS.append( {"event": "async try again", "ts": time.time()} )
        print("Gemini is asynchronously building an index, and has asked us to try the search again a little later...")
        time.sleep(2)
        continue
    elif errors:
        STATS.append( {"event": "query errors", "ts": time.time()} )
        print("We got search errors->", errors)
        consec_errs += 1
        if consec_errs > 5:
            print("Too many errors.  Let's stop here.")
            break
    elif data:
        STATS.append( {"event": "query sucess", "ts": time.time()} )
        print("Successful search, data->", data)
        consec_errs = 0
        break
    else:
        print("Unknown result! Let's stop here.")
        break

STATS.append( {"event": "end train", "ts": time.time()} )

#
# export the STATS csv
#
if not args.dontexport:
    df = pd.DataFrame(STATS)
    df.to_csv(EXPORT_FNAME)
    print("Wrote results", EXPORT_FNAME)


print("Done.")


sys.exit(0)
