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
import numpy 
import pandas
import platform

#
# Configuration
#

# Weaviate connection string
WEAVIATE_CONN       = "http://localhost:8091"

# Weaviate import batch size
BATCH_SIZE          = 1

# Name of the Weaviate custom class we use for benchmarking
BENCH_CLASS_NAME    = "BenchmarkDeep1B"

# File system location of all the benchmark datasets
BENCH_DATASET_DIR  = "/mnt/nas1/fvs_benchmark_datasets/"
#BENCH_DATASET_DIR   = "/Users/gwilliams/Projects/GSI/Weaviate/data"

# Set to True to print more messages for debugging purposes
VERBOSE             = False

# Hostname used for output
HOSTNAME            = platform.node()

# The "K" in KNN gets set at args
K_NEIGHBORS         = -1

# dir for export
RESULTS_DIR         = "results"

#
# Globals
#

# The index we are benchmarking against ( gets set by args )
VECTOR_INDEX        = -1

# The total number of imports - will be retrieved via args
TOTAL_ADDS          = -1

# Store timings for later export to CSV
STATS               = []

# Retrieved from gemini schema
GEMINI_TRAINING_BITS= -1

# Retrieved from gemini schema
GEMINI_SEARCH_TYPE  = -1

# csv export file, gets set later
EXPORT_FNAME        = None

# this gets retrieved from the index schema
ALLOW_CACHEING      = None

#
# Parse cmdline arguments
#

parser = argparse.ArgumentParser()
parser.add_argument("-n", required=True)
parser.add_argument("-q", type=int, required=True)
parser.add_argument("-k", type=int, required=True)
parser.add_argument("--gemini", action="store_true")
parser.add_argument("--dontexport",  default=False, action="store_true")
args = parser.parse_args()

# Set the search dabasize size
if args.n == "10K":
    TOTAL_ADDS = 10000
elif args.n == "1M":
    TOTAL_ADDS = 1000000
elif args.n == "2M":
    TOTAL_ADDS = 2000000
elif args.n == "5M":
    TOTAL_ADDS = 5000000
elif args.n == "10M":
    TOTAL_ADDS = 10000000
elif args.n == "20M":
    TOTAL_ADDS = 20000000
else:
    TOTAL_ADDS = int(args.n)

# get the index
if args.gemini:
    VECTOR_INDEX = "gemini"
else:
    VECTOR_INDEX = "hnsw"
print("Got requested vector index=", VECTOR_INDEX)

# get k
K_NEIGHBORS = args.k

# Check results dir
if not os.path.exists(RESULTS_DIR):
    raise Exception("The output dir %s does not exist." % RESULTS_DIR)

#
# Load the GT file
#

gt_file = None
if TOTAL_ADDS == 10000:
    gt_file = os.path.join( BENCH_DATASET_DIR, "deep-10K-gt-%d.npy" %  args.q )
elif TOTAL_ADDS == 1000000:
    gt_file = os.path.join( BENCH_DATASET_DIR, "deep-1M-gt-%d.npy" %  args.q )
elif TOTAL_ADDS == 2000000:
    gt_file = os.path.join( BENCH_DATASET_DIR, "deep-2M-gt-%d.npy" %  args.q )
elif TOTAL_ADDS == 5000000:
    gt_file = os.path.join( BENCH_DATASET_DIR, "deep-5M-gt-%d.npy" %  args.q )
elif TOTAL_ADDS == 10000000:
    gt_file = os.path.join( BENCH_DATASET_DIR, "deep-10M-gt-%d.npy" %  args.q )
elif TOTAL_ADDS == 20000000:
    gt_file = os.path.join( BENCH_DATASET_DIR, "deep-20M-gt-%d.npy" %  args.q )
    
print("GTFILE=", gt_file)
gt_dset = numpy.load(gt_file, mmap_mode='r')    
print("Got ground truth file:", gt_dset.shape)

#
# Schemaa checks
#

print("Connecting to Weaviate...")
client = weaviate.Client(WEAVIATE_CONN)
print("Done.")

print("Getting the Weaviate schema...")
schema = client.schema.get()
print(schema)
print("Done.")

# Check if the schema already has our test class.
# If so, try to delete it.
if BENCH_CLASS_NAME not in [ cls["class"] for cls in schema["classes"] ]:
    raise Exception("Could not find class '%s'" % BENCH_CLASS_NAME)
print("Found class='%s'.  Verifying schema..." % BENCH_CLASS_NAME)

# Get class schema and validate
cls_schema = None
for cls in schema["classes"]:
    if cls["class"] == BENCH_CLASS_NAME: cls_schema = cls
if cls_schema==None:
    raise Exception("Could not retrieve schema for class='%s'" % BENCH_CLASS_NAME)
if cls_schema['vectorIndexType'] != VECTOR_INDEX:
    raise Exception("The schema for class='%s' is not an %s index." % (BENCH_CLASS_NAME, VECTOR_INDEX))
if cls_schema['vectorIndexType'] == "gemini":
    GEMINI_TRAINING_BITS = cls_schema["vectorIndexConfig"]["nBits"]
    GEMINI_SEARCH_TYPE = cls_schema["vectorIndexConfig"]["searchType"]
elif cls_schema['vectorIndexType'] == "hnsw":
    ALLOW_CACHEING = False if cls_schema["vectorIndexConfig"]["vectorCacheMaxObjects"]==0 else True

print("Verified.")

# Get object count
print("Getting db size...")
resp = client.query.aggregate(BENCH_CLASS_NAME).with_meta_count().do()
# print(resp)
# should look something like this - {'data': {'Aggregate': {'Benchmark_Deep1B': [{'meta': {'count': 0}}]}}}
count = -1
try:
    count = resp['data']['Aggregate'][BENCH_CLASS_NAME][0]['meta']['count']
except:
    traceback.print_exc()
    raise Exception("Could not get size for '%s'" % BENCH_CLASS_NAME)
print("Got database size=%d, verifying..." % count)

# Check its the right db size
if count !=  TOTAL_ADDS:
    raise Exception("Expected database size of %d but got %d" % ( TOTAL_ADDS, count ))
print("Verifed.")

# Check results dir
if not os.path.exists(RESULTS_DIR):
    raise Exception("The output dir %s does not exist." % RESULTS_DIR)

# Get CSV export if any
if not args.dontexport:
    vectorstr = "%s__%s" % (VECTOR_INDEX, ("allowcacheing_%s__" % str(ALLOW_CACHEING))  \
        if VECTOR_INDEX == "hnsw" else ("bt_%d__st_%s" % (GEMINI_TRAINING_BITS, GEMINI_SEARCH_TYPE) ))
    EXPORT_FNAME = "%s/Search-%s__%s__sz_%d_of_Deep1B__q_%d__k_%d__%s__%f.csv" % \
        ( RESULTS_DIR, HOSTNAME, BENCH_CLASS_NAME,  TOTAL_ADDS, args.q, K_NEIGHBORS, vectorstr, time.time() )
    if os.path.exists(EXPORT_FNAME):
        raise Exception("File exists %s" % EXPORT_FNAME)
    print("export fname=", EXPORT_FNAME)


#
# Perform the benchmark searches
#

def parse_result(result):
    '''Parse a search response extracting the info we need for benchmarking.'''

    if 'errors' in result:
        print(result)
        raise Exception("Got error response from search query")
    elif 'errors' in result['data']['Get']:
        print(result)
        raise Exception("Got error response from search query")

    items = result['data']['Get']['BenchmarkDeep1B']
    timing = int(items[0]['_additional']['lastUpdateTimeUnix'])
    inds = [ int(item['index']) for item in items ]

    return timing, inds

def compute_recall(a, b):
    '''Computes the recall metric on query results.'''

    #print(a, b)
    nq, rank = a.shape
    intersect = [ numpy.intersect1d(a[i, :rank], b[i, :rank]).size for i in range(nq) ]
    ninter = sum( intersect )
    return ninter / a.size, intersect

def do_benchmark_query(idx):
    '''This performs a query from the query set and processes the results.'''
   
    # prepare and perform the weaviate query 
    nearText = {"concepts": [ "q-%d" % idx ]}
    result = client.query.get( BENCH_CLASS_NAME, ["index"] ).with_additional(['lastUpdateTimeUnix']).with_near_text(nearText).with_limit(K_NEIGHBORS).do()

    # get the data from the results we want
    timing, inds = parse_result(result)
    if VERBOSE:
        print("Test query: timing-", timing, "inds=", inds )
        print("GT=", gt_dset[idx][0:K_NEIGHBORS])

    # compute recall
    a =  numpy.array([inds])
    b =  numpy.array( [ list(gt_dset[idx][0:K_NEIGHBORS]) ] ) 
    recall = compute_recall( a, b ) 
    if VERBOSE: print("Recall=", recall)

    return timing, recall[0]


# do one test first
print("Testing one query...")
timing, recall = do_benchmark_query(1)
print("Verfied.")

# now do the entire query set
print("Running %d queries..." % args.q)
for idx in range(args.q):

    timing, recall = do_benchmark_query(idx)
    print("Ran %d:" % (idx+1), timing, recall)

    # accumulate results
    STATS.append( { "qidx": idx, "recall": recall, "searchTime": timing, "host": HOSTNAME, \
                    "gt_file": gt_file, "dset_size": TOTAL_ADDS, "argsn": args.n , "vectorindex": VECTOR_INDEX } )

print("Done.")

# export results to csv
if not args.dontexport:
    df = pandas.DataFrame( STATS )
    print(df)
    df.to_csv(EXPORT_FNAME)
    print("Saved", EXPORT_FNAME)

sys.exit(0)

