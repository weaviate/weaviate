#
# standard imports
#
import time
import sys
import shutil
import os
import socket

#
# external/installed packages
#
import numpy
import pandas as pd
import swagger_client
from swagger_client.models import *

#
# constants
#

Allocation_id = "fd283b38-3e4a-11eb-a205-7085c2c5e516"

# path to retrieve/store configs and data
Benchmark_Data_File = "/tmp/gemini_fvs.csv"

# benchmark datasets
Benchmark_Datasets = [
    {   "name":         "deep1M-q1000",
        "dataset_path": "/mnt/nas1/fvs_benchmark_datasets/deep-1M.npy",
        "queries_path": "/mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy",
        "tries":        1 }
]

# the dataframe/csv columns
Benchmark_Data_Columns = [ "name", "allocationid", "datasetid", \
        "ts_train_start", "ts_train_end", \
        "ts_query_start", "ts_query_end" ]

# set to True to save the query results
Query_Results_Save = True

#
# globals
#

# the dataframe will contain the benchmark results
benchmark_data = None

#
# functions
#

def init_benchmark_data():
    '''Validate and setup benchmark data in preparation for benchmarking.'''

    global benchmark_data

    for dataset in Benchmark_Datasets:
        print("Validating config for", dataset["name"],"...")

        if not os.path.exists( dataset["dataset_path"] ):
            raise Exception("The datset path does not exist->", dataset["dataset_path"])

        if not os.path.exists( dataset["queries_path"] ):
            raise Exception("The queries_path does not exist->", dataset["queries_path"])

        print("...is ok.")

    if os.path.exists( Benchmark_Data_File ):
        print("Validating committed benchmark data at", Benchmark_Data_File )
        benchmark_data = pd.read_csv( Benchmark_Data_File)

        if benchmark_data.columns != Benchmark_Data_Columns:
            raise Exception("Expected these columns->", Benchmark_Data_Columns, \
                    "but got these->", benchmark_data.columns)

    else:
        benchmark_data = pd.DataFrame(columns=Benchmark_Data_Columns)


def run_benchmark(benchmark_dataset):
    '''Run a specific benchmark dataset.'''

    global benchmark_data

    print("Running benchmark for dataset=%s" % benchmark_dataset["name"] )

    # Make sure dataset is under /home/public
    dataset_public_path = os.path.join("/home/public", \
            os.path.basename( benchmark_dataset["dataset_path"] ) )
    if not os.path.exists(dataset_public_path):
        print("Copy dataset to /home/public/ ...")
        shutil.copyfile( benchmark_dataset["dataset_path"], dataset_public_path)    

    # Make sure queries is under /home/public
    queries_public_path = os.path.join("/home/public", \
            os.path.basename( benchmark_dataset["queries_path"] ) )
    if not os.path.exists(queries_public_path):
        print("Copy queries to /home/public/ ...")
        shutil.copyfile( benchmark_dataset["queries_path"], queries_public_path)    

    # Setup connection to local FVS api
    server = socket.gethostbyname(socket.gethostname())
    port = "7761"
    version = 'v1.0'

    # Create FVS api objects
    config = swagger_client.configuration.Configuration()
    api_config = swagger_client.ApiClient(config)
    gsi_datasets_apis = swagger_client.DatasetsApi(api_config)
    gsi_dataset_apis = swagger_client.DatasetApi(api_config)
    gsi_search_apis = swagger_client.SearchApi(api_config)
    gsi_utilities_apis = swagger_client.UtilitiesApi(api_config)
    gsi_demo_apis = swagger_client.DemoApi(api_config)

    # Configure the FVS api
    config.verify_ssl = False
    config.host = f'http://{server}:{port}/{version}'
    
    # Set default header
    api_config.default_headers["allocationToken"] = Allocation_id

    # Import dataset
    print("Importing the dataset...")
    ts_train_start = time.time()
    response = gsi_datasets_apis.controllers_dataset_controller_import_dataset(
                    ImportDatasetRequest(ds_file_path=dataset_public_path, train_ind=True, \
                            nbits=768), allocation_token=Allocation_id)
    dataset_id = response.dataset_id
    print("...got dataset_id=", dataset_id)

    # Wait until training the dataset before loading it
    print("Waiting until training ends...")
    train_status = None
    while train_status != "completed":
        train_status = gsi_dataset_apis.controllers_dataset_controller_get_train_status(\
                dataset_id=dataset_id, allocation_token=Allocation_id).dataset_status
        #print(train_status)
        time.sleep(1)
    ts_train_end = time.time()
    print("..training ended.")

    # Load the queries
    print("Loading the queries...")
    response = gsi_demo_apis.controllers_demo_controller_import_queries(
                        ImportQueriesRequest(queries_file_path=queries_public_path), 
                        allocation_token=Allocation_id)
    queries_id = response.added_query['id']
    queries_path = response.added_query['queriesFilePath']

    # Load dataset
    print("Loading the dataset...")
    gsi_datasets_apis.controllers_dataset_controller_load_dataset(
                    LoadDatasetRequest(allocation_id=Allocation_id, dataset_id=dataset_id), 
                    allocation_token=Allocation_id)

    # Set in focusa
    print("Setting focus...")
    gsi_datasets_apis.controllers_dataset_controller_focus_dataset(
        FocusDatasetRequest(allocation_id=Allocation_id, dataset_id=dataset_id), 
        allocation_token=Allocation_id)

    # Search
    print("Batch search...")
    ts_query_start = time.time()
    response = gsi_search_apis.controllers_search_controller_search(
                    SearchRequest(allocation_id=Allocation_id, dataset_id=dataset_id, 
                        queries_file_path=queries_path, topk=10), allocation_token=Allocation_id)
    ts_query_end = time.time()
    print("search result=", response.search)
    inds = numpy.array(response.indices)
    if Query_Results_Save:
        fname = "/tmp/query_inds.npy"
        numpy.save(fname, inds)
        print("Saved query results indices to", fname)
        dist = numpy.array(response.distance)
        fname = "/tmp/query_inds.npy"
        numpy.save(fname, dist)
        print("Saved query results distances to", fname)

    # Unload dataset
    print("Unloading dataset...")
    gsi_datasets_apis.controllers_dataset_controller_unload_dataset(
                    UnloadDatasetRequest(allocation_id=Allocation_id, dataset_id=dataset_id), 
                    allocation_token=Allocation_id)

    # Removing queries...
    print("Removing queries...")
    gsi_demo_apis.controllers_demo_controller_remove_query(query_id=queries_id, 
            allocation_token=Allocation_id)

    # Removing dataset...
    print("Removing dataset...")
    gsi_datasets_apis.controllers_dataset_controller_remove_dataset(dataset_id=dataset_id, 
            allocation_token=Allocation_id)

    # Store and commit the results
    benchmark_data = benchmark_data.append( \
            {   "name": benchmark_dataset["name"], \
                "allocationid": Allocation_id, \
                "datasetid": dataset_id, \
                "ts_train_start": ts_train_start, \
                "ts_train_end": ts_train_end, \
                "ts_query_start": ts_query_start, \
                "ts_query_end": ts_query_end },
            ignore_index=True )
    benchmark_data.to_csv( Benchmark_Data_File, index=True)
    print("Committed benchmark result.")

    print("Done benchmark for dataset=%s" % benchmark_dataset["name"] )

def run_all_benchmarks():
    '''Determine the benchmarks we need to run and run them.'''

    global benchmark_data

    print("Running all required benchmarks...")

    # Iterate benchmark datasets
    for dset in Benchmark_Datasets:
      
        name = dset["name"]
        tries = dset["tries"]

        # get benchmark data rows for this dataset which completed
        rows = benchmark_data[ \
                (benchmark_data["name"]==name) & \
                (benchmark_data["ts_query_end"]>0) ]
        print("For dataset=%s, %d/%d run(s) completed." % (name, rows.shape[0], tries))

        # try to complete the benchmarks for this dataset (up to 'tries')
        for _ in range(tries-rows.shape[0]):
            run_benchmark(dset)

    print("Done running all required benchmarks.")

if __name__ == "__main__":

    init_benchmark_data()

    run_all_benchmarks()
