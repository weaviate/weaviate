
GSI Technology's Gemini Fast Vector Search (FVS)

# Introduction

The Weaviate Gemini Plugin leverages GSI Technologies FVS Software Library.

This directory provides profiling, debugging, and benchmark utilities and scripts for FVS.

# Benchmarks

[TBD]

## How To Runa



Here are the steps to run FVS benchmarks:

1) Use or create a new python environment with python version >= 3.8
2) Install the python packages in this directory via '''python3 -m pip install -r requirements.txt'''
3) Create an FVS allocation and copy the value of its id for use later
4) Locate the full path to your datasets numpy file (.npy)
5) Locate the full path to your queries numpy file (.npy)
6) Before launching the benchmark script make sure no other processes/applications are utilizing a lot of processor or memory.  A good thing to do is to run the "who" command at the terminal to see who else is logged in and tell them to log off for a while.
7) Run the benchmark script.  Here is an example:
'''python gemini_fvs.py -a fd283b38-3e4a-11eb-a205-7085c2c5e516 -d /mnt/nas1/fvs_benchmark_datasets/deep-1M.npy -q /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy -o /tmp/benchmarks.csv'''
8) When it's done, the results will be appended to the CSV file that you specified via the "-o" argument.

