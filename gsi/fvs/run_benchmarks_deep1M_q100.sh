#!/bin/bash

#
# You need to change these parameters to reflect your local setup
#

# Write/append the benchmark results to this file
OUTPUT="results/benchmarks-deep1M-q100.csv"

# Set a valid allocation id
ALLOCATION_ID="fd283b38-3e4a-11eb-a205-7085c2c5e516"

# Path to dataset numpy file
DATASET="/mnt/nas1/fvs_benchmark_datasets/deep-1M.npy"

# Path to queries numpy file
QUERIES="/mnt/nas1/fvs_benchmark_datasets/deep-queries-100.npy"

## Path to the ground truth numpy file
GROUNDTRUTH="/mnt/nas1/fvs_benchmark_datasets/deep-1M-gt-100.npy"

#
# Check the arguments and the system before running the benchmarks.
#
if [ -f "$OUTPUT" ]; then
	echo "Error:  The file $OUTPUT already exists.  Please move that file or use a file path that does not already exist."
	exit 1
fi

# Make sure this script exists on any error going forward.
set -e

#
# Now run all the benchmarks.  Note that this might take a while, so you should consider running it behind the 'screen' utility.
#
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 768
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 768
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 768
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 512
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 512
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 512
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 256
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 256
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 256
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 128
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 128
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 128
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 64
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 64
python gemini_fvs.py -a "$ALLOCATION_ID" -d "$DATASET" -q "$QUERIES" -g "$GROUNDTRUTH"  -o "$OUTPUT" --b 64


