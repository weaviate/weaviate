#!/bin/bash

# change to script directory
cd "${0%/*}" || exit

# create benchmark/sift directory
mkdir -p ./sift

# check that the files sift_base.fvecs and sift_query.fvecs exist in the benchmark/sift directory.
# download them otherwise.
if [ ! -f "./sift/sift_base.fvecs" ] || [ ! -f "./sift/sift_query.fvecs" ]; then
  echo "Downloading SIFT dataset"
  wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz -P /tmp/
  tar -xvf /tmp/sift.tar.gz -C /tmp
  mv /tmp/sift/sift_base.fvecs  /tmp/sift/sift_query.fvecs ./sift/
fi

go run . -name "SIFT" -numberEntries 100000 -fail "-1" -numBatches "1"
