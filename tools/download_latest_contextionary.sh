#!/bin/bash

##
# Run in $WEAVIATE/ root folder
##

# Create contextionary dir if not available
mkdir -p ./contextionary

# Download the latest files
wget -O ./contextionary/contextionary.vocab https://contextionary.creativesoftwarefdn.org/$(curl -sS https://contextionary.creativesoftwarefdn.org/contextionary.json | jq -r ".latestVersion")/en/contextionary.vocab && echo "vocab file = done" &
wget -O ./contextionary/contextionary.idx https://contextionary.creativesoftwarefdn.org/$(curl -sS https://contextionary.creativesoftwarefdn.org/contextionary.json | jq -r ".latestVersion")/en/contextionary.idx && echo "idx file = done" &
wget -O ./contextionary/contextionary.knn https://contextionary.creativesoftwarefdn.org/$(curl -sS https://contextionary.creativesoftwarefdn.org/contextionary.json | jq -r ".latestVersion")/en/contextionary.knn && echo "knn file = done" &

# Wiat to finish download
wait

echo "Done downlaoding the open source contextionary."
