#!/bin/bash

##
# Run in $WEAVIATE/ root folder
##

# Create contextionary dir if not available
mkdir -p ./contextionary

# Download the latest files and remove old ones
for SINGLEEXT in idx knn; do
    rm -f ./contextionary/contextionary.$SINGLEEXT && wget -O ./contextionary/contextionary.$SINGLEEXT https://contextionary.creativesoftwarefdn.org/$(curl -sS https://contextionary.creativesoftwarefdn.org/contextionary.json | jq -r ".latestVersion")/en/contextionary.$SINGLEEXT && echo "$SINGLEEXT file = done" &
done 

# Wiat to finish download
wait

echo "Done downloading the open source contextionary."
