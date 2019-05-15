#!/bin/bash

##
# Run in $WEAVIATE/ root folder
##

# Create contextionary dir if not available
mkdir -p ./contextionary

# Download the latest files and remove old ones
for SINGLEEXT in idx knn; do
    echo "Start Downloading $SINGLEEXT" && \
    rm -f ./contextionary/contextionary.$SINGLEEXT && \
    wget --quiet -O ./contextionary/contextionary.$SINGLEEXT https://c11y.semi.technology/0.4.0/en/contextionary.$SINGLEEXT && \
    echo "$SINGLEEXT file = done" &
done 

# Wiat to finish download
wait

echo "Done downloading the open source contextionary."
exit 0
