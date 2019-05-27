#!/bin/bash

##
# Run in $WEAVIATE/ root folder
##

VERSION=0.6.0

# Create contextionary dir if not available
mkdir -p ./contextionary

# Download the latest files and remove old ones
for FILE in stopwords.json contextionary.idx contextionary.knn; do
    echo "Start Downloading $FILE" && \
    rm -f ./contextionary/$FILE && \
    wget --quiet -O ./contextionary/$FILE https://c11y.semi.technology/$VERSION/en/$FILE && \
    echo "$FILE = done" &
done 

# Wait to finish download
wait

echo "Done downloading open source contextionary v$VERSION."
exit 0
