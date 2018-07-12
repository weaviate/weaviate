#!/usr/bin/env bash

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR/..

# Add header to files
HEADER=$(cat $DIR/header.txt)
for i in $(find . -name \*.go); do
    if [ "${i:2:7}" = "restapi" ] || [ "${i:2:6}" = "models" ] || [ "${i:2:3}" = "cmd" ] && [ "$(basename $i)" != "configure_weaviate.go" ]; then
        DOCUMENT=$(cat $i)
        echo "$HEADER $DOCUMENT" > $i
    fi
done

# Remove temp files
find . -type f -name '*.go-e' -delete

# run go fmt
go fmt ./...
