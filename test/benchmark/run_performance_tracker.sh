#!/bin/bash

# change to script directory
cd "${0%/*}" || exit

go run . -name "SIFT" -numberEntries 100000 -fail "-1"