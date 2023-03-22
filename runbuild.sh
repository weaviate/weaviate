#!/bin/bash

# This script will help you build the weaivate executable locally (ie, without docker )
# Note that we may likely remove this script as a cleanup activity if/when we merge with main weaviate.

GOPATH="$HOME/go"
GOROOT=/usr/local/go
export PATH=$GOPATH:$GOROOT/bin:$PATH 

cp go.mod.last go.mod 
go build -o ./weaviate-server ./cmd/weaviate-server
