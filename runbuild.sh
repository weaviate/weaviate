#!/bin/bash

GOPATH="$HOME/go"
GOROOT=/usr/local/go
export PATH=$GOPATH:$GOROOT/bin:$PATH 

cp go.mod.last go.mod
go build -o ./weaviate-server ./cmd/weaviate-server
